/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */

package com.linkedin.proxy.main;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;
import org.rocksdb.RocksDB;

import com.linkedin.proxy.netty.MysqlInitializer;
import com.linkedin.proxy.netty.RocksdbInitializer;
import com.linkedin.proxy.pool.BlockingMysqlConnectionPool;
import com.linkedin.proxy.pool.BlockingRocksdbConnectionPool;
import com.linkedin.proxy.pool.ConnectionPool;

public class ProxyServer
{
  public enum ProxyMode
  {
    MYSQL, ROCKSDB
  }

  private static final String FLAG_PROXY_MODE     = "mode";
  private static final String FLAG_PROXY_RUNTIME  = "runtime";
  private static final String FLAG_PROXY_THR      = "thrPool";
  private static final String FLAG_PROXY_PORT     = "port";

  private static final Logger m_log = Logger.getLogger(ProxyServer.class);

  private static String DB_PORT_MAP_FILE = "";
  private static String DB_SET_FILE = "";
  private static String PROP_FILE = "";

  private static EventLoopGroup bossGroup;
  private static EventLoopGroup workerGroup;
  private static ConnectionPool connPool;
  private static Channel ch;
  private static boolean isClosed = false;

  public static void main(String[] args) throws Exception
  {
    //process command line
    if(processCommandLine(args) == false)
    {
      m_log.fatal("Command line processing error. Closing...");
      return;
    }

    //process properties file
    Properties prop = getProperties(PROP_FILE);
    if(prop == null)
    {
      m_log.fatal("Error in processing properties file. Closing...");
      return;
    }

    //set mode
    ProxyMode runMode;
    String temp = prop.getProperty(FLAG_PROXY_MODE);
    if(temp == null)
    {
      m_log.fatal("Mode is missing. Closing...");
      return;
    }
    else if(temp.equals("mysql"))
    {
      runMode = ProxyMode.MYSQL;
    }
    else if(temp.equals("rocksdb"))
    {
      runMode = ProxyMode.ROCKSDB;
    }
    else
    {
      m_log.fatal("Unknown mode " + temp + ". Closing...");
      return;
    }

    //Process mode specific files
    Set<String> dbSet = new HashSet<String>();
    if(runMode == ProxyMode.ROCKSDB)
    {
      //for rocksdb, I need db names
      if(DB_SET_FILE.equals(""))
      {
        m_log.fatal("DB set file is missing. Closing...");
        return;
      }
      else if(processDbSet(dbSet, DB_SET_FILE) == false)
      {
        m_log.fatal("Error in processing Dbset file. Closing...");
        return;
      }
      else
      {
        m_log.info("DB set file is processed");
      }
    }
    else
    {
      m_log.fatal("Unknown mode " + runMode + ". Closing...");
      return;
    }

    //perform mode based initializations if any
    if(runMode == ProxyMode.ROCKSDB)
    {
      RocksDB.loadLibrary();
    }

    //get run time
    int runTime;
    temp = prop.getProperty(FLAG_PROXY_RUNTIME);
    if(temp == null)
    {
      runTime = 0;
    }
    else
    {
      runTime = Integer.parseInt(temp);
    }
    m_log.info("Runtime is " + runTime);

    //get thread pool size
    int thrSize;
    temp = prop.getProperty(FLAG_PROXY_THR);
    if(temp == null)
    {
      m_log.warn("Thread pool size parameter is missing. It is set to 10 by default");
      thrSize = 10;
    }
    else
    {
      thrSize = Integer.parseInt(temp);
    }

    //get listening port
    int port;
    temp = prop.getProperty(FLAG_PROXY_PORT);
    if(temp == null)
    {
      m_log.fatal("Listening port is not specified. Closing...");
      return;
    }
    else
    {
      port = Integer.parseInt(temp);
    }

    //init thread pools
    bossGroup = new NioEventLoopGroup(1);
    workerGroup = new NioEventLoopGroup(thrSize);

    //create connection pools
    if(runMode == ProxyMode.ROCKSDB)
    {
      connPool = new BlockingRocksdbConnectionPool(dbSet);
    }
    else if(runMode == ProxyMode.MYSQL)
    {
      connPool = new BlockingMysqlConnectionPool();
    }
    else
    {
      m_log.fatal("Unkown setup. Closing...");
      return;
    }

    //init connection pool
    if(connPool.init(prop) == false)
    {
      m_log.fatal("Cannot init conn pool. Closing...");
      return;
    }

    //if run time is specified, then start closing thread
    Thread closingThread = null;
    if(runTime > 0)
    {
      closingThread = new ClosingThread(runTime);
      closingThread.start();

      System.out.println("Closing in " + runTime + " seconds.");
    }
    else
    {
      System.out.println("Type \"close\" to close proxy.");
    }

    try
    {
      ServerBootstrap b = new ServerBootstrap();

      if(runMode == ProxyMode.MYSQL)
      {
        b.group(bossGroup, workerGroup)
          .channel(NioServerSocketChannel.class)
          .handler(new LoggingHandler(LogLevel.INFO))
          .childHandler(new MysqlInitializer(prop, connPool));
      }
      else if(runMode == ProxyMode.ROCKSDB)
      {
        b.group(bossGroup, workerGroup)
        .channel(NioServerSocketChannel.class)
        .handler(new LoggingHandler(LogLevel.INFO))
        .childHandler(new RocksdbInitializer(prop, connPool));
      }

      ch = b.bind(port).sync().channel();

      if(runTime > 0)
      {
        ch.closeFuture().sync();
      }
      else
      {
        BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
        while(true)
        {
          String line = in.readLine();
          m_log.debug("Got line: " + line);

          if(line == null || "close".equals(line.toLowerCase()))
          {
            break;
          }
        }
      }
    }
    catch(Exception e)
    {
      m_log.error("Error..", e);
    }
    finally
    {
      close();
    }
  }

  public static void close() throws Exception
  {
    if(!isClosed)
    {
      if(ch != null)
        ch.close();
      bossGroup.shutdownGracefully();
      workerGroup.shutdownGracefully();
      connPool.closeAll();

      isClosed = true;
    }
  }

  private static String getFlags()
  {
    StringBuilder sb = new StringBuilder();

    sb.append("Allowed flags:\n");
    sb.append("-prop=<FILE_PATH>: Properties file for the execution\n");
    sb.append("-dbSet=<FILE_PATH>: File containing name of the databases\n");
    sb.append("-dbPortMap=<FILE_PATH>: Mapping from database name to host port.");

    return sb.toString();
  }

  /**
   * -propFile=FILE_PATH
   * -dbPortMap=FILE_PATH
   * -dbSet=DB_SET
   * @param args
   * @return true if all information is provided. false otherwise.
   */
  private static boolean processCommandLine(String[] args)
  {
    try
    {
      for(int a = 0; a<args.length; a++)
      {
        if(args[a].startsWith("-dbPortMap"))
        {
          DB_PORT_MAP_FILE = args[a].split("=")[1];
          m_log.info("DbPortMapFile=" + DB_PORT_MAP_FILE);
        }
        else if(args[a].startsWith("-dbSet"))
        {
          DB_SET_FILE = args[a].split("=")[1];
          m_log.info("DbSetFile=" + DB_SET_FILE);
        }
        else if(args[a].startsWith("-propFile"))
        {
          PROP_FILE = args[a].split("=")[1];
          m_log.info("RocksDbFolder=" + PROP_FILE);
        }
        else
        {
          m_log.error("Unknown flag: " + args[a]);
          return false;
        }
      }

      if(PROP_FILE.equals(""))
      {
        m_log.error("Properties file is not given.");
        return false;
      }

      return true;
    }
    catch(Exception e)
    {
      m_log.error("Error while processing command line", e);
      m_log.error(getFlags());

      return false;
    }
  }

  private static boolean processDbSet(Set<String> dbSet, String filename)
  {
    try
    {
      FileInputStream fs = new FileInputStream(filename);
      return processDbSet(dbSet, fs);
    }
    catch(Exception e)
    {
      m_log.fatal("Error in processing dbSet file", e);
      return false;
    }
  }

  private static boolean processDbSet(Set<String> dbSet, InputStream inStr)
  {
    BufferedReader reader = new BufferedReader(new InputStreamReader(inStr));

    try
    {
      String line = reader.readLine();
      while(line != null)
      {
        dbSet.add(line);

        line = reader.readLine();
      }

      reader.close();
      return true;
    }
    catch(Exception e)
    {
      m_log.fatal("Cannot process input stream", e);
      return false;
    }
  }

  /**
   * Tries opening the given properties file, and reads it.
   * @param filename File path to the properties
   * @return Properties object for the given file. Null if any error occurs.
   */
  private static Properties getProperties(String filename)
  {
    try
    {
      Properties values = new Properties();
      File f = new File(filename);
      values.load(new FileReader(f));
      return values;
    }
    catch(Exception e)
    {
      m_log.fatal("Error in processing properties file", e);
      return null;
    }
  }
}
