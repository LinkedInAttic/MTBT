/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.proxy.pool;

import java.sql.DriverManager;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;

import com.linkedin.proxy.conn.MyConnection;
import com.linkedin.proxy.conn.MysqlConnection;

public class BlockingMysqlConnectionPool implements ConnectionPool
{
  public static final String FLAG_PROXY_MYSQL_HOST       = "mysql.host";
  public static final String FLAG_PROXY_MYSQL_PORT       = "mysql.port";
  public static final String FLAG_PROXY_MYSQL_USERNAME   = "mysql.userName";
  public static final String FLAG_PROXY_MYSQL_USERPASS   = "mysql.userPass";
  public static final String FLAG_PROXY_MYSQL_CONN_POOL  = "mysql.connPool";

  private static final Logger m_log = Logger.getLogger(BlockingMysqlConnectionPool.class);

  protected String m_connStr;
  protected BlockingQueue<MyConnection> m_que;

  @Override
  public boolean init(Properties prop) throws Exception
  {
    //get mysql host name
    String hostName;
    String temp = prop.getProperty(FLAG_PROXY_MYSQL_HOST);
    if(temp == null)
    {
      m_log.error("Mysql host name is missing");
      return false;
    }
    else
    {
      hostName = temp;
      m_log.debug("Mysql host name: " + hostName);
    }

    //get mysql host port
    int hostPort;
    temp = prop.getProperty(FLAG_PROXY_MYSQL_PORT);
    if(temp == null)
    {
      m_log.error("Mysql host port is missing");
      return false;
    }
    else
    {
      hostPort = Integer.parseInt(temp);
      m_log.debug("Mysql host port: " + hostPort);
    }

    //get mysql user name
    String userName;
    temp = prop.getProperty(FLAG_PROXY_MYSQL_USERNAME);
    if(temp == null)
    {
      m_log.error("Mysql user name is missing");
      return false;
    }
    else
    {
      userName = temp;
      m_log.debug("Mysql user name: " + userName);
    }

    //get mysql user pass
    String userPass;
    temp = prop.getProperty(FLAG_PROXY_MYSQL_USERPASS);
    if(temp == null)
    {
      m_log.error("Mysql user pass is missing");
      return false;
    }
    else
    {
      userPass = temp;
      m_log.debug("Mysql user pass: " + userPass);
    }

    //get size of the connection pool
    int connPool;
    temp = prop.getProperty(FLAG_PROXY_MYSQL_CONN_POOL);
    if(temp == null)
    {
      m_log.error("Connection pool size is missing");
      return false;
    }
    else
    {
      connPool = Integer.parseInt(temp);
      m_log.debug("Connection pool size: " + connPool);
    }

    m_connStr = "jdbc:mysql://" + hostName + ":" + hostPort + "/?useUnicode=true&characterEncoding=utf-8" + "&user=" + userName + "&password=" + userPass;
    m_que = new LinkedBlockingQueue<MyConnection>(connPool);

    try
    {
      for(int a = 0; a<connPool; a++)
      {
        MyConnection conn = new MysqlConnection(DriverManager.getConnection(m_connStr), "");
        m_que.add(conn);
      }
    }
    catch(Exception e)
    {
      m_log.fatal("Cannot create connection to Mysql Database", e);
      return false;
    }

    return true;
  }

  public MyConnection getConnection(String dbName) throws Exception
  {
    MyConnection conn = m_que.take();

    if(conn.isClosed())
    {
      m_log.debug(Thread.currentThread().getName() + ": Connection from pool is closed. Starting a new connection...");
      conn = new MysqlConnection(DriverManager.getConnection(m_connStr), "");
    }

    return conn;
  }

  public void releaseConnection(MyConnection conn) throws Exception
  {
    m_que.put(conn);
  }

  public void closeAll() throws Exception
  {
    if(m_que != null)
    {
      while(!m_que.isEmpty())
      {
        MyConnection conn = m_que.take();
        conn.closeConn();
      }
    }
  }
}
