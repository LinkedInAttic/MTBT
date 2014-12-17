/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.proxy.pool;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.log4j.Logger;
import org.rocksdb.BloomFilter;
import org.rocksdb.CompactionStyle;
import org.rocksdb.Filter;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksEnv;
import org.rocksdb.util.SizeUnit;

import com.linkedin.proxy.conn.MyConnection;
import com.linkedin.proxy.conn.RocksdbConnection;

public class BlockingRocksdbConnectionPool implements ConnectionPool
{
  public static final String FLAG_ROCKSDB_DATA_FOLDER  = "rocksdb.dataFolder";
  public static final String FLAG_ROCKSDB_FLUSH_POOL   = "rocksdb.flushPool";
  public static final String FLAG_ROCKSDB_COMPACT_POOL = "rocksdb.compactPool";
  public static final String FLAG_ROCKSDB_WRITE_BUFFER = "rocksdb.writeBuffer";
  public static final String FLAG_ROCKSDB_TARGET_FILE  = "rocksdb.targetFile";
  public static final String FLAG_ROCKSDB_CACHE_SIZE   = "rocksdb.cacheSize";
  public static final String FLAG_ROCKSDB_SOFT_RATE    = "rocksdb.softRate";
  public static final String FLAG_ROCKSDB_BLOCK_SIZE   = "rocksdb.blockSize";
  public static final String FLAG_ROCKSDB_UNIVERSAL_COMPACTION  = "rocksdb.compaction.universal";

  private static final Logger m_log = Logger.getLogger(BlockingRocksdbConnectionPool.class);

  protected Map<String, BlockingQueue<MyConnection>> m_map;
  private List<Options> m_optionList;
  private Set<String> m_dbSet;

  public BlockingRocksdbConnectionPool(Set<String> dbSet)
  {
    m_dbSet = dbSet;
    m_map = new HashMap<String, BlockingQueue<MyConnection>>();
    m_optionList = new ArrayList<Options>();
  }

  @Override
  public boolean init(Properties prop) throws Exception
  {
    //get data folder
    String folderPath;
    String temp = prop.getProperty(FLAG_ROCKSDB_DATA_FOLDER);
    if(temp == null)
    {
      m_log.error("Folder path is not specified");
      return false;
    }
    else if(temp.endsWith("/"))
    {
      folderPath = temp.substring(0, temp.length()-1);
    }
    else
    {
      folderPath = temp;
    }
    m_log.debug("Data folder: " + folderPath);

    //get flush pool size
    int flushPoolSize;
    temp = prop.getProperty(FLAG_ROCKSDB_FLUSH_POOL);
    if(temp == null)
    {
      flushPoolSize = 10;
      m_log.warn("Flush pool size is set to 10 by default");
    }
    else
    {
      flushPoolSize = Integer.parseInt(temp);
      m_log.debug("Flush pool size: " + flushPoolSize);
    }

    //get compact pool size
    int compactPoolSize;
    temp = prop.getProperty(FLAG_ROCKSDB_COMPACT_POOL);
    if(temp == null)
    {
      compactPoolSize = 10;
      m_log.warn("Compact pool size is set to 10 by default");
    }
    else
    {
      compactPoolSize = Integer.parseInt(temp);
      m_log.debug("Compact pool size: " + compactPoolSize);
    }

    //set write buffer size
    long writeBufferSize;
    temp = prop.getProperty(FLAG_ROCKSDB_WRITE_BUFFER);
    if(temp == null)
    {
      writeBufferSize = 64 * SizeUnit.MB;
      m_log.warn("Write buffer size is set to " + writeBufferSize + " by default");
    }
    else
    {
      writeBufferSize = Long.parseLong(temp);
      m_log.debug("Write buffer size: " + writeBufferSize);
    }

    //set target file size
    int targetFileSize;
    temp = prop.getProperty(FLAG_ROCKSDB_TARGET_FILE);
    if(temp == null)
    {
      targetFileSize = (int) (64 * SizeUnit.MB);
      m_log.warn("Target file size is set to " + targetFileSize + " by default");
    }
    else
    {
      targetFileSize = Integer.parseInt(temp);
      m_log.debug("Target file size: " + targetFileSize);
    }

    //set cache size
    long cacheSize;
    temp = prop.getProperty(FLAG_ROCKSDB_CACHE_SIZE);
    if(temp == null)
    {
      cacheSize = 64 * SizeUnit.MB;
      m_log.warn("Cache size is set to " + cacheSize + " by default");
    }
    else
    {
      cacheSize = Long.parseLong(temp);
      m_log.debug("Cache file size: " + cacheSize);
    }

    //set soft rate
    double softRate;
    temp = prop.getProperty(FLAG_ROCKSDB_SOFT_RATE);
    if(temp == null)
    {
      softRate = 50;
      m_log.warn("Soft rate is set to " + softRate + " by default");
    }
    else
    {
      softRate = Double.parseDouble(temp);
      m_log.debug("Soft rate: " + softRate);
    }

    //set block size
    long blockSize;
    temp = prop.getProperty(FLAG_ROCKSDB_BLOCK_SIZE);
    if(temp == null)
    {
      blockSize = 8 * SizeUnit.KB;
      m_log.warn("Block size is set to " + blockSize + " by default");
    }
    else
    {
      blockSize = Long.parseLong(temp);
      m_log.debug("Block size: " + blockSize);
    }

    //set universal compaction
    boolean isUniversalCompaction;
    temp = prop.getProperty(FLAG_ROCKSDB_UNIVERSAL_COMPACTION);
    if(temp == null)
    {
      isUniversalCompaction = false;
    }
    else
    {
      temp = temp.toLowerCase();
      if(temp.equals("1") || temp.equals("true") || temp.equals("yes"))
        isUniversalCompaction = true;
      else
        isUniversalCompaction = false;
    }
    m_log.debug("Universal compaction: " + isUniversalCompaction);

    //create RocksDB connections
    RocksEnv re = RocksEnv.getDefault();
    re.setBackgroundThreads(compactPoolSize, RocksEnv.COMPACTION_POOL);
    re.setBackgroundThreads(flushPoolSize, RocksEnv.FLUSH_POOL);

    Filter filter = new BloomFilter(10);
    Iterator<String> itr = m_dbSet.iterator();
    while(itr.hasNext())
    {
      Options opt = new Options();

      if(isUniversalCompaction)
        opt.setCompactionStyle(CompactionStyle.UNIVERSAL);

      String dbName = itr.next();
      String dbPath = folderPath + "/" + dbName;

      opt.setCreateIfMissing(true);
      opt.setWriteBufferSize(writeBufferSize);
      opt.setTargetFileSizeBase(targetFileSize);
      opt.setMaxBackgroundCompactions(compactPoolSize);
      opt.setMaxBackgroundFlushes(flushPoolSize);
      opt.setCacheSize(cacheSize);
      opt.setBlockSize(blockSize);
      opt.setFilter(filter);

      m_optionList.add(opt);

      BlockingQueue<MyConnection> que = new LinkedBlockingQueue<MyConnection>();
      MyConnection conn = new RocksdbConnection(RocksDB.open(opt, dbPath), dbName);

      m_log.debug("Opened RocksDB connection to " + dbPath);
      que.add(conn);

      m_map.put(dbName, que);
    }

    return true;
  }

  @Override
  public MyConnection getConnection(String dbName) throws Exception
  {
    MyConnection conn = m_map.get(dbName).take();
    return conn;
  }

  @Override
  public void releaseConnection(MyConnection conn) throws Exception
  {
    m_map.get(conn.getDbName()).put(conn);
  }

  @Override
  public void closeAll() throws Exception
  {
    Iterator<String> itr = m_map.keySet().iterator();
    while(itr.hasNext())
    {
      String curDB = itr.next();
      BlockingQueue<MyConnection> que = m_map.get(curDB);
      while(!que.isEmpty())
      {
        MyConnection conn = que.take();
        conn.closeConn();
      }
    }

    for(int a = 0; a<m_optionList.size(); a++)
      m_optionList.get(a).dispose();
  }
}
