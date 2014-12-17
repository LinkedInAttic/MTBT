/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.proxy.pool;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.junit.BeforeClass;
import org.junit.Test;
import org.rocksdb.RocksDB;

public class TestRocksDBConnPool
{
  @BeforeClass
  public static void initDB()
  {
    try
    {
      RocksDB.loadLibrary();
    }
    catch(Exception e)
    {
      e.printStackTrace();
    }
  }

  @Test
  public void test()
  {
    Set<String> dbSet = new HashSet<String>();

    Properties prop = new Properties();
    prop.put(BlockingRocksdbConnectionPool.FLAG_ROCKSDB_DATA_FOLDER, "folder/");

    ConnectionPool connPool = new BlockingRocksdbConnectionPool(dbSet);
    try
    {
      connPool.init(prop);
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
    finally
    {
      tryClose(connPool);
    }
  }

  private void tryClose(ConnectionPool conn)
  {
    try
    {
      if(conn != null)
        conn.closeAll();
    }
    catch (Exception e)
    {
      e.printStackTrace();
    }
  }
}
