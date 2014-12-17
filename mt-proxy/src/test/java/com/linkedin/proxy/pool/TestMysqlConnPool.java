/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.proxy.pool;


import java.util.Properties;
import org.junit.Test;

public class TestMysqlConnPool
{
  @Test
  public void test()
  {
    Properties prop = new Properties();
    prop.put(BlockingMysqlConnectionPool.FLAG_PROXY_MYSQL_HOST, "localhost");
    prop.put(BlockingMysqlConnectionPool.FLAG_PROXY_MYSQL_PORT, "3306");
    prop.put(BlockingMysqlConnectionPool.FLAG_PROXY_MYSQL_USERNAME, "perftool");
    prop.put(BlockingMysqlConnectionPool.FLAG_PROXY_MYSQL_USERPASS, "perftool");
    prop.put(BlockingMysqlConnectionPool.FLAG_PROXY_MYSQL_CONN_POOL, "10");

    ConnectionPool connPool = new BlockingMysqlConnectionPool();
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
