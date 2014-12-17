/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.proxy.pool;

import java.util.Properties;

import com.linkedin.proxy.conn.MyConnection;

public interface ConnectionPool
{
  /**
   * Get a Connection from the connection pool.
   * @param dbName Name of the database instance
   * @return A live connection to the database.
   * @throws Exception
   */
  public MyConnection getConnection(String dbName) throws Exception;

  /**
   * Release a previously acquired connection to the pool.
   * @param conn Previously acquired connection.
   * @throws Exception
   */
  public void releaseConnection(MyConnection conn) throws Exception;

  /**
   * Close all open Connections.
   * @throws Exception
   */
  public void closeAll() throws Exception;

  /**
   * Init this connection pool using the given properties.
   * @param prop Properties
   * @return True if everything goes well. False otherwise.
   * @throws Exception
   */
  public boolean init(Properties prop) throws Exception;
}