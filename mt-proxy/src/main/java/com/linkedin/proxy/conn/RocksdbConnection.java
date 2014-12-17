/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.proxy.conn;

import org.rocksdb.RocksDB;

public class RocksdbConnection extends MyConnection
{
  public RocksdbConnection(RocksDB conn, String dbName) throws Exception
  {
    super(conn, dbName);
  }

  public boolean isClosed() throws Exception
  {
    //TODO: RocksDB does not have a function to check if connection is closed
    return false;
  }

  public void closeConn() throws Exception
  {
    RocksDB conn = (RocksDB) m_conn;
    conn.close();
  }
}
