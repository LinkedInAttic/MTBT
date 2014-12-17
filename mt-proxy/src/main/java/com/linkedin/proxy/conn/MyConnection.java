/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.proxy.conn;

public abstract class MyConnection
{
  protected Object m_conn;
  protected String m_dbName;

  public MyConnection(Object conn, String dbName) throws Exception
  {
    m_conn = conn;
    m_dbName = dbName;
  }

  public Object getConn()
  {
    return m_conn;
  }

  public String getDbName()
  {
    return m_dbName;
  }

  public abstract boolean isClosed() throws Exception;

  public abstract void closeConn() throws Exception;
}
