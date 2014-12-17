/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.proxy.query;

public class Query
{
  public enum QueryType
  {
    WRITE, READ, DELETE, CREATE, INVALID
  }

  public enum QueryResult
  {
    OK, FAIL, NA
  }

  protected String m_dbName;
  protected String m_key;
  protected byte[] m_val;
  protected QueryType m_type;
  protected QueryResult m_result;

  public Query()
  {
    m_dbName = "";
    m_key = "";
    m_val = null;
    m_type = QueryType.INVALID;
    m_result = QueryResult.NA;
  }

  public String getDbName()
  {
    return m_dbName;
  }

  public void setDbName(String dbName)
  {
    m_dbName = dbName;
  }

  public String getKey()
  {
    return m_key;
  }

  public void setKey(String key)
  {
    m_key = key;
  }

  public byte[] getValue()
  {
    return m_val;
  }

  public void setValue(byte[] val)
  {
    m_val = val;
  }

  public QueryType getType()
  {
    return m_type;
  }

  public void setType(QueryType type)
  {
    m_type = type;
  }

  public boolean isSuccessfull()
  {
    return m_result == QueryResult.OK;
  }

  public void setResult(QueryResult res)
  {
    m_result = res;
  }

  public String toString()
  {
    StringBuilder sb = new StringBuilder();

    sb.append("dbName=");
    sb.append(m_dbName);

    sb.append(" key=");
    sb.append(m_key);

    if(m_val != null)
    {
      sb.append(" value=");
      sb.append(new String(m_val));
    }

    sb.append(" type=");
    sb.append(m_type);

    sb.append(" result=");
    sb.append(m_result);

    return sb.toString();
  }
}
