/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.proxy.query;

public class MysqlQuery extends Query
{
  protected String m_tableName;
  protected String m_keyCol;
  protected String m_valCol;

  public MysqlQuery()
  {
    super();
    m_tableName = "";
    m_keyCol = "";
    m_valCol = "";
  }

  public String getTableName()
  {
    return m_tableName;
  }

  public void setTableName(String tableName)
  {
    m_tableName = tableName;
  }

  public String getKeyColName()
  {
    return m_keyCol;
  }

  public void setKeyColName(String keyColName)
  {
    m_keyCol = keyColName;
  }

  public String getValueColName()
  {
    return m_valCol;
  }

  public void setValueColName(String valueColName)
  {
    m_valCol = valueColName;
  }

  public String toString()
  {
    StringBuilder sb = new StringBuilder();

    sb.append("dbName=");
    sb.append(m_dbName);

    sb.append(" table=");
    sb.append(m_tableName);

    sb.append(" keyColName=");
    sb.append(m_keyCol);

    sb.append(" key=");
    sb.append(m_key);

    sb.append(" valColName=");
    sb.append(m_valCol);

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
