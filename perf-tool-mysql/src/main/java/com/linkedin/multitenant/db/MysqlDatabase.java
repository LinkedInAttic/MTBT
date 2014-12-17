/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.multitenant.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.multitenant.common.Query;
import com.linkedin.multitenant.main.RunExperiment;
import com.linkedin.multitenant.workload.CoreWorkload;

public class MysqlDatabase implements Database
{
  private static final Logger _LOG = Logger.getLogger(MysqlDatabase.class);

  public static final String FLAG_MYSQL_DB_NAME     = "mysql.dbName";
  public static final String FLAG_MYSQL_TABLE_NAME  = "mysql.tableName";
  public static final String FLAG_MYSQL_USER_NAME  = "mysql.userName";
  public static final String FLAG_MYSQL_USER_PASS  = "mysql.userPass";
  public static final String FLAG_MYSQL_KEY_COL  = "mysql.keyCol";
  public static final String FLAG_MYSQL_VAL_COL  = "mysql.valCol";
  public static final String FLAG_MYSQL_TIMEOUT  = "mysql.timeout";

  protected Connection _conn = null;
  protected String _connStr = null;
  protected int _timeout;

  protected String _keyColName;
  protected String _valColName;
  protected String _tableName;

  protected PreparedStatement _writeStmt = null;
  protected PreparedStatement _readStmt = null;
  protected PreparedStatement _deleteStmt = null;

  @Override
  public DatabaseResult init(Map<String, String> workPlanProperties, Map<String, String> jobProperties) throws Exception
  {
    String hostName;
    int hostPort;
    String jobName;
    String dbName;
    String userName;
    String userPass;
    String tableName;
    String keyColName;
    String valColName;
    int valueColSize;
    int timeout;

    //get job name
    String temp = getParamStr(jobProperties, RunExperiment.FLAG_JOB_NAME);
    if(temp == null)
    {
      _LOG.fatal("Job name is not specified");
      return DatabaseResult.FAIL;
    }
    else
    {
      jobName = temp;
    }

    //get hostname
    temp = getParamStr(workPlanProperties, RunExperiment.FLAG_WORK_HOST);
    if(temp == null)
    {
      _LOG.fatal("Hostname is not specified");
      return DatabaseResult.FAIL;
    }
    else
    {
      hostName = temp;
    }

    //get host port
    hostPort = getParamInt(workPlanProperties, RunExperiment.FLAG_WORK_PORT);
    if(hostPort == -1)
    {
      _LOG.fatal("Host port is not specified");
      return DatabaseResult.FAIL;
    }

    //get db name
    temp = getParamStr(jobProperties, FLAG_MYSQL_DB_NAME);
    if(temp == null)
    {
      _LOG.fatal("Mysql database name is not specified for job " + jobName);
      return DatabaseResult.FAIL;
    }
    else
    {
      dbName = temp;
    }

    //get user name
    temp = getParamStr(workPlanProperties, FLAG_MYSQL_USER_NAME);
    if(temp == null)
    {
      _LOG.fatal("Mysql user name is not specified");
      return DatabaseResult.FAIL;
    }
    else
    {
      userName = temp;
    }

    //get user pass
    temp = getParamStr(workPlanProperties, FLAG_MYSQL_USER_PASS);
    if(temp == null)
    {
      _LOG.fatal("Mysql user pass is not specified");
      return DatabaseResult.FAIL;
    }
    else
    {
      userPass = temp;
    }

    //get table name
    temp = getParamStr(jobProperties, FLAG_MYSQL_TABLE_NAME);
    if(temp == null)
    {
      _LOG.fatal("Mysql table name is not specified for job " + jobName);
      return DatabaseResult.FAIL;
    }
    else
    {
      tableName = temp;
    }

    //get key column name
    temp = getParamStr(jobProperties, FLAG_MYSQL_KEY_COL);
    if(temp == null)
    {
      _LOG.fatal("Mysql key col name is not specified for job " + jobName);
      return DatabaseResult.FAIL;
    }
    else
    {
      keyColName = temp;
    }

    //get value column name
    temp = getParamStr(jobProperties, FLAG_MYSQL_VAL_COL);
    if(temp == null)
    {
      _LOG.fatal("Mysql val col name is not specified for job " + jobName);
      return DatabaseResult.FAIL;
    }
    else
    {
      valColName = temp;
    }

    //set value col size
    valueColSize = getParamInt(jobProperties, CoreWorkload.FLAG_JOB_VALUE_SIZE);
    if(valueColSize == -1)
    {
      _LOG.fatal("Mysql value col size is not specified for job " + jobName);
      return DatabaseResult.FAIL;
    }

    //set timeout
    timeout = getParamInt(workPlanProperties, FLAG_MYSQL_TIMEOUT);
    if(timeout == -1)
      timeout = 0;

    return init(hostName, hostPort, userName, userPass, dbName, tableName, keyColName, valColName, valueColSize, timeout);
  }

  public DatabaseResult init(String dbHost, int dbPort, String userName, String userPass, String dbName, String tableName,
        String keyColName, String valColName, int valueColSize, int timeout) throws Exception
  {
    _tableName = tableName;
    _keyColName = keyColName;
    _valColName = valColName;
    _timeout = timeout;

    _connStr = "jdbc:mysql://" + dbHost + ":" + dbPort + "/" + dbName + "?useUnicode=true&characterEncoding=utf-8" + "&user=" + userName + "&password=" + userPass;
    prepareConn();

    String createTableQuery = "create table if not exists " + _tableName + " (" + _keyColName + " varchar(40) not null, " + _valColName + " blob(" + valueColSize + "),"
        + " primary key(" + _keyColName + ")) engine= InnoDB";
    PreparedStatement stmt = _conn.prepareStatement(createTableQuery);
    try
    {
      stmt.executeUpdate();
    }
    catch(Exception e)
    {
      _LOG.error("Error creating table", e);
    }
    finally
    {
      tryClose(stmt);
    }

    prepareWriteStmt();
    prepareReadStmt();
    prepareDeleteStmt();

    return DatabaseResult.OK;
  }

  private void prepareConn() throws Exception
  {
    _conn = DriverManager.getConnection(_connStr);
  }

  private void prepareWriteStmt() throws Exception
  {
    StringBuilder sb = new StringBuilder();

    sb.append("insert into ");
    sb.append(_tableName);
    sb.append(" (");
    sb.append(_keyColName);
    sb.append(", ");
    sb.append(_valColName);
    sb.append(") values (?, ?) on duplicate key update ");
    sb.append(_valColName);
    sb.append("= ?");

    String writeStr = sb.toString();
    _writeStmt = _conn.prepareStatement(writeStr);
    if(_timeout > 0)
      _writeStmt.setQueryTimeout(_timeout);
  }

  private void prepareReadStmt() throws Exception
  {
    StringBuilder sb = new StringBuilder();

    sb.append("select ");
    sb.append(_valColName);
    sb.append(" from ");
    sb.append(_tableName);
    sb.append(" where ");
    sb.append(_keyColName);
    sb.append("= ?");

    String readStr = sb.toString();
    _readStmt = _conn.prepareStatement(readStr);
    if(_timeout > 0)
      _readStmt.setQueryTimeout(_timeout);
  }

  private void prepareDeleteStmt() throws Exception
  {
    StringBuilder sb = new StringBuilder();

    sb.append("delete from ");
    sb.append(_tableName);
    sb.append(" where ");
    sb.append(_keyColName);
    sb.append("=?");

    String deleteStr = sb.toString();
    _deleteStmt = _conn.prepareStatement(deleteStr);
    if(_timeout > 0)
      _deleteStmt.setQueryTimeout(_timeout);
  }

  private void tryClose(Statement s)
  {
    try
    {
      if(s != null)
      {
        s.close();
      }
    }
    catch(Exception e)
    {
      _LOG.error("Failed to close prepared statement", e);
    }
  }

  private void tryClose(Connection c)
  {
    try
    {
      if(c != null)
        c.close();
    }
    catch(Exception e)
    {
      _LOG.error("Failed to close connection." + e);
    }
  }

  private void tryClose(ResultSet rs)
  {
    try
    {
      if(rs != null)
        rs.close();
    }
    catch(Exception e)
    {
      _LOG.error("Failed to close resultset." + e);
    }
  }

  @Override
  public DatabaseResult doInsert(Query q)
  {
    String keyStr = q.getKey();
    byte valByte[] = q.getValue();

    try
    {
      if(_writeStmt.isClosed() || _conn.isClosed())
      {
        close();
        prepareConn();
        prepareWriteStmt();
        prepareReadStmt();
        prepareDeleteStmt();
      }

      _writeStmt.setString(1, keyStr);
      _writeStmt.setBytes(2, valByte);
      _writeStmt.setBytes(3, valByte);
      _writeStmt.executeUpdate();
      return DatabaseResult.OK;
    }
    catch(Exception e)
    {
      _LOG.error("Query execution exception", e);
      return DatabaseResult.FAIL;
    }
  }

  @Override
  public DatabaseResult doUpdate(Query q)
  {
    String keyStr = q.getKey();
    byte valByte[] = q.getValue();

    try
    {
      if(_writeStmt.isClosed() || _conn.isClosed())
      {
        close();
        prepareConn();
        prepareWriteStmt();
        prepareReadStmt();
        prepareDeleteStmt();
      }

      _writeStmt.setString(1, keyStr);
      _writeStmt.setBytes(2, valByte);
      _writeStmt.setBytes(3, valByte);
      _writeStmt.executeUpdate();
      return DatabaseResult.OK;
    }
    catch(Exception e)
    {
      _LOG.error("Query execution exception", e);
      return DatabaseResult.FAIL;
    }
  }

  @Override
  public DatabaseResult doRead(Query q)
  {
    ResultSet rs = null;

    try
    {
      if(_readStmt.isClosed() || _conn.isClosed())
      {
        close();
        prepareConn();
        prepareWriteStmt();
        prepareReadStmt();
        prepareDeleteStmt();
      }

      _readStmt.setString(1, q.getKey());
      rs = _readStmt.executeQuery();

      if(rs.next())
      {
        @SuppressWarnings("unused")
        byte valueB[] = rs.getBytes(_valColName);
      }

      return DatabaseResult.OK;
    }
    catch(Exception e)
    {
       _LOG.error("Query execution exception", e);
       return DatabaseResult.FAIL;
    }
    finally
    {
      tryClose(rs);
    }
  }

  @Override
  public DatabaseResult doDelete(Query q)
  {
    try
    {
      if(_deleteStmt.isClosed() || _conn.isClosed())
      {
        close();
        prepareConn();
        prepareWriteStmt();
        prepareReadStmt();
        prepareDeleteStmt();
      }

      _deleteStmt.setString(1, q.getKey());
      _deleteStmt.executeUpdate();

      return DatabaseResult.OK;
    }
    catch(Exception e)
    {
      _LOG.error("Query execution exception", e);
      return DatabaseResult.FAIL;
    }
  }

  @Override
  public DatabaseResult close()
  {
    tryClose(_conn);
    tryClose(_writeStmt);
    tryClose(_readStmt);
    tryClose(_deleteStmt);

    return DatabaseResult.OK;
  }

  private String getParamStr(Map<String, String> properties, String propertyName)
  {
    String val = properties.get(propertyName);
    if(val == null)
      return null;
    else
      return val;
  }

  private int getParamInt(Map<String, String> properties, String propertyName)
  {
    String val = properties.get(propertyName);
    if(val == null)
      return -1;
    else
      return Integer.parseInt(val);
  }
}
