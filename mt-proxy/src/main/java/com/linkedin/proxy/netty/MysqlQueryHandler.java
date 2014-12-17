/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.proxy.netty;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.linkedin.proxy.conn.MyConnection;
import com.linkedin.proxy.pool.ConnectionPool;
import com.linkedin.proxy.query.MysqlQuery;
import com.linkedin.proxy.query.Query.QueryResult;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class MysqlQueryHandler extends SimpleChannelInboundHandler<MysqlQuery>
{
  private static final Logger _LOG = Logger.getLogger(MysqlQueryHandler.class);
  private ConnectionPool _connPool;

  public MysqlQueryHandler(ConnectionPool connPool)
  {
    _connPool = connPool;
  }

  @Override
  protected void messageReceived(ChannelHandlerContext ctx, MysqlQuery msg) throws Exception
  {
    switch (msg.getType())
    {
      case WRITE:
        executeWrite(msg);
        break;
      case READ:
        executeRead(msg);
        break;
      case DELETE:
        executeDelete(msg);
        break;
      case CREATE:
        executeCreate(msg);
        break;
      case INVALID:
        msg.setResult(QueryResult.FAIL);
        break;

      default:
        break;
    }

    ctx.writeAndFlush(msg);
  }

  private void executeWrite(MysqlQuery q)
  {
    MyConnection conn = null;
    PreparedStatement stmt = null;
    try
    {
      StringBuilder sb = new StringBuilder();
      sb.append("insert into ");
      sb.append(q.getDbName());
      sb.append(".");
      sb.append(q.getTableName());
      sb.append(" (");
      sb.append(q.getKeyColName());
      sb.append(", ");
      sb.append(q.getValueColName());
      sb.append(") values ");
      sb.append("(\"");
      sb.append(q.getKey());
      sb.append("\", ?) on duplicate key update ");
      sb.append(q.getValueColName());
      sb.append("=?");
      String writeStr = sb.toString();

      conn = _connPool.getConnection(q.getDbName());
      stmt = ((Connection) conn.getConn()).prepareStatement(writeStr);
      stmt.setBytes(1,  q.getValue());
      stmt.setBytes(2,  q.getValue());
      stmt.executeUpdate();

      q.setResult(QueryResult.OK);
    }
    catch(Exception e)
    {
      _LOG.error(Thread.currentThread().getName() + ": Write query failed", e);
      q.setResult(QueryResult.FAIL);
    }
    finally
    {
      if(stmt != null)
        tryClose(stmt);
      if(conn != null)
        tryRelease(conn);
    }
  }

  private void executeRead(MysqlQuery q)
  {
    MyConnection conn = null;
    Statement stmt = null;
    ResultSet rs = null;
    try
    {
      StringBuilder sb = new StringBuilder();
      sb.append("select ");
      sb.append(q.getValueColName());
      sb.append(" from ");
      sb.append(q.getDbName());
      sb.append(".");
      sb.append(q.getTableName());
      sb.append(" where ");
      sb.append(q.getKeyColName());
      sb.append("=\"");
      sb.append(q.getKey());
      sb.append("\"");
      String readStr = sb.toString();

      conn = _connPool.getConnection(q.getDbName());
      stmt = ((Connection) conn.getConn()).createStatement();
      rs = stmt.executeQuery(readStr);

      byte[] value;
      if(rs.next())
      {
        value = rs.getBytes(q.getValueColName());
        q.setValue(value);
        q.setResult(QueryResult.OK);
      }
      else
      {
        value = "no-result".getBytes();
        q.setValue(value);
        q.setResult(QueryResult.FAIL);
      }
    }
    catch(Exception e)
    {
      _LOG.error(Thread.currentThread().getName() + ": Read query failed", e);
      q.setResult(QueryResult.FAIL);
    }
    finally
    {
      if(stmt != null)
        tryClose(stmt);
      if(rs != null)
        tryClose(rs);
      if(conn != null)
        tryRelease(conn);
    }
  }

  private void executeDelete(MysqlQuery q)
  {
    MyConnection conn = null;
    Statement stmt = null;
    try
    {
      StringBuilder sb = new StringBuilder();
      sb.append("delete from ");
      sb.append(q.getDbName());
      sb.append(".");
      sb.append(q.getTableName());
      sb.append(" where ");
      sb.append(q.getKeyColName());
      sb.append("=\"");
      sb.append(q.getKey());
      sb.append("\"");
      String deleteStr = sb.toString();

      conn = _connPool.getConnection(q.getDbName());
      stmt = ((Connection) conn.getConn()).createStatement();
      stmt.executeUpdate(deleteStr);

      q.setResult(QueryResult.OK);
    }
    catch(Exception e)
    {
      _LOG.error(Thread.currentThread().getName() + ": Delete query failed", e);
      q.setResult(QueryResult.FAIL);
    }
    finally
    {
      if(stmt != null)
        tryClose(stmt);
      if(conn != null)
        tryRelease(conn);
    }
  }

  private void executeCreate(MysqlQuery q)
  {
    MyConnection conn = null;
    Statement stmt = null;
    try
    {
      int valColSize = Integer.parseInt(new String(q.getValue()));

      String createTableQuery = "create table if not exists " + q.getDbName() + "." + q.getTableName() + " ("
          + q.getKeyColName() + " varchar(40) not null, "
          + q.getValueColName() + " blob(" + valColSize + "),"
          + " primary key(" + q.getKeyColName() + ")) engine= InnoDB";

      conn = _connPool.getConnection(q.getDbName());
      stmt = ((Connection) conn.getConn()).createStatement();
      stmt.executeUpdate(createTableQuery);

      q.setResult(QueryResult.OK);
    }
    catch(Exception e)
    {
      _LOG.error(Thread.currentThread().getName() + ": Create query failed", e);
      q.setResult(QueryResult.FAIL);
    }
    finally
    {
      if(stmt != null)
        tryClose(stmt);
      if(conn != null)
        tryRelease(conn);
    }
  }

  private void tryClose(ResultSet rs)
  {
    try
    {
      rs.close();
    }
    catch(Exception e)
    {
      _LOG.error(Thread.currentThread().getName() + ": Failed to close ResultSet", e);
    }
  }

  private void tryClose(Statement s)
  {
    try
    {
      s.close();
    }
    catch(Exception e)
    {
      _LOG.error(Thread.currentThread().getName() + ": Failed to close Statement", e);
    }
  }

  private void tryRelease(MyConnection conn)
  {
    try
    {
      _connPool.releaseConnection(conn);
    }
    catch(Exception e)
    {
      _LOG.error(Thread.currentThread().getName() + ": Failed to releaseConnection", e);
    }
  }
}
