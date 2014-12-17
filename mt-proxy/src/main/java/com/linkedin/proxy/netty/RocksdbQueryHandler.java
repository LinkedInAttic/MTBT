/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.proxy.netty;

import org.apache.log4j.Logger;
import org.rocksdb.RocksDB;

import com.linkedin.proxy.conn.MyConnection;
import com.linkedin.proxy.pool.ConnectionPool;
import com.linkedin.proxy.query.Query;
import com.linkedin.proxy.query.Query.QueryResult;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

public class RocksdbQueryHandler extends SimpleChannelInboundHandler<Query>
{
  private static final Logger _log = Logger.getLogger(RocksdbQueryHandler.class);
  private ConnectionPool _connPool;

  public RocksdbQueryHandler(ConnectionPool connPool)
  {
    _connPool = connPool;
  }

  @Override
  protected void messageReceived(ChannelHandlerContext ctx, Query msg) throws Exception
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
      case INVALID:
        msg.setResult(QueryResult.FAIL);
        break;

      default:
        break;
    }

    ctx.writeAndFlush(msg);
  }

  private void executeWrite(Query q)
  {
    MyConnection conn = null;
    try
    {
      byte[] key = q.getKey().getBytes();
      conn = _connPool.getConnection(q.getDbName());
      _log.debug(Thread.currentThread().getName() + ": Got conn");
      ((RocksDB) conn.getConn()).put(key, q.getValue());
      _log.debug(Thread.currentThread().getName() + ": Performed put");

      q.setResult(QueryResult.OK);
    }
    catch(Exception e)
    {
      _log.error(Thread.currentThread().getName() + ": Write query failed", e);
      q.setResult(QueryResult.FAIL);
    }
    finally
    {
      if(conn != null)
        tryRelease(conn);
      _log.debug(Thread.currentThread().getName() + ": Released conn");
    }
  }

  private void executeRead(Query q)
  {
    MyConnection conn = null;
    try
    {
      byte[] key = q.getKey().getBytes();
      conn = _connPool.getConnection(q.getDbName());
      _log.debug(Thread.currentThread().getName() + ": Got conn");
      byte[] val = ((RocksDB) conn.getConn()).get(key);
      _log.debug(Thread.currentThread().getName() + ": Performed get");

      if(val == null)
      {
        q.setValue("no-result".getBytes());
        q.setResult(QueryResult.FAIL);
        _log.debug(Thread.currentThread().getName() + ": No result for get");
      }
      else
      {
        q.setValue(val);
        q.setResult(QueryResult.OK);
        _log.debug(Thread.currentThread().getName() + ": There is result for get");
      }
    }
    catch(Exception e)
    {
      _log.error(Thread.currentThread().getName() + ": Read query failed", e);
      q.setResult(QueryResult.FAIL);
    }
    finally
    {
      if(conn != null)
        tryRelease(conn);
      _log.debug(Thread.currentThread().getName() + ": Released conn");
    }
  }

  private void executeDelete(Query q)
  {
    MyConnection conn = null;
    try
    {
      byte[] key = q.getKey().getBytes();
      conn = _connPool.getConnection(q.getDbName());
      _log.debug(Thread.currentThread().getName() + ": Got conn");
      ((RocksDB) conn.getConn()).remove(key);
      _log.debug(Thread.currentThread().getName() + ": Performed remove");

      q.setResult(QueryResult.OK);
    }
    catch(Exception e)
    {
      _log.error(Thread.currentThread().getName() + ": Delete query failed", e);
      q.setResult(QueryResult.FAIL);
    }
    finally
    {
      if(conn != null)
        tryRelease(conn);
      _log.debug(Thread.currentThread().getName() + ": Released conn");
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
      _log.error(Thread.currentThread().getName() + ": Failed to releaseConnection", e);
    }
  }
}
