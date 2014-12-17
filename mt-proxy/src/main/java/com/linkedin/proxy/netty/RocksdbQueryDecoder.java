/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.proxy.netty;

import java.util.List;

import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageDecoder;
import io.netty.handler.codec.http.FullHttpRequest;
import io.netty.handler.codec.http.HttpMethod;

import org.apache.log4j.Logger;

import com.linkedin.proxy.query.Query;
import com.linkedin.proxy.query.Query.QueryType;

public class RocksdbQueryDecoder extends MessageToMessageDecoder<FullHttpRequest>
{
  private static final Logger _LOG = Logger.getLogger(RocksdbQueryDecoder.class);

  @Override
  protected void decode(ChannelHandlerContext ctx, FullHttpRequest msg, List<Object> out) throws Exception
  {
    /*
     * Expected inputs:
     * PUT /dbName/key <value in content>
     * GET /dbName/key
     * DELETE /dbName/key
     */

    Query result = new Query();

    try
    {
      HttpMethod met = msg.getMethod();
      String uri = msg.getUri();
      int s = 0;
      int e = uri.length();
      if(uri.charAt(0) == '/')
        s = 1;
      if(uri.charAt(e-1) == '/')
        e--;

      String parts[] = uri.substring(s, e).split("/");

      result.setDbName(parts[0]);
      _LOG.debug("DbName: " + parts[0]);
      result.setKey(parts[1]);
      _LOG.debug("Key: " + parts[1]);

      if(met.equals(HttpMethod.PUT))
      {
        /*
         * If HttpRequest method is PUT, I interpret it as a WRITE query.
         * Query instance's value is set as the value in the HttpRequest.
         */
        byte[] tempData = new byte[msg.content().readableBytes()];
        msg.content().readBytes(tempData);
        result.setValue(tempData);
        _LOG.debug("Value size: " + tempData.length);

        result.setType(QueryType.WRITE);
      }
      else if(met.equals(HttpMethod.GET))
      {
        /*
         * If HttpRequest method is GET, I interpret it as a READ query.
         * Once the query is processed, the result value (if any) is written to MysqlQuery.value.
         */
        result.setType(QueryType.READ);
      }
      else if(met.equals(HttpMethod.DELETE))
      {
        /*
         * If HttpRequest method is DELETE, I interpret it as a DELETE query.
         */
        result.setType(QueryType.DELETE);
      }
      else
      {
        result.setType(QueryType.INVALID);
        _LOG.error("Unhandled HttpMethod: " + met);
        _LOG.error("Type=" + QueryType.INVALID);
      }

      _LOG.debug("Type: " + result.getType());
    }
    catch(Exception e)
    {
      _LOG.error("Exception occured during HttpRequest processing", e);
      result.setType(QueryType.INVALID);
      _LOG.error("Type=" + QueryType.INVALID);
    }

    out.add(result);
  }
}
