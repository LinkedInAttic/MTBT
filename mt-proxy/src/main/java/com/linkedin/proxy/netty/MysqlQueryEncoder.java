/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.proxy.netty;

import java.util.List;

import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.http.DefaultFullHttpResponse;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpHeaders;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.netty.handler.codec.http.HttpVersion;

import com.linkedin.proxy.query.MysqlQuery;

import org.apache.log4j.Logger;

public class MysqlQueryEncoder extends MessageToMessageEncoder<MysqlQuery>
{
  private static final byte[] CONTENT = {'D', 'O', 'N', 'E'};
  @SuppressWarnings("unused")
  private static final Logger _LOG = Logger.getLogger(MysqlQueryEncoder.class);

  @Override
  protected void encode(ChannelHandlerContext ctx, MysqlQuery msg, List<Object> out) throws Exception
  {
    FullHttpResponse response = null;

    if(msg.isSuccessfull())
    {
      switch(msg.getType())
      {
        case WRITE:
        case DELETE:
        case CREATE:
          response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.wrappedBuffer(CONTENT));
          response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "octet-stream");
          response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());
          break;
        case READ:
          response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK, Unpooled.wrappedBuffer(msg.getValue()));
          response.headers().set(HttpHeaders.Names.CONTENT_TYPE, "octet-stream");
          response.headers().set(HttpHeaders.Names.CONTENT_LENGTH, response.content().readableBytes());
          break;
        default:
          break;
      }
    }
    else
    {
      response = new DefaultFullHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.NO_CONTENT);
    }

    out.add(response);
  }
}
