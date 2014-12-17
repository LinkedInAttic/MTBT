/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.proxy.netty;

import java.util.Properties;

import org.apache.log4j.Logger;

import com.linkedin.proxy.pool.ConnectionPool;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;

public class MysqlInitializer extends BaseInitializer
{
  private static final Logger _LOG = Logger.getLogger(MysqlInitializer.class);
  private ConnectionPool _connPool;

  public MysqlInitializer(Properties prop, ConnectionPool pool)
  {
    super(prop);
    _connPool = pool;
  }

  public void initChannel(SocketChannel ch)
  {
    String thrName = Thread.currentThread().getName() + ": ";
    _LOG.debug(thrName + "Initializing SocketChannel...");

    ChannelPipeline p = ch.pipeline();

    //HttpMessage encoder/decoder
    p.addLast("httpDecoder", new HttpRequestDecoder());
    p.addLast("httpAggr", new HttpObjectAggregator(_httpBufferSize));
    p.addLast("httpEncoder", new HttpResponseEncoder());

    //MysqlQuery encoder/decoder
    p.addLast("mysqlDecoder", new MysqlQueryDecoder());
    p.addLast("mysqlEncoder", new MysqlQueryEncoder());

    //MysqlQuery handler
    p.addLast("mysqlHandler", new MysqlQueryHandler(_connPool));
  }
}
