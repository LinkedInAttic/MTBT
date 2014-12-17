/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.proxy.netty;

import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.HttpRequestDecoder;
import io.netty.handler.codec.http.HttpResponseEncoder;

import java.util.Properties;

import org.apache.log4j.Logger;

import com.linkedin.proxy.pool.ConnectionPool;

public class RocksdbInitializer extends BaseInitializer
{
  private static final Logger m_log = Logger.getLogger(RocksdbInitializer.class);

  private ConnectionPool m_connPool;

  public RocksdbInitializer(Properties prop, ConnectionPool connPool)
  {
    super(prop);
    m_connPool = connPool;
  }

  public void initChannel(SocketChannel ch)
  {
    String thrName = Thread.currentThread().getName() + ": ";
    m_log.debug(thrName + "Initializing SocketChannel...");

    ChannelPipeline p = ch.pipeline();

    //HttpMessage encoder/decoder
    p.addLast("httpDecoder", new HttpRequestDecoder());
    p.addLast("httpAggr", new HttpObjectAggregator(_httpBufferSize));
    p.addLast("httpEncoder", new HttpResponseEncoder());

    //Rocksdb encoder/decoder
    p.addLast("rocksdbDecoder", new RocksdbQueryDecoder());
    p.addLast("rocksdbEncoder", new RocksdbQueryEncoder());

    //Rocksdb query handler
    p.addLast("rocksdbHandler", new RocksdbQueryHandler(m_connPool));
  }
}
