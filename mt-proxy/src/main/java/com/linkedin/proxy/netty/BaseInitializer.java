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

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

public abstract class BaseInitializer extends ChannelInitializer<SocketChannel>
{
  public static final String FLAG_INIT_HTTP_BUFFER = "httpBuffer";

  protected static final Logger _LOG = Logger.getLogger(BaseInitializer.class);
  protected int _httpBufferSize;

  public BaseInitializer(Properties prop)
  {
    String temp = prop.getProperty(FLAG_INIT_HTTP_BUFFER);
    if(temp == null)
    {
      _LOG.warn("Http buffer is set to 64KB by default)");
      _httpBufferSize = 64 * 1024;
    }
    else
    {
      _httpBufferSize = Integer.parseInt(temp);
    }
  }
}
