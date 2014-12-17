/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.proxy.main;

import org.apache.log4j.Logger;

import com.linkedin.proxy.common.Constants;

public class ClosingThread extends Thread
{
  private static final Logger _LOG = Logger.getLogger(ClosingThread.class);

  private long m_waitingTime;

  public ClosingThread(int waitTimeSeconds)
  {
    m_waitingTime = waitTimeSeconds;
  }

  public void run()
  {
    long st = System.nanoTime();
    long en = st + (m_waitingTime * Constants.BILLION);

    try
    {
      while(System.nanoTime() < en)
      {
        //sleep for 2 ms while we haven't reached end time
        Thread.sleep(2);
      }

      ProxyServer.close();
    }
    catch(Exception e)
    {
      _LOG.fatal("Cannot sleep", e);
    }
  }
}
