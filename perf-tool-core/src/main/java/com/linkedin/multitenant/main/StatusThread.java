/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.multitenant.main;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import com.linkedin.multitenant.common.Constants;

public class StatusThread extends Thread
{
  private List<WorkerThread> _thrList;
  private AtomicInteger _flag;
  private int _sleep;
  private long _startTime = 0;

  public StatusThread(List<WorkerThread> thrList, int sleepSeconds)
  {
    _thrList = thrList;
    _flag = new AtomicInteger(1);
    _sleep = sleepSeconds;
  }

  public void clear()
  {
    _flag.set(0);
  }

  public void run()
  {
    _startTime = System.nanoTime();
    long sleepTill = _startTime + (_sleep * Constants.BILLION);

    while(_flag.get() == 1)
    {
      Map<String, Integer> jobMap = new HashMap<String, Integer>();

      for(int a = 0; a<_thrList.size(); a++)
      {
        String jobName = _thrList.get(a).getJobName();
        int optSucc = _thrList.get(a).getOptSucceeded();
        int optFail = _thrList.get(a).getOptFailed();

        int total = optSucc + optFail;

        if(jobMap.containsKey(jobName))
        {
          total += jobMap.get(jobName).intValue();
        }

        jobMap.put(jobName, total);
      }

      long timeElapsed = System.nanoTime() - _startTime;
      timeElapsed /= Constants.BILLION;
      Iterator<String> itr = jobMap.keySet().iterator();
      while(itr.hasNext())
      {
        String jobName = itr.next();
        int opt = jobMap.get(jobName);

        System.out.println("Time elapsed=" + timeElapsed + " job=" + jobName + " opt=" + opt);
      }

      try
      {
        while(System.nanoTime() < sleepTill)
          Thread.sleep(2);

        sleepTill += _sleep * Constants.BILLION;
      }
      catch (InterruptedException e)
      {
      }
    }
  }
}
