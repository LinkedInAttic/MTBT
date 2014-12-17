/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.multitenant.profiler;

import static org.junit.Assert.fail;

import java.util.Random;

import com.linkedin.multitenant.common.Query.QueryType;
import com.linkedin.multitenant.db.Database.DatabaseResult;
import com.linkedin.multitenant.profiler.Profiler;

import org.junit.Test;

public class TestProfiler
{
  @Test
  public void test() throws Exception
  {
    int histogramSize = 10;
    int testSize = 10000000;
    int gran = 2;
    int runTime = 10;

    Profiler pr = new Profiler(runTime, gran, histogramSize);

    Random ran = new Random();
    for(int a = 0; a<testSize; a++)
    {
      int lat = ran.nextInt(histogramSize + (histogramSize/5));
      long latencyInNs = 1000L * 1000L * lat;
      int timeFromStart = ran.nextInt(runTime);
      QueryType optType;
      int temp = ran.nextInt(4);
      if(temp == 0)
        optType = QueryType.INSERT;
      else if(temp == 1)
        optType = QueryType.READ;
      else if(temp == 2)
        optType = QueryType.DELETE;
      else
        optType = QueryType.UPDATE;

      pr.add(timeFromStart, latencyInNs, optType, DatabaseResult.OK);
    }

    byte tempData[] = pr.toByteArray();
    Profiler pr2 = new Profiler(tempData);

    String pr1Str = pr.toString();
    String pr2Str = pr2.toString();

    if(!pr1Str.equals(pr2Str))
      fail("Byte conversion is wrong");
  }
}
