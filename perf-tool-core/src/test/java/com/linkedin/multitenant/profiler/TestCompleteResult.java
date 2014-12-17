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

import com.linkedin.multitenant.db.Database.DatabaseResult;
import com.linkedin.multitenant.profiler.CompleteResult;

import org.junit.Test;

public class TestCompleteResult
{
  @Test
  public void test()
  {
    int histogramSize = 10;
    int testSize = 10000000;
    int gran = 5;
    int runTime = 100;

    CompleteResult cr = new CompleteResult(runTime, gran, histogramSize);

    Random ran = new Random();
    for(int a = 0; a<testSize; a++)
    {
      int lat = ran.nextInt(histogramSize + (histogramSize/5));
      long latencyInNs = 1000L * 1000L * lat;
      int timeFromStart = ran.nextInt(runTime);

      cr.add(timeFromStart, latencyInNs, DatabaseResult.OK);
    }

    System.out.println(cr.toString());
  }

  @Test
  public void testByteConversion() throws Exception
  {
    int histogramSize = 10;
    int testSize = 10000000;
    int gran = 5;
    int runTime = 100;

    CompleteResult cr = new CompleteResult(runTime, gran, histogramSize);

    Random ran = new Random();
    for(int a = 0; a<testSize; a++)
    {
      int lat = ran.nextInt(histogramSize + (histogramSize/5));
      long latencyInNs = 1000L * 1000L * lat;
      int timeFromStart = ran.nextInt(runTime);

      cr.add(timeFromStart, latencyInNs, DatabaseResult.OK);
    }

    byte tempData[] = cr.toByteArray();
    CompleteResult cr2 = new CompleteResult(tempData);

    String cr1Str = cr.toString();
    String cr2Str = cr2.toString();

    if(!cr1Str.equals(cr2Str))
      fail("conversion error");
  }
}
