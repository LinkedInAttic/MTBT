/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.multitenant.profiler;

import static org.junit.Assert.*;

import java.util.List;
import java.util.Random;

import com.linkedin.multitenant.db.Database.DatabaseResult;
import com.linkedin.multitenant.profiler.EpochResult;

import org.junit.Test;

public class TestEpochResult
{
  @Test
  public void testRegularAdd()
  {
    int histogramSize = 10;
    int testSize = 10000000;

    EpochResult er = new EpochResult(histogramSize, 0, 100);

    long total = 0;
    int cnt = 0;
    int auxArr[] = new int[histogramSize + 1];
    for(int a = 0; a<histogramSize + 1; a++)
      auxArr[a] = 0;

    Random ran = new Random();
    for(int a = 0; a<testSize; a++)
    {
      int chosen = ran.nextInt(histogramSize + (histogramSize / 5));
      long realLatency = 1000L * 1000L * chosen;
      er.add(realLatency, DatabaseResult.OK);

      total += realLatency;
      cnt++;

      if(chosen >= histogramSize)
        chosen = histogramSize;

      auxArr[chosen]++;
    }

    String erStr = er.toString();

    StringBuffer sb = new StringBuffer();

    sb.append(cnt);
    sb.append(" " + total);
    for(int a = 0; a<auxArr.length; a++)
      sb.append(" " + auxArr[a]);

    String auxStr = sb.toString();

    System.out.println("Er: " + erStr);
    System.out.println("Au: " + auxStr);

    /*
    if(!erStr.equals(auxStr))
      fail("Values do not match");*/
  }

  @Test
  public void testCombine()
  {
    int histogramSize = 1000;
    int testSize = 10000000;

    EpochResult er1 = new EpochResult(histogramSize, 0, 100);
    EpochResult er2 = new EpochResult(histogramSize, 0, 100);

    long total = 0;
    int cnt = 0;
    int auxArr[] = new int[histogramSize + 1];
    for(int a = 0; a<histogramSize + 1; a++)
      auxArr[a] = 0;

    Random ran = new Random();
    for(int a = 0; a<testSize; a++)
    {
      int chosen = ran.nextInt(histogramSize );
      long realLatency = 1000L * 1000L * chosen;
      er1.add(realLatency, DatabaseResult.OK);

      total += realLatency;
      cnt++;

      if(chosen >= histogramSize)
        chosen = histogramSize;

      auxArr[chosen]++;
    }
    for(int a = 0; a<testSize; a++)
    {
      int chosen = ran.nextInt(histogramSize);
      long realLatency = 1000L * 1000L * chosen;
      er2.add(realLatency, DatabaseResult.OK);

      total += realLatency;
      cnt++;

      if(chosen >= histogramSize)
        chosen = histogramSize;

      auxArr[chosen]++;
    }

    String er1Str = er1.toString();
    String er2Str = er2.toString();
    er1.add(er2);
    String totalStr = er1.toString();

    StringBuffer sb = new StringBuffer();

    sb.append(cnt);
    sb.append(" " + total);
    for(int a = 0; a<auxArr.length; a++)
      sb.append(" " + auxArr[a]);

    String auxStr = sb.toString();

    System.out.println("Er1: " + er1Str);
    System.out.println("Er2: " + er2Str);
    System.out.println("Tot: " + totalStr);
    System.out.println("Aux: " + auxStr);

    System.out.println("Summary start");
    List<Object> summary = er1.summarize();
    for(int a = 0; a<summary.size(); a++)
      System.out.println(summary.get(a));
    System.out.println("Summary end");

    /*
    if(!totalStr.equals(auxStr))
      fail("Values do not match");*/
  }

  @Test
  public void testByteConversion() throws Exception
  {
    int histogramSize = 1000;
    int testSize = 10000000;

    EpochResult er = new EpochResult(histogramSize, 0, 100);

    Random ran = new Random();
    for(int a = 0; a<testSize; a++)
    {
      int chosen = ran.nextInt(histogramSize + (histogramSize / 5));
      long realLatency = 1000L * 1000L * chosen;
      er.add(realLatency, DatabaseResult.OK);
    }

    byte tempData[] = er.toByteArray();
    EpochResult er2 = new EpochResult(tempData);


    String er1Str = er.toString();
    String er2Str = er2.toString();

    System.out.println("******");
    System.out.println("Er1: " + er1Str);
    System.out.println("Er2: " + er2Str);
    System.out.println("******");


    if(!er1Str.equals(er2Str))
      fail("Values do not match");
  }
}
