/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.multitenant.profiler;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.linkedin.multitenant.db.Database.DatabaseResult;

import org.apache.log4j.Logger;

public class EpochResult
{
  @SuppressWarnings("unused")
  private static final Logger _LOG = Logger.getLogger(EpochResult.class);
  private static long M = 1000 * 1000;

  //mapping that holds how many operations ended with what latency
  private Map<Integer, Integer> _map;

  //maximum latency to keep track of
  private int _histogramMax;

  //total latency in nanoseconds for this epoch
  private long _totalLat;

  //number of successful operations
  private int _succOpt;

  //number of failed operations
  private int _failedOpt;

  //starting time in seconds for this epoch (inclusive)
  private int _start;

  //ending time in seconds for this epoch (exclusive)
  private int _end;

  public EpochResult(int histogramSize, int start, int end)
  {
    _map = new HashMap<Integer, Integer>();

    if(histogramSize <= 0)
      _histogramMax = 100;
    else
      _histogramMax = histogramSize;

    _totalLat = 0;
    _succOpt = 0;
    _failedOpt = 0;

    _start = start;
    _end = end;
  }

  public EpochResult(byte arr[]) throws Exception
  {
    ByteArrayInputStream bs = new ByteArrayInputStream(arr);
    DataInputStream in = new DataInputStream(bs);

    //read start time
    _start = in.readInt();
    //read end time
    _end = in.readInt();
    //read histogramMax
    _histogramMax = in.readInt();
    //read totalLat
    _totalLat = in.readLong();
    //read opt
    _succOpt = in.readInt();
    //read failedOpt
    _failedOpt = in.readInt();
    //read number of elements in map
    int size = in.readInt();
    _map = new HashMap<Integer, Integer>();
    for(int a = 0; a<size; a++)
    {
      int currentKey = in.readInt();
      int currentVal = in.readInt();

      _map.put(currentKey, currentVal);
    }

    in.close();
  }

  public EpochResult copy() throws Exception
  {
    byte[] tempArr = toByteArray();

    return new EpochResult(tempArr);
  }

  public byte[] toByteArray() throws Exception
  {
    ByteArrayOutputStream bs = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bs);

    //write start time
    out.writeInt(_start);
    //write end time
    out.writeInt(_end);
    //write histogramMax
    out.writeInt(_histogramMax);
    //write totalLat
    out.writeLong(_totalLat);
    //write opt
    out.writeInt(_succOpt);
    //write failedOpt
    out.writeInt(_failedOpt);
    //number of elements in map
    out.writeInt(_map.size());
    //write each element in the map
    Iterator<Integer> itr = _map.keySet().iterator();
    while(itr.hasNext())
    {
      int currentKey = itr.next().intValue();
      int currentVal = _map.get(currentKey).intValue();

      out.writeInt(currentKey);
      out.writeInt(currentVal);
    }

    out.close();
    return bs.toByteArray();
  }

  /**
   * Returns total latency in nanoseconds
   * @return
   */
  public long getTotalLat()
  {
    return _totalLat;
  }

  public int getStartTime()
  {
    return _start;
  }

  public int getEndTime()
  {
    return _end;
  }

  public int getSuccOpt()
  {
    return _succOpt;
  }

  public int getFailedOpt()
  {
    return _failedOpt;
  }

  public Map<Integer, Integer> getMap()
  {
    return _map;
  }

  /**
   * Add a measured latency value to the epoch result
   * @param latency Latency in nanoseconds
   */
  public void add(long latency, DatabaseResult optResult)
  {
    if(optResult == DatabaseResult.FAIL)
    {
      _failedOpt++;
      return;
    }

    //calculate the latency bucket.
    int actualKey = (int) (latency / M);
    if(actualKey >= _histogramMax)
      actualKey = _histogramMax;
    else if(actualKey < 0)
      actualKey = 0;

    int val = 1;
    if(_map.containsKey(actualKey))
    {
      val += _map.get(actualKey).intValue();
    }

    _map.put(actualKey, val);

    _totalLat += latency;
    _succOpt++;
  }

  /**
   * Add given instance to this instance. Result is written in this instance.
   * @param rhs
   */
  public void add(EpochResult rhs)
  {
    _totalLat += rhs.getTotalLat();
    _succOpt += rhs.getSuccOpt();
    _failedOpt += rhs.getFailedOpt();

    Map<Integer, Integer> rhsMap = rhs.getMap();
    Iterator<Integer> itr = rhsMap.keySet().iterator();

    while(itr.hasNext())
    {
      Integer currentKey = itr.next();
      int newVal = rhsMap.get(currentKey);
      if(_map.containsKey(currentKey))
        newVal += _map.get(currentKey).intValue();

      _map.put(currentKey, newVal);
    }
  }

  public String toString()
  {
    StringBuilder sb = new StringBuilder();

    sb.append("[");
    sb.append(_start);
    sb.append(", ");
    sb.append(_end);
    sb.append(")");

    sb.append(" SuccOpt=");
    sb.append(_succOpt);

    sb.append(" TotalLatInNanoSec=");
    sb.append(_totalLat);

    sb.append(" FailedOpt=");
    sb.append(_failedOpt);

    sb.append(" Lat-Opt\t" );
    for(int a = 0; a <= _histogramMax; a++)
    {
      Integer cnt = _map.get(a);
      if(cnt == null)
      {
        sb.append(" ");
        sb.append(a);
        sb.append(":");
        sb.append("0");
      }
      else
      {
        sb.append(" ");
        sb.append(a);
        sb.append(":");
        sb.append(cnt);
      }
    }

    return sb.toString();
  }

  /**
   * Summarizes data in this epoch.
   * @return List of Objects.<br>
   * Index-0: (Integer) End of this epoch in seconds<br>
   * Index-1: (Integer) Number of successful operations<br>
   * Index-2: (Double) Average Latency in milliseconds<br>
   * Index-3: (Integer) 95% latency in milliseconds<br>
   * Index-4: (Integer) 99% latency in milliseconds
   */
  public List<Object> summarize()
  {
    List<Object> rtnList = new ArrayList<Object>();

    //add end time
    rtnList.add(new Integer(_end));

    //add number of operations
    rtnList.add(new Integer(_succOpt));

    //add average latency
    long avgLatInNs;
    if(_succOpt == 0)
      avgLatInNs = 0;
    else
      avgLatInNs = _totalLat/_succOpt;
    double avgLatInMS = ((double) avgLatInNs) / 1000000;
    rtnList.add(new Double(avgLatInMS));

    //calculate 95% and 99% latencies
    long swappedOpt = 0;
    int lat95 = -1;
    int lat99 = -1;
    int currentLat = 0;
    long limit95 = _succOpt * 95L / 100L;
    long limit99 = _succOpt * 99L / 100L;

    while(lat99 == -1 && currentLat <= _histogramMax)
    {
      if(_map.containsKey(currentLat))
      {
        swappedOpt += _map.get(currentLat).longValue();

        if(lat95 == -1 && swappedOpt >= limit95)
        {
          lat95 = currentLat;
        }

        if(lat99 == -1 && swappedOpt >= limit99)
        {
          lat99 = currentLat;
        }
      }
      currentLat++;
    }

    //add 95% latency
    rtnList.add(new Integer(lat95));

    //add 99% latency
    rtnList.add(new Integer(lat99));

    return rtnList;
  }
}
