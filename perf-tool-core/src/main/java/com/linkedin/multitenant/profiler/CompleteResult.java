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

import org.apache.log4j.Logger;

import com.linkedin.multitenant.db.Database.DatabaseResult;

public class CompleteResult
{
  private static final Logger _LOG = Logger.getLogger(CompleteResult.class);

  private int _gran;
  private EpochResult _arr[];

  public CompleteResult(int runTime, int gran, int histogramSize)
  {
    _gran = gran;

    int slots = (runTime + gran - 1) / gran;
    _LOG.trace("Number of epochs is " + slots);

    _arr = new EpochResult[slots];
    for(int a = 0; a<slots; a++)
    {
      int start = a * gran;
      int end = (a+1) * gran;
      _arr[a] = new EpochResult(histogramSize, start, end);
    }
  }

  public CompleteResult(byte arr[]) throws Exception
  {
    ByteArrayInputStream bs = new ByteArrayInputStream(arr);
    DataInputStream in = new DataInputStream(bs);

    //read gran
    _gran = in.readInt();
    _LOG.trace("Gran: " + _gran);
    //read number of EpochResul
    int size = in.readInt();
    _LOG.trace("Number of epochs: " + size);
    _arr = new EpochResult[size];
    //read each Epochresult
    for(int a = 0; a<size; a++)
    {
      //read size of byte array
      int s = in.readInt();
      _LOG.trace("epoch-" + a + " size: " + s);

      byte epochData[] = new byte[s];
      //read data
      in.readFully(epochData);

      _arr[a] = new EpochResult(epochData);
    }

    in.close();
  }

  public CompleteResult copy() throws Exception
  {
    byte[] temp = toByteArray();

    return new CompleteResult(temp);
  }

  public byte[] toByteArray() throws Exception
  {
    ByteArrayOutputStream bs = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bs);

    //write gran
    out.writeInt(_gran);
    _LOG.trace("Gran: " + _gran);
    //write number of EpochResult
    out.writeInt(_arr.length);
    _LOG.trace("Number of epochs: " + _arr.length);
    //write each EpochResult
    for(int a = 0; a<_arr.length; a++)
    {
      byte tempArr[] = _arr[a].toByteArray();

      //write length
      out.writeInt(tempArr.length);
      _LOG.trace("epoch-" + a + " size: " + tempArr.length);

      //write data
      out.write(tempArr);
    }

    out.close();
    return bs.toByteArray();
  }

  public EpochResult[] getArr()
  {
    return _arr;
  }

  /**
   * Add operation data that finished at TimeFromStart(s) with latency(ms).
   * @param timeFromStart Time passed in seconds since the start of experiment
   * @param latency Latency in nanoseconds for the operation
   * @param optResult Result of the operation
   */
  public void add(int timeFromStart, long latency, DatabaseResult optResult)
  {
    int slot = timeFromStart / _gran;

    if(0 <= slot && slot < _arr.length)
      _arr[slot].add(latency, optResult);
    else
      _LOG.trace("Invalid slot");
  }

  /**
   * Add rhs CompleteResult to this instance. The result is this instance.
   * @param rhs
   */
  public void add(CompleteResult rhs)
  {
    EpochResult rhsArr[] = rhs.getArr();

    if(rhsArr.length != _arr.length)
      return;

    for(int a = 0; a<rhsArr.length; a++)
      _arr[a].add(rhsArr[a]);
  }

  public String toString()
  {
    StringBuilder sb = new StringBuilder();

    for(int a = 0; a<_arr.length; a++)
    {
      sb.append(_arr[a].toString());
      sb.append("\n");
    }

    return sb.toString();
  }
}
