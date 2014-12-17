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

import com.linkedin.multitenant.common.Query.QueryType;
import com.linkedin.multitenant.db.Database.DatabaseResult;

public class Profiler
{
  private static final Logger _LOG = Logger.getLogger(Profiler.class);

  private CompleteResult _insertResult;
  private CompleteResult _readResult;
  private CompleteResult _deleteResult;
  private CompleteResult _updateResult;

  public Profiler(int runTime, int gran, int histogramSize)
  {
    _insertResult = new CompleteResult(runTime, gran, histogramSize);
    _readResult = new CompleteResult(runTime, gran, histogramSize);
    _deleteResult = new CompleteResult(runTime, gran, histogramSize);
    _updateResult = new CompleteResult(runTime, gran, histogramSize);
  }

  public Profiler(byte arr[]) throws Exception
  {
    ByteArrayInputStream bs = new ByteArrayInputStream(arr);
    DataInputStream in = new DataInputStream(bs);

    //read insert results size and data
    int size = in.readInt();
    _LOG.debug("Insert CompleteResult len: " + size);
    byte temp[] = new byte[size];
    in.readFully(temp);
    _insertResult = new CompleteResult(temp);

    //read read results size and data
    size = in.readInt();
    _LOG.debug("Read CompleteResult len: " + size);
    temp = new byte[size];
    in.readFully(temp);
    _readResult = new CompleteResult(temp);

    //read delete results size and data
    size = in.readInt();
    _LOG.debug("Delete CompleteResult len: " + size);
    temp = new byte[size];
    in.readFully(temp);
    _deleteResult = new CompleteResult(temp);

    //read update results size and data
    size = in.readInt();
    _LOG.debug("Update CompleteResult len: " + size);
    temp = new byte[size];
    in.readFully(temp);
    _updateResult = new CompleteResult(temp);

    in.close();
  }

  public byte[] toByteArray() throws Exception
  {
    ByteArrayOutputStream bs = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(bs);

    //write write results size and data
    byte temp[] = _insertResult.toByteArray();
    out.writeInt(temp.length);
    _LOG.debug("Write CompleteResult len: " + temp.length);
    out.write(temp);

    //write read results size and data
    temp = _readResult.toByteArray();
    out.writeInt(temp.length);
    _LOG.debug("Read CompleteResult len: " + temp.length);
    out.write(temp);

    //write delete results size and data
    temp = _deleteResult.toByteArray();
    out.writeInt(temp.length);
    _LOG.debug("Delete CompleteResult len: " + temp.length);
    out.write(temp);

    //write update results size and data
    temp = _updateResult.toByteArray();
    out.writeInt(temp.length);
    _LOG.debug("Update CompleteResult len: " + temp.length);
    out.write(temp);

    out.close();
    return bs.toByteArray();
  }

  public CompleteResult getInsertResults()
  {
    return _insertResult;
  }

  public CompleteResult getReadResults()
  {
    return _readResult;
  }

  public CompleteResult getDeleteResults()
  {
    return _deleteResult;
  }

  public CompleteResult getUpdateResults()
  {
    return _updateResult;
  }

  /**
   * Insert a completed operation data.
   * @param timeFromStart Time passed in seconds since the start of experiments
   * @param latency Latency in nanoseconds for the operation
   * @param optType Operation type
   */
  public void add(int timeFromStart, long latency, QueryType optType, DatabaseResult optResult)
  {
    switch (optType)
    {
      case INSERT:
        _insertResult.add(timeFromStart, latency, optResult);
        break;

      case READ:
        _readResult.add(timeFromStart, latency, optResult);
        break;

      case DELETE:
        _deleteResult.add(timeFromStart, latency, optResult);
        break;

      case UPDATE:
        _updateResult.add(timeFromStart, latency, optResult);
        break;

      default:
        _LOG.debug("Unknown operation type: " + optType);
        break;
    }
  }

  /**
   * Add rhs Profiler to this instance. The result is this instance.
   * @param rhs
   */
  public void add(Profiler rhs)
  {
    _insertResult.add(rhs.getInsertResults());
    _readResult.add(rhs.getReadResults());
    _deleteResult.add(rhs.getDeleteResults());
    _updateResult.add(rhs.getUpdateResults());
  }

  public String toString()
  {
    StringBuilder sb = new StringBuilder();

    sb.append("Insert:");
    sb.append("\n");
    sb.append(_insertResult.toString());

    sb.append("\n");

    sb.append("Read:");
    sb.append("\n");
    sb.append(_readResult.toString());

    sb.append("\n");

    sb.append("Delete:");
    sb.append("\n");
    sb.append(_deleteResult.toString());

    sb.append("\n");

    sb.append("Update:");
    sb.append("\n");
    sb.append(_updateResult.toString());

    return sb.toString();
  }
}
