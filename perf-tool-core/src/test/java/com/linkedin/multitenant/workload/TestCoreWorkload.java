/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.multitenant.workload;

import java.util.HashMap;
import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.linkedin.multitenant.common.Query;
import com.linkedin.multitenant.main.RunExperiment;

public class TestCoreWorkload
{
  private static Workload w;

  @BeforeClass
  public static void prepare()
  {
    int myId = 1;
    int numberOfWorkers = 10;
    Map<String, String> jobProp = new HashMap<String, String>();

    jobProp.put(RunExperiment.FLAG_JOB_NAME, "job1");
    jobProp.put(RunExperiment.FLAG_JOB_ROW, "100");

    jobProp.put(CoreWorkload.FLAG_JOB_INSERT_RATE, "0.30");
    jobProp.put(CoreWorkload.FLAG_JOB_READ_RATE, "0.20");
    jobProp.put(CoreWorkload.FLAG_JOB_DELETE_RATE, "0.20");
    jobProp.put(CoreWorkload.FLAG_JOB_UPDATE_RATE, "0.30");

    jobProp.put(CoreWorkload.FLAG_JOB_VALUE_SIZE, "15");
    jobProp.put(CoreWorkload.FLAG_JOB_VALUE_SIZE_MIN, "5");
    jobProp.put(CoreWorkload.FLAG_JOB_VALUE_SIZE_DISTRIBUTION, "uniform");

    w = new CoreWorkload();
    w.init(myId, numberOfWorkers, null, jobProp);
  }

  @AfterClass
  public static void closeDown()
  {
    w.close();
  }

  @Test
  public void testLoad()
  {
    try
    {
      System.out.println("**********");
      int testSize = 100;
      for(int a = 0; a<testSize; a++)
      {
        Query q = w.generateInsertLoad();
        System.out.println(q.toString());
      }
      System.out.println("**********");
    }
    catch(Exception e)
    {
      e.printStackTrace();
    }
  }

  @Test
  public void testTransaction()
  {
    try
    {
      System.out.println("**********");
      int testSize = 100;
      for(int a = 0; a<testSize; a++)
      {
        Query q = w.generateTransaction();
        System.out.println(q.toString());
      }
      System.out.println("**********");
    }
    catch(Exception e)
    {
      e.printStackTrace();
    }
  }
}
