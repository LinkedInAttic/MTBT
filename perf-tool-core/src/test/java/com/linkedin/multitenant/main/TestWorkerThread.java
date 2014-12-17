/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.multitenant.main;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.linkedin.multitenant.profiler.Profiler;
import com.linkedin.multitenant.xml.XmlJob;
import com.linkedin.multitenant.xml.XmlParser;
import com.linkedin.multitenant.xml.XmlWorkPlan;

public class TestWorkerThread
{
  @Test
  public void testLoad() throws Exception
  {
    String inputData =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<workPlan>" +
              "<property name=\"work.gran\" value=\"10\"/>" +
              "<property name=\"work.status.period\" value=\"2\"/>" +
              "<job>" +
                "<property name=\"job.name\" value=\"job-1\"/>" +
                "<property name=\"job.threads\" value=\"2\"/>" +
                "<property name=\"job.targetThr\" value=\"5\"/>" +
                "<property name=\"job.rowCount\" value=\"10\"/>" +
                "<property name=\"job.valueSize\" value=\"10\"/>" +
                "<timeline>" +
                  "<change at=\"120\" to=\"40\"/>" +
                  "<change at=\"150\" to=\"30\"/>" +
                "</timeline>" +
              "</job>" +
            "</workPlan>";

    byte[] workPlanData = inputData.getBytes();
    XmlWorkPlan xmlWork;

    try
    {
      xmlWork = XmlParser.parseWorkPlan(workPlanData);
      if(xmlWork == null)
        System.out.println("parsing returned null");
      else
        System.out.println(xmlWork.toString());
    }
    catch(Exception e)
    {
      e.printStackTrace();
      return;
    }

    String temp;
    List<WorkerThread> thrList = new ArrayList<WorkerThread>();
    List<XmlJob> jobList = xmlWork.getJobList();
    for(int a = 0; a<jobList.size(); a++)
    {
      XmlJob xmlJob = xmlWork.getJobList().get(a);
      String jobName;
      int threads;

      temp = xmlJob.getProperties().get(RunExperiment.FLAG_JOB_NAME);
      if(temp == null)
      {
        System.out.println("Job name is not specified");
        continue;
      }
      else
      {
        jobName = temp;
      }

      temp = xmlJob.getProperties().get(WorkerThread.FLAG_JOB_THREADS);
      if(temp == null)
      {
         System.out.println("Number of threads for job " + jobName + " is not specified");
         continue;
      }
      else
      {
        threads = Integer.parseInt(temp);
      }

      for(int b = 0; b<threads; b++)
      {
        WorkerThread thr = new WorkerThread(RunExperiment.Mode.LOAD, b, threads, xmlWork, xmlJob);
        thrList.add(thr);
      }
    }

    temp = xmlWork.getProperties().get(RunExperiment.FLAG_WORK_STATUS_PERIOD);
    int statusPeriod = 10;
    if(temp != null)
    {
      statusPeriod = Integer.parseInt(temp);
    }

    StatusThread statThread = new StatusThread(thrList, statusPeriod);

    for(int a = 0; a<thrList.size(); a++)
      thrList.get(a).start();
    statThread.start();

    for(int a = 0; a<thrList.size(); a++)
      thrList.get(a).join();
    statThread.clear();
    statThread.join();

    System.out.println("Joined all threads");

    Map<String, Profiler> profilerMap = new HashMap<String, Profiler>();
    for(int a = 0; a<thrList.size(); a++)
    {
      String jobName = thrList.get(a).getJobName();
      Profiler p = thrList.get(a).getProfiler();

      if(profilerMap.containsKey(jobName))
      {
        Profiler prevProf = profilerMap.get(jobName);
        prevProf.add(p);

        profilerMap.put(jobName, prevProf);
      }
      else
      {
        profilerMap.put(jobName, p);
      }
    }

    Iterator<String> itr = profilerMap.keySet().iterator();
    while(itr.hasNext())
    {
      String jobName = itr.next();
      Profiler jobProf = profilerMap.get(jobName);

      System.out.println("Profiler for job=" + jobName);
      System.out.println(jobProf.toString());
    }

    for(int a = 0; a<thrList.size(); a++)
    {
      System.out.println("Thread-" + thrList.get(a).getIdentifier() + " succeeded opt " + thrList.get(a).getOptSucceeded());
      System.out.println("Thread-" + thrList.get(a).getIdentifier() + " failed opt " + thrList.get(a).getOptFailed());
      System.out.println("Thread-" + thrList.get(a).getIdentifier() + " slept for " + thrList.get(a).getSleepTime() + "ms");
    }
  }

  @Test
  public void testRun() throws Exception
  {
    String inputData =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<workPlan>" +
              "<property name=\"work.runTime\" value=\"10\"/>" +
              "<property name=\"work.gran\" value=\"2\"/>" +
              "<property name=\"work.status.period\" value=\"2\"/>" +
              "<job>" +
                "<property name=\"job.name\" value=\"job-1\"/>" +
                "<property name=\"job.threads\" value=\"2\"/>" +
                "<property name=\"job.targetThr\" value=\"5\"/>" +
                "<property name=\"job.rowCount\" value=\"10\"/>" +
                "<property name=\"job.valueSize\" value=\"10\"/>" +
                "<timeline>" +
                  "<change at=\"4\" to=\"100\"/>" +
                  "<change at=\"6\" to=\"5\"/>" +
                "</timeline>" +
              "</job>" +
            "</workPlan>";

    byte[] workPlanData = inputData.getBytes();
    XmlWorkPlan xmlWork;

    try
    {
      xmlWork = XmlParser.parseWorkPlan(workPlanData);
      if(xmlWork == null)
        System.out.println("parsing returned null");
      else
        System.out.println(xmlWork.toString());
    }
    catch(Exception e)
    {
      e.printStackTrace();
      return;
    }

    String temp;
    List<WorkerThread> thrList = new ArrayList<WorkerThread>();
    List<XmlJob> jobList = xmlWork.getJobList();
    for(int a = 0; a<jobList.size(); a++)
    {
      XmlJob xmlJob = xmlWork.getJobList().get(a);
      String jobName;
      int threads;

      temp = xmlJob.getProperties().get(RunExperiment.FLAG_JOB_NAME);
      if(temp == null)
      {
        System.out.println("Job name is not specified");
        continue;
      }
      else
      {
        jobName = temp;
      }

      temp = xmlJob.getProperties().get(WorkerThread.FLAG_JOB_THREADS);
      if(temp == null)
      {
         System.out.println("Number of threads for job " + jobName + " is not specified");
         continue;
      }
      else
      {
        threads = Integer.parseInt(temp);
      }

      for(int b = 0; b<threads; b++)
      {
        WorkerThread thr = new WorkerThread(RunExperiment.Mode.RUN, b, threads, xmlWork, xmlJob);
        thrList.add(thr);
      }
    }

    temp = xmlWork.getProperties().get(RunExperiment.FLAG_WORK_STATUS_PERIOD);
    int statusPeriod = 10;
    if(temp != null)
    {
      statusPeriod = Integer.parseInt(temp);
    }

    StatusThread statThread = new StatusThread(thrList, statusPeriod);

    for(int a = 0; a<thrList.size(); a++)
      thrList.get(a).start();
    statThread.start();

    for(int a = 0; a<thrList.size(); a++)
      thrList.get(a).join();
    statThread.clear();
    statThread.join();

    System.out.println("Joined all threads");

    Map<String, Profiler> profilerMap = new HashMap<String, Profiler>();
    for(int a = 0; a<thrList.size(); a++)
    {
      String jobName = thrList.get(a).getJobName();
      Profiler p = thrList.get(a).getProfiler();

      if(profilerMap.containsKey(jobName))
      {
        Profiler prevProf = profilerMap.get(jobName);
        prevProf.add(p);

        profilerMap.put(jobName, prevProf);
      }
      else
      {
        profilerMap.put(jobName, p);
      }
    }

    Iterator<String> itr = profilerMap.keySet().iterator();
    while(itr.hasNext())
    {
      String jobName = itr.next();
      Profiler jobProf = profilerMap.get(jobName);

      System.out.println("Profiler for job=" + jobName);
      System.out.println(jobProf.toString());
    }

    for(int a = 0; a<thrList.size(); a++)
    {
      System.out.println("Thread-" + thrList.get(a).getIdentifier() + " succeeded opt " + thrList.get(a).getOptSucceeded());
      System.out.println("Thread-" + thrList.get(a).getIdentifier() + " failed opt " + thrList.get(a).getOptFailed());
      System.out.println("Thread-" + thrList.get(a).getIdentifier() + " slept for " + thrList.get(a).getSleepTime() + "ms");
    }
  }
}
