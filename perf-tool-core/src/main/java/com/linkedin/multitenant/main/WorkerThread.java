/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.multitenant.main;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.multitenant.common.Constants;
import com.linkedin.multitenant.common.Query;
import com.linkedin.multitenant.common.Query.QueryType;
import com.linkedin.multitenant.db.Database;
import com.linkedin.multitenant.db.Database.DatabaseResult;
import com.linkedin.multitenant.profiler.Profiler;
import com.linkedin.multitenant.workload.Workload;
import com.linkedin.multitenant.workload.Workload.WorkloadResult;
import com.linkedin.multitenant.xml.XmlChange;
import com.linkedin.multitenant.xml.XmlJob;
import com.linkedin.multitenant.xml.XmlWorkPlan;

public class WorkerThread extends Thread
{
  private class Change
  {
    private int _time;
    private double _thr;

    public Change(int time, double thr)
    {
      _time = time;
      _thr = thr;
    }

    public int getTime()
    {
      return _time;
    }

    public double getThr()
    {
      return _thr;
    }
  }

  private static final Logger _log = Logger.getLogger(WorkerThread.class);

  public static final String FLAG_JOB_THR                     = "job.targetThr";
  public static final String FLAG_JOB_THREADS                 = "job.threads";

  public static final String FLAG_WORK_DATABASE_CLASS         = "work.databaseClass";
  public static final String FLAG_WORK_GRANULARITY            = "work.gran";
  public static final String FLAG_WORK_HISTOGRAM              = "work.histogramSize";
  public static final String FLAG_WORK_RUNTIME                = "work.runTime";
  public static final String FLAG_WORK_WORKLOAD_CLASS         = "work.workloadClass";

  //name of the job
  protected String _jobName;
  //total number of workers for this job. It is equal to numberOfMachines * threadsForThisJob
  protected int _numberOfWorkers;
  //id of this worker thread
  protected int _id;
  //running mode
  protected RunExperiment.Mode _mode;
  //total run time for the
  protected int _runTime;
  //target throughput per second
  protected double _targetThrSec;
  //changes in workload dynamically
  protected List<Change> _changeList;

  //statistics related variables
  protected int _optSucceeded;
  protected int _optFailed;
  protected long _sleepTime;
  protected long _startTime;

  //core variables
  protected Profiler _prof;
  protected Workload _work;
  protected Database _db;

  public WorkerThread(RunExperiment.Mode mode, int id, int numberOfWorkers, XmlWorkPlan xmlWork, XmlJob xmlJob) throws Exception
  {
    Map<String, String> workProperties = xmlWork.getProperties();
    Map<String, String> jobProperties = xmlJob.getProperties();

    //set job name
    String temp = jobProperties.get(RunExperiment.FLAG_JOB_NAME);
    if(temp == null)
    {
      _log.error("Job name is not specified");
      throw new Exception("Job name is not specified in workPlan xml");
    }
    else
    {
      _jobName = temp;
    }

    //set input parameters
    _id = id;
    _numberOfWorkers = numberOfWorkers;
    _mode = mode;

    //for debugging purposes
    String identifier = getIdentifier();

    //set run time of the experiment
    temp = workProperties.get(FLAG_WORK_RUNTIME);
    if(temp != null)
    {
      _runTime = Integer.parseInt(temp);
      _log.debug(identifier + ": Run time is set to " + _runTime + " seconds.");
    }
    else
    {
      if(_mode == RunExperiment.Mode.RUN)
      {
        _log.error(identifier + ": Run time for the experiment is not specified.");
        throw new Exception("Run time not specified");
      }
      else
      {
        _runTime = 100;
        _log.warn(identifier + ": Run time for the experiment is not specified.");
        _log.warn(identifier + ": Setting run time to 100 seconds by default for profiling purposes. This will not affect the loading phase.");
      }
    }

    //set target throughput
    temp = jobProperties.get(FLAG_JOB_THR);
    if(temp != null)
    {
      _targetThrSec = Double.parseDouble(temp);
      _log.debug(identifier + ": Target throughput per second is set to " + _targetThrSec);
    }
    else
    {
      _log.error(identifier + ": Target throughout per second is not given.");
      throw new Exception("Target throughput for the job " + getJobName() + " is not specified");
    }

    //set statisics related parameters
    _optSucceeded = 0;
    _optFailed = 0;
    _sleepTime = 0;

    //set granularity
    int gran;
    temp = workProperties.get(FLAG_WORK_GRANULARITY);
    if(temp != null)
    {
      gran = Integer.parseInt(temp);
      _log.debug(identifier + ": Granularity is set to " + gran);
    }
    else
    {
      gran = 10;
      _log.warn(identifier + ": Granularity is missing. It is set to 10 seconds by default");
    }

    //set histogram size
    int histogramSize;
    temp = workProperties.get(FLAG_WORK_HISTOGRAM);
    if(temp != null)
    {
      histogramSize = Integer.parseInt(temp);
      _log.debug(identifier + ": Histogram size is set to " + histogramSize);
    }
    else
    {
      histogramSize = 100;
      _log.warn(identifier + ": Histogram size is missing. It is set to 100 by default");
    }

    //set profiler
    _prof = new Profiler(_runTime, gran, histogramSize);

    //set workload
    temp = workProperties.get(FLAG_WORK_WORKLOAD_CLASS);
    if(temp == null)
    {
      _log.warn(identifier + ": Workload class is mising. It is set to com.linkedin.multitenant.workload.CoreWorkload by default");
      temp = "com.linkedin.multitenant.workload.CoreWorkload";
    }
    ClassLoader classLoader = WorkerThread.class.getClassLoader();
    @SuppressWarnings("rawtypes")
    Class w = classLoader.loadClass(temp);
    _work = (Workload) w.newInstance();
    _log.debug(identifier + ": Loaded workload class " + temp);
    WorkloadResult wRes = _work.init(_id, _numberOfWorkers, workProperties, jobProperties);
    if(wRes == WorkloadResult.FAIL)
    {
      _log.error(identifier + ": Workload init failed");
      throw new Exception("Workload init failed");
    }
    else
    {
      _log.debug(identifier + ": Init workload instance finished");
    }

    //set database
    temp = workProperties.get(FLAG_WORK_DATABASE_CLASS);
    if(temp == null)
    {
      _log.warn(identifier + ": Database class is missing. It is set to com.linkedin.multitenant.db.DiscardDatabase by default");
      temp = "com.linkedin.multitenant.db.DiscardDatabase";
    }
    @SuppressWarnings("rawtypes")
    Class d = classLoader.loadClass(temp);
    _db = (Database) d.newInstance();
    _log.debug(identifier + ": Loaded db class " + temp);
    DatabaseResult dRes = _db.init(workProperties, jobProperties);
    if(dRes == DatabaseResult.FAIL)
    {
      _log.error(identifier + ": Database init failed");
      throw new Exception("Database init failed");
    }
    else
    {
      _log.debug(identifier + ": Init db instance finished");
    }

    _changeList = new ArrayList<WorkerThread.Change>();
    List<XmlChange> changeList = xmlJob.getTimeline();
    for(int a = 0; a<changeList.size(); a++)
    {
      Change newData = new Change(changeList.get(a).getTime(), changeList.get(a).getTargetThr());
      _changeList.add(newData);
    }
  }

  public Profiler getProfiler()
  {
    return _prof;
  }

  public String getJobName()
  {
    return _jobName;
  }

  public int getOptSucceeded()
  {
    return _optSucceeded;
  }

  public int getOptFailed()
  {
    return _optFailed;
  }

  public long getSleepTime()
  {
    return _sleepTime;
  }

  private void clean()
  {
    String identifier = getIdentifier();
    _work.close();
    _log.debug(identifier + ": Closed workload");

    _db.close();
    _log.debug(identifier + ": Closed db");
  }

  public String getIdentifier()
  {
    return "Thr-" + _id + "_job-" + _jobName;
  }

  public void run()
  {
    String identifier = getIdentifier();

    //capture starting time in nanoseconds
    _startTime = System.nanoTime();
    _log.debug(identifier + ": Start time " + _startTime);

    //check the running mode
    switch (_mode)
    {
      case RUN:
        _log.debug(identifier + ": Running in RUN mode");
        runModeRun();
        break;
      case LOAD:
        _log.debug(identifier + ": Running in LOAD mode");
        runModeLoad();
        break;
      default:
        _log.error(identifier + ": Unknown run mode. Code shouldn't come here anyway.");
        break;
    }

    //close any opened resource
    clean();

    _log.debug(identifier + ": Finished execution");
  }

  public void runModeLoad()
  {
    int rowsResponsible = _work.getRowsResponsible();

    for(int a = 0; a<rowsResponsible; a++)
    {
      //generate insert query for load mode
      Query q = _work.generateInsertLoad();

      //record starting and ending times for the execution of query
      long st = System.nanoTime();
      DatabaseResult res = _db.doInsert(q);
      long en = System.nanoTime();

      //get latency in nanoseconds
      long latNS = en - st;
      //time since start in seconds
      int secondsFromStart = (int)((en - _startTime) / Constants.BILLION);

      //update temp statistics
      if(res == DatabaseResult.OK)
        _optSucceeded++;
      else
        _optFailed++;

      //add profiling
      _prof.add(secondsFromStart, latNS, QueryType.INSERT, res);
    }
  }

  public void runModeRun()
  {
    String identifier = getIdentifier();
    int queriesInLastSecond = 0;
    long lastSecondToHaveQuery = 0;

    double targetThrMs = _targetThrSec / Constants.THOUSAND;

    long endTime = _startTime + (_runTime * Constants.BILLION);
    //till the specified end time
    while(System.nanoTime() < endTime)
    {
      //generate query
      Query q = _work.generateTransaction();

      //record starting and ending times for the execution of query
      long st = System.nanoTime();
      DatabaseResult res;
      switch (q.getType())
      {
        case INSERT:
          res = _db.doInsert(q);
          break;
        case READ:
          res = _db.doRead(q);
          break;
        case DELETE:
          res = _db.doDelete(q);
          break;
        case UPDATE:
          res = _db.doUpdate(q);
          break;
        default:
          res = DatabaseResult.FAIL;
          break;
      }
      long en = System.nanoTime();

      //statistics about the current transaction
      long latNS = en - st;
      int secondsFromStart = (int) ((en - _startTime)/Constants.BILLION);

      //update temporary statistics
      if(res == DatabaseResult.OK)
        _optSucceeded++;
      else
        _optFailed++;

      //update profiler
      _prof.add(secondsFromStart, latNS, q.getType(), res);

      //should I throttle
      boolean shouldThrottle = false;
      if(secondsFromStart == lastSecondToHaveQuery)
      {
        /*
         * If I had operations in current second before, then add this newly executed query to statistics
         * and throttle depending on the given qps.
         */
        queriesInLastSecond++;
        shouldThrottle = true;
      }
      else if(queriesInLastSecond == _targetThrSec)
      {
        /*
         * If this is the first query to end in this new second and for the previous second I have executed
         * requested amount of queries, then update current second data, update opt done, and throttle
         */
        lastSecondToHaveQuery = secondsFromStart;
        queriesInLastSecond = 1;
        shouldThrottle = true;
      }
      else
      {
        /*
         * Else, that means the this executed query it belongs to the previous second, because query took too much time.
         * Hence, we don't need to throttle. Just update current second data.
         */
        lastSecondToHaveQuery = secondsFromStart;
        queriesInLastSecond = 0;
      }

      //Throttle
      if(shouldThrottle)
      {
        //First calculate the beginning of the current 1-sec epoch in nano seconds.
        long lastSecondHeadNs = _startTime + (lastSecondToHaveQuery * Constants.BILLION);

        //calculate the ms passed in this 1-sec epoch
        long millisecondsFromLastHead = (System.nanoTime() - lastSecondHeadNs) / Constants.MILLION;

        //while enough ms has passed, sleep for 1 ms
        while(millisecondsFromLastHead < (queriesInLastSecond / targetThrMs))
        {
          try
          {
            Thread.sleep(1);
            millisecondsFromLastHead = (System.nanoTime() - lastSecondHeadNs) / Constants.MILLION;
            _sleepTime += 1;
          }
          catch(Exception e)
          {
            millisecondsFromLastHead = (long) (queriesInLastSecond / targetThrMs);
          }
        }
      }

      //check if time has come to change throughput
      if(_changeList.size() > 0 && secondsFromStart >= _changeList.get(0).getTime())
      {
        _targetThrSec = _changeList.get(0).getThr();
        targetThrMs = _targetThrSec / Constants.THOUSAND;
        _log.warn(identifier + ": " + "Target opt/s is changed to " + _targetThrSec + " at time=" + secondsFromStart + "s");

        _changeList.remove(0);
      }
    }
  }
}
