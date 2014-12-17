/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.multitenant.main;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.multitenant.exporter.ConsoleExporter;
import com.linkedin.multitenant.exporter.DataExporter;
import com.linkedin.multitenant.profiler.Profiler;
import com.linkedin.multitenant.xml.XmlJob;
import com.linkedin.multitenant.xml.XmlParser;
import com.linkedin.multitenant.xml.XmlWorkPlan;

public class RunExperiment
{
  public enum Mode
  {
    LOAD, RUN
  }

  private static class SlavePair
  {
    public String host;
    public Integer port;
  }

  //Property flags
  public static final String FLAG_WORK_HOST                   = "work.host";
  public static final String FLAG_WORK_PORT                   = "work.port";
  public static final String FLAG_WORK_EXPORTER_CLASS         = "work.exporterClass";
  public static final String FLAG_WORK_STATUS_PERIOD          = "work.status.period";
  public static final String FLAG_JOB_NAME                    = "job.name";
  public static final String FLAG_JOB_ROW                     = "job.rowCount";

  //command line flags
  public static final String CMD_SLAVE      = "slave";
  public static final String CMD_PLAN       = "plan";
  public static final String CMD_SLAVEDATA  = "slaveData";
  public static final String CMD_LOAD       = "load";
  public static final String CMD_WAIT       = "wait";

  //default port slave connections
  public static final int SD_DEFAULT_PORT = 12981;

  private static final Logger _LOG = Logger.getLogger(RunExperiment.class);

  /**
   * Processes command line options.<br>
   * Allowed commands are:<br>
   * -slave=PORT: This machine is a slave machine.<br>
   * -plan=PATH: Workload plan file is given at location PATH
   * -slaveData=PATH: Slave information file is given at location PATH
   * -wait=SECONDS: One time waiting before thread execution
   * -load: Insert data to database. If this is not specified, then run workload.
   * @param args Command line options
   * @return If input is valid, then returns a map instance containing parameters.<br>
   * Otherwise returns null.
   */
  private static Map<String, String> processCommandLine(String [] args)
  {
    Map<String, String> result = new HashMap<String, String>();

    for(int a = 0; a<args.length; a++)
    {
      if(args[a].startsWith("-" + CMD_SLAVEDATA))
      {
        String parts[] = args[a].split("=");
        if(parts.length == 1)
        {
          _LOG.error("file is not specified for slave data");
          return null;
        }
        else if(parts.length > 3)
        {
          _LOG.error("multiple = character for slave data");
          return null;
        }
        else
        {
          result.put(CMD_SLAVEDATA, parts[1]);
          _LOG.info("Read from console: " + CMD_SLAVEDATA + " " + parts[1]);
        }
      }
      else if(args[a].startsWith("-" + CMD_WAIT))
      {
        String parts[] = args[a].split("=");
        if(parts.length == 1)
        {
          _LOG.error("waiting time is not specified");
          return null;
        }
        else
        {
          result.put(CMD_WAIT, parts[1]);
          _LOG.info("Read from console: " + CMD_WAIT + " " + parts[1]);
        }
      }
      else if(args[a].startsWith("-" + CMD_PLAN))
      {
        String parts[] = args[a].split("=");
        if(parts.length == 1)
        {
          _LOG.error("file is not specified for work plan");
          return null;
        }
        else if(parts.length > 3)
        {
          _LOG.error("multiple = character for work plan");
          return null;
        }
        else
        {
          result.put(CMD_PLAN, parts[1]);
          _LOG.info("Read from console: " + CMD_PLAN + " " + parts[1]);
        }
      }
      else if(args[a].startsWith("-" + CMD_SLAVE))
      {
        String parts[] = args[a].split("=");
        String port = null;
        if(parts.length == 1)
        {
          _LOG.warn("port number is set to default, " + SD_DEFAULT_PORT);
          port = String.valueOf(SD_DEFAULT_PORT);
        }
        else if(parts.length == 2)
        {
          port = parts[1];
        }
        else
        {
          _LOG.error("Slave port number is not assigned correctly");
          return null;
        }

        result.put(CMD_SLAVE, port);
        _LOG.info("Read from console: " + CMD_SLAVE + " " + port);
      }
      else if(args[a].startsWith("-" + CMD_LOAD))
      {
        result.put(CMD_LOAD, "");
        _LOG.info("Read from console: " + CMD_LOAD);
      }
      else
      {
        _LOG.error("Unknown option: " + args[a]);
        return null;
      }
    }

    return result;
  }

  /**
   * Processes slave data file to get host name and port number for each slave.
   * @param filename Path to the slave data file.
   * @return If input is valid, then returns the mapping.<br>
   * Otherwise returns null.
   */
  private static List<SlavePair> processSlaveData(String filename) throws Exception
  {
    List<SlavePair> result = new ArrayList<SlavePair>();

    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
    String line = reader.readLine();
    while(line != null)
    {
      String parsed[] = line.split(":");

      if(parsed.length == 2)
      {
        String host = parsed[0];
        Integer port = Integer.parseInt(parsed[1]);

        SlavePair newPair = new SlavePair();
        newPair.host = host;
        newPair.port = port;

        result.add(newPair);
        _LOG.info("Slave info: " + host + " : " + port);
      }
      else if(parsed.length == 1)
      {
        String host = parsed[0];
        Integer port = new Integer(SD_DEFAULT_PORT);

        SlavePair newPair = new SlavePair();
        newPair.host = host;
        newPair.port = port;

        result.add(newPair);
        _LOG.info("Slave info: " + host + " : " + SD_DEFAULT_PORT);
      }
      else
      {
        _LOG.error("Slave data should be HOST:PORT");
        reader.close();
        return null;
      }

      line = reader.readLine();
    }

    reader.close();
    return result;
  }

  public static void main(String[] args) throws Exception
  {
    //read command line options
    Map<String, String> cmdOptions = processCommandLine(args);
    if(cmdOptions == null)
    {
      _LOG.fatal("Cannot process command line. Closing.");
      return;
    }

    //check waiting time
    if(cmdOptions.containsKey(CMD_WAIT))
    {
      int waitTimeSeconds = Integer.parseInt(cmdOptions.get(CMD_WAIT));
      if(waitTimeSeconds > 0)
      {
        _LOG.warn("Sleeping for " + waitTimeSeconds + " seconds");
        Thread.sleep(waitTimeSeconds * 1000);
        _LOG.warn("Woke up from the sleep. Now starting proxy.");
      }
    }

    //some variables
    boolean isMaster = !(cmdOptions.containsKey(CMD_SLAVE));
    List<SlavePair> slaveData = null;
    List<Socket> sockList = new ArrayList<Socket>();
    byte workPlanData[] = null;
    int machineId;
    int machineCount = 0;
    Mode mode;
    if(cmdOptions.containsKey(CMD_LOAD))
      mode = Mode.LOAD;
    else
      mode = Mode.RUN;

    //read slave information
    if(isMaster)
    {
      String slaveDataFilename = cmdOptions.get(CMD_SLAVEDATA);
      if(slaveDataFilename != null)
      {
        slaveData = processSlaveData(slaveDataFilename);
        if(slaveData == null)
        {
          _LOG.fatal("Cannot process slave data file");
          return;
        }
      }
      else
      {
        slaveData = new ArrayList<SlavePair>();
      }

      machineCount = slaveData.size() + 1;
    }

    //exchange work plan
    if(isMaster)
    {
      //if I am the server, then send work plan to all slave machines if any
      _LOG.info("This machine is master");

      machineId = 0;

      //read config data to byte array
      String planFilename = cmdOptions.get(CMD_PLAN);
      if(planFilename == null)
      {
        _LOG.fatal("Plan file is not specified");
        return;
      }
      else
      {
        File f = new File(planFilename);
        FileInputStream fs = new FileInputStream(f);
        workPlanData = new byte[(int)f.length()];
        fs.read(workPlanData);
        fs.close();
       }

      for(int a = 0; a<slaveData.size(); a++)
      {
        int clientId = a+1;
        String host = slaveData.get(a).host;
        int port = slaveData.get(a).port.intValue();

        Socket newSock = new Socket(host, port);
        sockList.add(newSock);

        //send this client's ID
        DataOutputStream outStr = new DataOutputStream(newSock.getOutputStream());
        outStr.writeInt(clientId);
        _LOG.debug("Sent ID to client " + clientId);

        //send number of machines
        outStr.writeInt(machineCount);
        _LOG.debug("Sent machine count to client " + clientId);

        //send mode
        outStr.writeUTF(mode.name());
        _LOG.debug("Sent mode to client " + clientId);

        //send work plan data
        outStr.writeInt(workPlanData.length);
        outStr.write(workPlanData);
        _LOG.debug("Sent work plan data to client " + clientId);
      }
    }
    else
    {
      //if I am a slave, then listen to the master for work plan.
      _LOG.info("This machine is slave");

      //listen from the specified port
      int port = Integer.parseInt(cmdOptions.get(CMD_SLAVE));
      ServerSocket listener = new ServerSocket(port);
      Socket serverSock = listener.accept();
      sockList.add(serverSock);
      listener.close();

      //get my Id from the master
      DataInputStream inStr = new DataInputStream(serverSock.getInputStream());
      machineId = inStr.readInt();
      _LOG.info("Recevied machine ID from master, which is " + machineId);

      //get number of machines from the master
      machineCount = inStr.readInt();
      _LOG.info("Received number of machines from master, which is " + machineCount);

      //get mode from the master
      mode = Mode.valueOf(inStr.readUTF());
      _LOG.info("Received mode from master, which is " + mode);

      //get work plan data from the master
      int workPlanSize = inStr.readInt();
      workPlanData = new byte[workPlanSize];
      inStr.readFully(workPlanData);
      _LOG.info("Received work plan data from master");
    }

    //read config
    _LOG.info("size of workPlanData: " + workPlanData.length);
    XmlWorkPlan xmlWork = XmlParser.parseWorkPlan(workPlanData);
    _LOG.info("Read work plan:");
    _LOG.info(xmlWork.toString());

    //create worker threads
    List<WorkerThread> threadList = new ArrayList<WorkerThread>();
    List<XmlJob> jobList = xmlWork.getJobList();
    for(int a = 0; a<jobList.size(); a++)
    {
      XmlJob xmlCurrentJob = jobList.get(a);
      String jobName = getParamStr(xmlCurrentJob.getProperties(), FLAG_JOB_NAME);
      if(jobName == null)
      {
        _LOG.fatal("Job name is missing. Closing.");
        return;
      }

      int threadCount = getParamInt(xmlCurrentJob.getProperties(), WorkerThread.FLAG_JOB_THREADS);
      if(threadCount == -1)
      {
        _LOG.fatal("ThreadCount is missing for job " + jobName + ". Closing.");
        return;
      }
      else
      {
        _LOG.debug("Number of threads for job " + jobName + " is " + threadCount);
      }
      int numberOfWorkers = machineCount * threadCount;

      for(int b = 0; b<threadCount; b++)
      {
        int threadId = (machineId * threadCount) + b;
        WorkerThread newThr = new WorkerThread(mode, threadId, numberOfWorkers, xmlWork, xmlCurrentJob);
        threadList.add(newThr);

        _LOG.debug("Added thread: " + newThr.getIdentifier());
      }
    }

    _LOG.debug("Number of total threads: " + threadList.size());

    //create status thread
    int statusPeriod = getParamInt(xmlWork.getProperties(), FLAG_WORK_STATUS_PERIOD);
    if(statusPeriod == -1)
      statusPeriod = 10;
    StatusThread statThread = new StatusThread(threadList, statusPeriod);

    //barrier to sync execution time
    if(isMaster)
    {
      for(int a = 0; a<sockList.size(); a++)
      {
        Socket slaveSock = sockList.get(a);
        DataOutputStream out = new DataOutputStream(slaveSock.getOutputStream());

        out.writeInt(1);
      }
    }
    else
    {
      Socket masterSock = sockList.get(0);
      DataInputStream in = new DataInputStream(masterSock.getInputStream());

      in.readInt();
    }

    //start all threads
    for(int a = 0; a<threadList.size(); a++)
      threadList.get(a).start();
    statThread.start();

    _LOG.info("Started worker threads");

    //join all threads
    for(int a = 0; a<threadList.size(); a++)
      threadList.get(a).join();

    _LOG.info("Joined worker threads");

    //stop status thread
    statThread.clear();
    statThread.join();

    _LOG.info("Joined status thread");

    //join thread-wide results to get machine-wide result
    _LOG.info("Combining thread-wide results");
    Map<String, Profiler> profilerMap = new HashMap<String, Profiler>();
    Map<String, Long> optMap = new HashMap<String, Long>();
    Map<String, Long> sleepMap = new HashMap<String, Long>();
    for(int a = 0; a<threadList.size(); a++)
    {
      String jobName = threadList.get(a).getJobName();
      long jobOpt = (long) threadList.get(a).getOptSucceeded();
      long jobSleep = threadList.get(a).getSleepTime();
      Profiler p = threadList.get(a).getProfiler();

      if(profilerMap.containsKey(jobName))
      {
        Profiler prevProf = profilerMap.get(jobName);
        prevProf.add(p);
        profilerMap.put(jobName, prevProf);

        long prevOpt = optMap.get(jobName).longValue();
        optMap.put(jobName, prevOpt + jobOpt);

        long prevSleep = sleepMap.get(jobName).longValue();
        sleepMap.put(jobName, prevSleep + jobSleep);
      }
      else
      {
        profilerMap.put(jobName, p);
        optMap.put(jobName, jobOpt);
        sleepMap.put(jobName, jobSleep);
      }
    }
    _LOG.info("Combined thread-wide results");

    Iterator<String> itrJob = optMap.keySet().iterator();
    while(itrJob.hasNext())
    {
      String curJob = itrJob.next();

      _LOG.info("job=" + curJob + " opt=" + optMap.get(curJob) + " sleep=" + sleepMap.get(curJob));
    }

    //combine machine-wide results
    if(isMaster)
    {
      //if i am server, get machine-wide results from other machines
      for(int a = 0; a<sockList.size(); a++)
      {
        Socket clientSock = sockList.get(a);
        DataOutputStream outStr = new DataOutputStream(clientSock.getOutputStream());
        DataInputStream inStr = new DataInputStream(clientSock.getInputStream());

        outStr.writeInt(3);

        //get machine-wide results
        int whoIs = inStr.readInt();
        _LOG.info("Got machine id: " + whoIs);

        int size = inStr.readInt();
        _LOG.info("Got number of jobs: " + size);

        for(int b = 0; b<size; b++)
        {
          String jobName = inStr.readUTF();
          _LOG.info("Got job name: " + jobName);

          int dataSize = inStr.readInt();
          _LOG.info("Got profiler byte length: " + dataSize);

          byte data[] = new byte[dataSize];
          inStr.readFully(data);
          _LOG.info("Got profiler data");

          Profiler newProf = new Profiler(data);
          if(profilerMap.containsKey(jobName))
          {
            Profiler oldProf = profilerMap.get(jobName);
            oldProf.add(newProf);
            profilerMap.put(jobName, oldProf);
          }
          else
          {
            profilerMap.put(jobName, newProf);
          }
        }

        outStr.writeInt(3);

        _LOG.info("Finished getting machine-wide results from machine-" + whoIs);

        clientSock.close();
        _LOG.info("Socket to machine " + whoIs + " is closed");
      }
    }
    else
    {
      //else, send my machine-wide result to the server
      Socket servSock = sockList.get(0);

      //send machine-wide results
      DataOutputStream out = new DataOutputStream(servSock.getOutputStream());
      DataInputStream in = new DataInputStream(servSock.getInputStream());

      in.readInt();

      //write size of profilerMap
      out.writeInt(machineId);
      _LOG.info("Sent machine id: " + machineId);

      out.writeInt(profilerMap.size());
      _LOG.info("Sent number of jobs: " + profilerMap.size());

      Iterator<String> itr = profilerMap.keySet().iterator();
      while(itr.hasNext())
      {
        String jobName = itr.next();
        byte data[] = profilerMap.get(jobName).toByteArray();

        out.writeUTF(jobName);
        _LOG.info("Sent job name: " + jobName);

        out.writeInt(data.length);
        _LOG.info("Sent profiler byte length " + data.length);

        out.write(data);
        _LOG.info("Sent profiler data");
      }

      in.readInt();
      _LOG.info("Socket to the master is closed");
    }

    if(isMaster)
    {
      DataExporter exp = null;
      ClassLoader classLoader = RunExperiment.class.getClassLoader();

      try
      {
        String exporterClass = getParamStr(xmlWork.getProperties(), FLAG_WORK_EXPORTER_CLASS);
        if(exporterClass == null)
        {
          _LOG.warn("Exporter class is changed to com.linkedin.multitenant.exporter.ConsoleExporter by default");
          exporterClass = "com.linkedin.multitenant.exporter.ConsoleExporter";
        }

        @SuppressWarnings("rawtypes")
        Class expClass = classLoader.loadClass(exporterClass);
        exp = (DataExporter) expClass.newInstance();
      }
      catch(Exception e)
      {
        _LOG.error("Error loading exporter", e);
        exp = new ConsoleExporter();
      }

      exp.init(xmlWork.getProperties(), profilerMap);
      exp.export();
    }

    _LOG.info("Closing...");
  }

  private static String getParamStr(Map<String, String> properties, String propertyName)
  {
    String val = properties.get(propertyName);
    if(val == null)
      return null;
    else
      return val;
  }

  private static int getParamInt(Map<String, String> properties, String propertyName)
  {
    String val = properties.get(propertyName);
    if(val == null)
      return -1;
    else
      return Integer.parseInt(val);
  }
}
