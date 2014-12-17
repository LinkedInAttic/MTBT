/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.multitenant.workload;

import java.util.Map;
import java.util.Random;

import org.apache.log4j.Logger;

import com.linkedin.multitenant.common.Query;
import com.linkedin.multitenant.common.Query.QueryType;
import com.linkedin.multitenant.main.RunExperiment;
import com.yahoo.ycsb.Utils;
import com.yahoo.ycsb.generator.ConstantIntegerGenerator;
import com.yahoo.ycsb.generator.CounterGenerator;
import com.yahoo.ycsb.generator.DiscreteGenerator;
import com.yahoo.ycsb.generator.HotspotIntegerGenerator;
import com.yahoo.ycsb.generator.IntegerGenerator;
import com.yahoo.ycsb.generator.ScrambledZipfianGenerator;
import com.yahoo.ycsb.generator.SkewedLatestGenerator;
import com.yahoo.ycsb.generator.UniformIntegerGenerator;
import com.yahoo.ycsb.generator.UpdatedUniformIntegerGenerator;

public class CoreWorkload implements Workload
{
  public static final String FLAG_JOB_INSERT_RATE             = "job.insertRate";
  public static final String FLAG_JOB_READ_RATE               = "job.readRate";
  public static final String FLAG_JOB_DELETE_RATE             = "job.deleteRate";
  public static final String FLAG_JOB_UPDATE_RATE             = "job.updateRate";
  public static final String FLAG_JOB_QUERY_DISTRIBUTION      = "job.queryDist";
  public static final String FLAG_JOB_HOTSPOT_SETFRAC         = "job.queryDist.hotSpot.setFrac";
  public static final String FLAG_JOB_HOTSPOT_OPNFRAC         = "job.queryDist.hotSpot.optFrac";
  public static final String FLAG_JOB_VALUE_SIZE_DISTRIBUTION = "job.valueSize.dist";
  public static final String FLAG_JOB_VALUE_SIZE              = "job.valueSize";
  public static final String FLAG_JOB_VALUE_SIZE_MIN          = "job.valueSize.min";

  public static final String CONST_QUERY_INSERT = "in";
  public static final String CONST_QUERY_READ = "re";
  public static final String CONST_QUERY_DELETE = "de";
  public static final String CONST_QUERY_UPDATE = "up";
  public static final String CONST_DIST_UNIFORM = "uniform";
  public static final String CONST_DIST_ZIPFIAN = "zipfian";
  public static final String CONST_DIST_LATEST = "latest";
  public static final String CONST_DIST_HOTSPOT = "hotspot";
  public static final String CONST_DIST_CONSTANT = "constant";

  private static final Logger _LOG = Logger.getLogger(CoreWorkload.class);

  protected int _id;
  protected DiscreteGenerator _operationGen;
  protected IntegerGenerator _loadInsertKeyGen;
  protected IntegerGenerator _transactionKeyGen;
  protected IntegerGenerator _transactionInsertKeyGen;
  protected IntegerGenerator _valueGen;
  protected Random _ranGen;
  protected int _rowsResponsible;

  public int getRowsResponsible()
  {
    return _rowsResponsible;
  }

  @Override
  public WorkloadResult init(int myId, int numberOfWorkers, Map<String, String> workPlanProperties, Map<String, String> jobProperties)
  {
    //get job name
    String jobName;
    String temp = jobProperties.get(RunExperiment.FLAG_JOB_NAME);
    if(temp == null)
    {
      _LOG.error("Job name is missing");
      return WorkloadResult.FAIL;
    }
    else
    {
      jobName = temp;
    }

    //set id
    _id = myId;

    //set key generator for insert operations in Load phase
    _loadInsertKeyGen = new CounterGenerator(0);

    //set key generator for insert operations in Run phase
    temp = jobProperties.get(RunExperiment.FLAG_JOB_ROW);
    if(temp != null)
    {
      int rowCount = Integer.parseInt(temp);
      _LOG.debug("Number of rows for job " + jobName + " is " + rowCount);
      _rowsResponsible = rowCount / numberOfWorkers;
      _LOG.debug("Thread " + _id + " for job " + jobName + " is responsible for " + _rowsResponsible + " rows");
      _transactionInsertKeyGen = new CounterGenerator(rowCount);
    }
    else
    {
      _LOG.error("Number of rows for job " + jobName + " is not specified.");
      return WorkloadResult.FAIL;
    }

    //set operation distributions
    _operationGen = new DiscreteGenerator();
    float insertRate;
    float readRate;
    float deleteRate;
    float updateRate;
    temp = jobProperties.get(FLAG_JOB_INSERT_RATE);
    if(temp == null)
    {
      insertRate = 0;
      _LOG.debug("Insert rate for job " + jobName + " is not given. Assigned 0 by default");
    }
    else
    {
      insertRate = Float.parseFloat(temp);
      _LOG.debug("Insert rate for job " + jobName + " is set to " + insertRate);
    }

    temp = jobProperties.get(FLAG_JOB_READ_RATE);
    if(temp == null)
    {
      readRate = 0;
      _LOG.debug("Read rate for job " + jobName + " is not given. Assigned 0 by default");
    }
    else
    {
      readRate = Float.parseFloat(temp);
      _LOG.debug("Read rate for job " + jobName + " is set to " + readRate);
    }

    temp = jobProperties.get(FLAG_JOB_DELETE_RATE);
    if(temp == null)
    {
      deleteRate = 0;
      _LOG.debug("Delete rate for job " + jobName + " is not given. Assigned 0 by default");
    }
    else
    {
      deleteRate = Float.parseFloat(temp);
      _LOG.debug("Delete rate for job " + jobName + " is set to " + deleteRate);
    }

    temp = jobProperties.get(FLAG_JOB_UPDATE_RATE);
    if(temp == null)
    {
      updateRate = 0;
      _LOG.debug("Update rate for job " + jobName + " is not given. Assigned 0 by default");
    }
    else
    {
      updateRate = Float.parseFloat(temp);
      _LOG.debug("Update rate for job " + jobName + " is set to " + updateRate);
    }

    //normalize operation rate values
    float sum = 0;
    if(readRate > 0)
      sum += readRate;
    if(insertRate > 0)
      sum += insertRate;
    if(deleteRate > 0)
      sum += deleteRate;
    if(updateRate > 0)
      sum += updateRate;

    if(readRate == 0 && insertRate == 0 && deleteRate == 0 && updateRate == 0)
    {
      _operationGen.addValue(0.25, CONST_QUERY_INSERT);
      _operationGen.addValue(0.25, CONST_QUERY_READ);
      _operationGen.addValue(0.25, CONST_QUERY_DELETE);
      _operationGen.addValue(0.25, CONST_QUERY_UPDATE);
    }
    else
    {
      if(readRate > 0)
        _operationGen.addValue(readRate/sum, CONST_QUERY_READ);
      if(insertRate > 0)
        _operationGen.addValue(insertRate/sum, CONST_QUERY_INSERT);
      if(deleteRate > 0)
        _operationGen.addValue(deleteRate/sum, CONST_QUERY_DELETE);
      if(updateRate > 0)
        _operationGen.addValue(updateRate/sum, CONST_QUERY_UPDATE);
    }

    //set query distribution. possible options are uniform, zipfian, latest, hotspot
    temp = jobProperties.get(FLAG_JOB_QUERY_DISTRIBUTION);
    if(temp == null)
    {
      temp = CONST_DIST_UNIFORM;
      _transactionKeyGen = new UpdatedUniformIntegerGenerator(0, (CounterGenerator) _transactionInsertKeyGen);
    }
    else
    {
      temp = temp.toLowerCase();

      if(temp.equals(CONST_DIST_UNIFORM))
      {
        _transactionKeyGen = new UpdatedUniformIntegerGenerator(0, (CounterGenerator) _transactionInsertKeyGen);
      }
      else if(temp.equals(CONST_DIST_ZIPFIAN))
      {
        _transactionKeyGen = new ScrambledZipfianGenerator(_rowsResponsible);
      }
      else if(temp.equals(CONST_DIST_LATEST))
      {
        _transactionKeyGen = new SkewedLatestGenerator((CounterGenerator) _transactionInsertKeyGen);
      }
      else if(temp.equals(CONST_DIST_HOTSPOT))
      {
        double setFrac;
        double opnFrac;

        String in = jobProperties.get(FLAG_JOB_HOTSPOT_SETFRAC);
        if(in == null)
        {
          setFrac = 0.3;
          _LOG.debug("Set fraction for hotspot is missing. It is set to 0.3 by defult.");
        }
        else
        {
          setFrac = Double.parseDouble(in);
          _LOG.debug("Set fraction for hotspot is set to " + setFrac);
        }

        in = jobProperties.get(FLAG_JOB_HOTSPOT_OPNFRAC);
        if(in == null)
        {
          opnFrac = 0.3;
          _LOG.debug("Operation fraction for hotspot is missing. It is set to 0.3 by default");
        }
        else
        {
          opnFrac = Double.parseDouble(in);
          _LOG.debug("Operation fraction for hotspot is set to " + opnFrac);
        }

        _transactionKeyGen = new HotspotIntegerGenerator(0, _rowsResponsible-1, setFrac, opnFrac);
      }
      else
      {
        _LOG.error("Unkown query distribution for job " + jobName + ": " + temp);
        return WorkloadResult.FAIL;
      }
    }
    _LOG.debug("Query distribution is set to " + temp);

    //set value size distribution
    int valSize;
    int valSizeMin;
    temp = jobProperties.get(FLAG_JOB_VALUE_SIZE);
    if(temp == null)
    {
      _LOG.error("Value size for job " + jobName + " is not specified.");
      return WorkloadResult.FAIL;
    }
    else
    {
      valSize = Integer.parseInt(temp);
    }
    temp = jobProperties.get(FLAG_JOB_VALUE_SIZE_MIN);
    if(temp == null)
    {
      valSizeMin = 1;
    }
    else
    {
      valSizeMin = Integer.parseInt(temp);
    }

    temp = jobProperties.get(FLAG_JOB_VALUE_SIZE_DISTRIBUTION);
    if(temp == null)
    {
      _valueGen = new ConstantIntegerGenerator(valSize);
      temp = CONST_DIST_UNIFORM;
    }
    else
    {
      temp = temp.toLowerCase();
      if(temp.equals(CONST_DIST_CONSTANT))
      {
        _valueGen = new ConstantIntegerGenerator(valSize);
      }
      else if(temp.equals(CONST_DIST_UNIFORM))
      {
        _valueGen = new UniformIntegerGenerator(valSizeMin, valSize);
      }
      else if(temp.equals(CONST_DIST_ZIPFIAN))
      {
        _valueGen = new ScrambledZipfianGenerator(valSizeMin, valSize);
      }
      else
      {
        _LOG.error("Unkown value size distribution for job " + jobName);
        return WorkloadResult.FAIL;
      }
    }
    _LOG.debug("Value size distibution for job " + jobName + " is set to " + temp);

    //init random generator
    _ranGen = new Random(_id);

    return WorkloadResult.OK;
  }

  @Override
  public Query generateInsertLoad()
  {
    Query result = new Query();

    //create key part
    long keyNum = _loadInsertKeyGen.nextInt();
    result.setKey(createKeyString(keyNum));

    //create value part
    int valueSize = _valueGen.nextInt();
    byte[] val = new byte[valueSize];
    _ranGen.nextBytes(val);
    result.setValue(val);

    //set query type
    result.setType(QueryType.INSERT);

    return result;
  }

  @Override
  public Query generateTransaction()
  {
    String nextOp = _operationGen.nextString();

    if(nextOp.equals(CONST_QUERY_INSERT))
    {
      return generateInsertTransaction();
    }
    else if(nextOp.equals(CONST_QUERY_READ))
    {
      return generateReadTransaction();
    }
    else if(nextOp.equals(CONST_QUERY_DELETE))
    {
      return generateDeleteTransaction();
    }
    else
    {
      return generateUpdateTransaction();
    }
  }

  @Override
  public WorkloadResult close()
  {
    return WorkloadResult.OK;
  }

  private Query generateInsertTransaction()
  {
    Query result = new Query();

    //create key part
    long keyNum = _transactionInsertKeyGen.nextInt();
    result.setKey(createKeyString(keyNum));

    //create value part
    int valueSize = _valueGen.nextInt();
    byte[] val = new byte[valueSize];
    _ranGen.nextBytes(val);
    result.setValue(val);

    //set query type
    result.setType(QueryType.INSERT);

    return result;
  }

  private Query generateUpdateTransaction()
  {
    Query result = new Query();

    //create key part
    long keyNum = _transactionKeyGen.nextInt();
    result.setKey(createKeyString(keyNum));

    //create value part
    int valueSize = _valueGen.nextInt();
    byte[] val = new byte[valueSize];
    _ranGen.nextBytes(val);
    result.setValue(val);

    //set query type
    result.setType(QueryType.UPDATE);

    return result;
  }

  private Query generateReadTransaction()
  {
    Query result = new Query();

    //create key part
    long keyNum = _transactionKeyGen.nextInt();
    result.setKey(createKeyString(keyNum));

    //set query type
    result.setType(QueryType.READ);

    return result;
  }

  private Query generateDeleteTransaction()
  {
    Query result = new Query();

    //create key part
    long keyNum = _transactionKeyGen.nextInt();
    result.setKey(createKeyString(keyNum));

    //set query type
    result.setType(QueryType.DELETE);

    return result;
  }

  private String createKeyString(long keyNum)
  {
    StringBuilder sb = new StringBuilder();
    sb.append(Utils.hash(keyNum));
    sb.append("-");
    sb.append(_id);

    return sb.toString();
  }
}
