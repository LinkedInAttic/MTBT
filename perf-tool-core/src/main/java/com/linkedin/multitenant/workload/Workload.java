/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.multitenant.workload;

import java.util.Map;

import com.linkedin.multitenant.common.Query;

public interface Workload
{
  public enum WorkloadResult
  {
    OK, FAIL
  }

  /**
   * Initialize this workload instance.
   * @param myId ID of the WorkerThread that instantiates this Workload instance.<br>
   * Should be between [0, numberOfWorkers)
   * @param numberOfWorkers Number of total workerThreads for this job.<br>
   * Total workers is numberOfMachines * numberOfThreads for this job.
   * @param workPlanProperties Properties for the work plan
   * @param jobProperties Properties for the job
   * @return init result in WorkloadResult type
   */
  public abstract WorkloadResult init(int myId, int numberOfWorkers, Map<String, String> workPlanProperties, Map<String, String> jobProperties);

  /**
   * Generate an insert query that is used in the data loading phase.
   * @return Returns query instance if succeeded. Null otherwise.
   */
  public abstract Query generateInsertLoad();

  /**
   * Generate a transaction chosen randomly amongst read/write/delete if specified before.
   * @return Returns query instance if succeeded. Null otherwise.
   */
  public abstract Query generateTransaction();

  /**
   * Close any open connection/file before quitting.
   * @return close result in WorkloadResult type
   */
  public abstract WorkloadResult close();

  /**
   * Return the number of rows that this Workload instances is responsible for.<br>
   * Different Workload implementations may have different assignment techniques to each thread.
   * @return The number of rows that this workload instance (hence, this thread) is responsible for.
   */
  public abstract int getRowsResponsible();
}
