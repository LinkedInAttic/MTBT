/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.multitenant.exporter;

import java.util.Map;

import com.linkedin.multitenant.profiler.Profiler;

public interface DataExporter
{
  /**
   * Export profiler data
   * @throws Exception
   */
  public void export() throws Exception;

  /**
   * Init this DataExporter class instance using the properties in workPlan, and profiler data collected after the experiment.
   * @param workPlanProperties Properties for the workPlan.
   * @param prof Mapping of profilers for each job after the experiment.
   * @throws Exception
   */
  public void init(Map<String, String> workPlanProperties, Map<String, Profiler> prof) throws Exception;
}
