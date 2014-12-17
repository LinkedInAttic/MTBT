/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.multitenant.exporter;

import java.util.Iterator;
import java.util.Map;

import com.linkedin.multitenant.profiler.Profiler;

public class ConsoleExporter implements DataExporter
{
  //mapping from jobs to profilers
  private Map<String, Profiler> _profMap;

  public ConsoleExporter()
  {
  }

  public void init(Map<String, String> workPlanProperties, Map<String, Profiler> profMap)
  {
    _profMap = profMap;
  }

  public void export()
  {
    Iterator<String> itr = _profMap.keySet().iterator();
    while(itr.hasNext())
    {
      String jobName = itr.next();
      Profiler prof = _profMap.get(jobName);

      System.out.println("***************************************");

      System.out.println("Job=" + jobName);
      System.out.println(prof.toString());

      System.out.println("***************************************");
    }
  }
}
