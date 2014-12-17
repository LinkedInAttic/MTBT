/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.multitenant.exporter;

import java.io.File;
import java.io.PrintWriter;
import java.util.Iterator;
import java.util.Map;

import com.linkedin.multitenant.profiler.Profiler;

public class FileExporter implements DataExporter
{
  public final static String FLAG_FILEEXPORTER_ISCOMBINED = "fileExporter.isCombined";
  public final static String FLAG_FILEEXPORTER_SINGLE_FILE_PATH = "fileExporter.singlePath";
  public final static String FLAG_FILEEXPORTER_COMBINED_FOLDER_PATH = "fileExporter.folderPath";

  //Should I combine all job results to a file or print them separately
  private boolean _isCombined;

  //File path for the output if isCombined is true
  private String _singleFilePath;

  //Folder path for the output of each job if isCombined is false
  private String _combinedFolderPath;

  //Mapping of jobs to profiler
  private Map<String, Profiler> _profMap;

  public FileExporter()
  {
  }

  public void init(Map<String, String> workPlanProperties, Map<String, Profiler> profMap)
  {
    //get isCombined from properties
    String val = workPlanProperties.get(FLAG_FILEEXPORTER_ISCOMBINED);
    if(val != null && (val.equals("1") || val.equals("true") || val.equals("yes")))
    {
      _isCombined = true;
    }
    else
    {
      _isCombined = false;
    }

    //get singleFilePath from properties
    val = workPlanProperties.get(FLAG_FILEEXPORTER_SINGLE_FILE_PATH);
    if(val != null)
    {
      _singleFilePath = val;
    }
    else
    {
      _singleFilePath = "results.txt";
    }

    //get combinedFolderPath from properties
    val = workPlanProperties.get(FLAG_FILEEXPORTER_COMBINED_FOLDER_PATH);
    if(val != null)
    {
      if(val.endsWith("/"))
        _combinedFolderPath = val.substring(0, val.length()-1);
      else
        _combinedFolderPath = val;
    }
    else
    {
      _combinedFolderPath = ".";
    }

    _profMap = profMap;
  }

  public void export() throws Exception
  {
    if(_isCombined)
    {
      PrintWriter out = new PrintWriter(new File(_singleFilePath));

      Iterator<String> itr = _profMap.keySet().iterator();
      while(itr.hasNext())
      {
        String jobName = itr.next();
        Profiler prof = _profMap.get(jobName);

        out.println("***************************************");

        out.println("Job=" + jobName);
        out.println(prof.toString());

        out.println("***************************************");
      }

      out.close();
    }
    else
    {
      Iterator<String> itr = _profMap.keySet().iterator();
      while(itr.hasNext())
      {
        String jobName = itr.next();
        PrintWriter out = new PrintWriter(new File(_combinedFolderPath + "/" + jobName + "-Result.txt"));
        Profiler prof = _profMap.get(jobName);

        out.println("***************************************");

        out.println("Job=" + jobName);
        out.println(prof.toString());

        out.println("***************************************");
        out.close();
      }
    }
  }
}
