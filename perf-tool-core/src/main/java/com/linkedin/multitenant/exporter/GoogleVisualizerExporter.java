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
import java.util.List;
import java.util.Map;

import com.linkedin.multitenant.profiler.EpochResult;
import com.linkedin.multitenant.profiler.Profiler;

public class GoogleVisualizerExporter implements DataExporter
{
  public final static String FLAG_GOOGLEEXPORTER_OUTPUT_PATH = "googleExporter.output";

  //file path to the output
  private String _outputFilename;

  //mapping from jobs to profilers
  private Map<String, Profiler> _profMap;

  public GoogleVisualizerExporter()
  {
  }

  public void init(Map<String, String> workPlanProperties, Map<String, Profiler> profMap)
  {
    //get output path
    String val = workPlanProperties.get(FLAG_GOOGLEEXPORTER_OUTPUT_PATH);
    if(val != null)
    {
      _outputFilename = val;
    }
    else
    {
      _outputFilename = "results.html";
    }

    _profMap = profMap;
  }

  private int getNumberOfEpochs(Map<String, Profiler> profMap)
  {
    Iterator<String> itr = profMap.keySet().iterator();
    String first = itr.next();
    Profiler firstProf = profMap.get(first);

    return firstProf.getInsertResults().getArr().length;
  }

  public void export() throws Exception
  {
    PrintWriter out = new PrintWriter(new File(_outputFilename));

    //print header info
    out.println("<html>");

    out.println("\t<head>");
    out.println("\t\t<script type=\"text/javascript\" src=\"https://www.google.com/jsapi\">");
    out.println("\t\t</script>");

    out.println("\t\t<script type=\"text/javascript\">");

    out.println("\t\t\tgoogle.load('visualization', '1', {'packages':['motionchart']});");
    out.println("\t\t\tgoogle.setOnLoadCallback(drawChart);");
    out.println("\t\t\tfunction drawChart() {");
    out.println("\t\t\t\tvar data = new google.visualization.DataTable();");
    out.println("\t\t\t\tdata.addColumn('string', 'JobName');");
    out.println("\t\t\t\tdata.addColumn('number', 'timePassed(s)');");
    out.println("\t\t\t\tdata.addColumn('number', 'InsertOptPerformed(opt)');");
    out.println("\t\t\t\tdata.addColumn('number', 'InsertAvgLat(ms)');");
    out.println("\t\t\t\tdata.addColumn('number', 'Insert95PercLat(ms)');");
    out.println("\t\t\t\tdata.addColumn('number', 'Insert99PercLat(ms)');");
    out.println("\t\t\t\tdata.addColumn('number', 'ReadOptPerformed(opt)');");
    out.println("\t\t\t\tdata.addColumn('number', 'ReadAvgLat(ms)');");
    out.println("\t\t\t\tdata.addColumn('number', 'Read95PercLat(ms)');");
    out.println("\t\t\t\tdata.addColumn('number', 'Read99PercLat(ms)');");
    out.println("\t\t\t\tdata.addColumn('number', 'DeleteOptPerformed(opt)');");
    out.println("\t\t\t\tdata.addColumn('number', 'DeleteAvgLat(ms)');");
    out.println("\t\t\t\tdata.addColumn('number', 'Delete95PercLat(ms)');");
    out.println("\t\t\t\tdata.addColumn('number', 'Delete99PercLat(ms)');");
    out.println("\t\t\t\tdata.addColumn('number', 'UpdateOptPerformed(opt)');");
    out.println("\t\t\t\tdata.addColumn('number', 'UpdateAvgLat(ms)');");
    out.println("\t\t\t\tdata.addColumn('number', 'Update95PercLat(ms)');");
    out.println("\t\t\t\tdata.addColumn('number', 'Update99PercLat(ms)');");
    out.println("\t\t\t\tdata.addColumn('number', 'TotalOptPerformed(opt)');");
    out.println("\t\t\t\tdata.addColumn('number', 'TotalAvgLat(ms)');");
    out.println("\t\t\t\tdata.addColumn('number', 'Total95PercLat(ms)');");
    out.println("\t\t\t\tdata.addColumn('number', 'Total99PercLat(ms)');");

    //add data
    int numberOfEpochs = getNumberOfEpochs(_profMap);
    for(int a = 0; a<numberOfEpochs; a++)
    {
      Iterator<String> itr = _profMap.keySet().iterator();
      while(itr.hasNext())
      {
        String jobName = itr.next();
        Profiler prof = _profMap.get(jobName);

        EpochResult insertEpoch = prof.getInsertResults().getArr()[a];
        EpochResult readEpoch = prof.getReadResults().getArr()[a];
        EpochResult deleteEpoch = prof.getDeleteResults().getArr()[a];
        EpochResult updateEpoch = prof.getUpdateResults().getArr()[a];
        EpochResult totalEpoch = insertEpoch.copy();
        totalEpoch.add(readEpoch);
        totalEpoch.add(deleteEpoch);
        totalEpoch.add(updateEpoch);

        out.print("\t\t\t\tdata.addRow([");

        //get write summary
        List<Object> summary = insertEpoch.summarize();

        //write job name
        out.print("'" +  jobName + "', ");
        //write end time
        out.print("100 + " + summary.get(0).toString() + ", ");

        //write writeOptPerformed
        out.print(summary.get(1).toString() + ", ");
        //write writeAvgLatency
        out.print(summary.get(2).toString() + ", ");
        //write write95PercLatency
        out.print(summary.get(3).toString() + ", ");
        //write write99PercLatency
        out.print(summary.get(4).toString() + ", ");

        //get read summary
        summary = readEpoch.summarize();
        //write readOptPerformed
        out.print(summary.get(1).toString() + ", ");
        //write readAvgLatency
        out.print(summary.get(2).toString() + ", ");
        //write read95PercLatency
        out.print(summary.get(3).toString() + ", ");
        //write read99PercLatency
        out.print(summary.get(4).toString() + ", ");

        //get delete summary
        summary = deleteEpoch.summarize();
        //write deleteOptPerformed
        out.print(summary.get(1).toString() + ", ");
        //write deleteAvgLatency
        out.print(summary.get(2).toString() + ", ");
        //write delete95PercLatency
        out.print(summary.get(3).toString() + ", ");
        //write delete99PercLatency
        out.print(summary.get(4).toString() + ", ");

        //get update summary
        summary = updateEpoch.summarize();
        //write updateOptPerformed
        out.print(summary.get(1).toString() + ", ");
        //write updateAvgLatency
        out.print(summary.get(2).toString() + ", ");
        //write update95PercLatency
        out.print(summary.get(3).toString() + ", ");
        //write update99PercLatency
        out.print(summary.get(4).toString() + ", ");

        //get total summary
        summary = totalEpoch.summarize();
        //write totalOptPerformed
        out.print(summary.get(1).toString() + ", ");
        //write totalAvgLatency
        out.print(summary.get(2).toString() + ", ");
        //write total95PercLatency
        out.print(summary.get(3).toString() + ", ");
        //write total99PercLatency
        out.print(summary.get(4).toString());

        out.print("]);\n");
      }
    }

    out.println("\t\t\t\tvar chart = new google.visualization.MotionChart(document.getElementById('chart_div'));");
    out.println("\t\t\t\tchart.draw(data, {width: 1200, height: 600});");

    out.println("\t\t\t}");

    out.println("\t\t</script>");
    out.println("\t</head>");

    out.println("\t<body>");
    out.println("\t\t<div id=\"chart_div\" style=\"width: 1200px; height: 600px;\">");
    out.println("\t\t</div>");
    out.println("\t<body>");
    out.println("</html>");

    out.close();
  }
}
