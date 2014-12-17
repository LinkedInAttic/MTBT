/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.multitenant.xml;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

public class XmlWorkPlan
{
  private static final Logger _LOG = Logger.getLogger(XmlWorkPlan.class);

  protected Map<String, String> _properties;
  protected List<XmlJob> _jobs;

  public XmlWorkPlan(Element headElement) throws Exception
  {
    _properties = new HashMap<String, String>();
    _jobs = new ArrayList<XmlJob>();

    NodeList tempList = headElement.getChildNodes();
    for(int a = 0; a<tempList.getLength(); a++)
    {
      if(tempList.item(a).getNodeType() != Node.ELEMENT_NODE)
        continue;

      Element childNode = (Element) tempList.item(a);
      if(!childNode.getNodeName().equals("property"))
        continue;

      String name = childNode.getAttribute("name");
      String value = childNode.getAttribute("value");

      if(name == null || name.equals(""))
      {
        throw new Exception("workPlan element - " + a + "th attribute - name is null or empty");
      }
      else
      {
        _properties.put(name, value);
        _LOG.debug("property: name=" + name + " value=" + value);
      }
    }

    tempList = headElement.getElementsByTagName("job");
    _LOG.debug("number of jobs: " + tempList.getLength());
    for(int a =0; a<tempList.getLength(); a++)
    {
      Element childNode = (Element) tempList.item(a);
      _LOG.debug("processing job-" + a);
      _jobs.add(new XmlJob(childNode));
    }
  }

  public Map<String, String> getProperties()
  {
    return _properties;
  }

  public List<XmlJob> getJobList()
  {
    return _jobs;
  }

  public String toString()
  {
    StringBuilder sb = new StringBuilder();

    sb.append("Work Plan Properties:");
    Iterator<String> itr = _properties.keySet().iterator();
    while(itr.hasNext())
    {
      String name = itr.next();
      String val = _properties.get(name);

      sb.append("\n");
      sb.append(name + ":" + val);
    }

    for(int a = 0; a<_jobs.size(); a++)
    {
      sb.append("\n");
      sb.append("*****\n");
      sb.append(_jobs.get(a).toString());
    }

    sb.append("\n********");

    return sb.toString();
  }
}
