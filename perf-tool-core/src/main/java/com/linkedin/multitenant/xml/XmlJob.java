/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.multitenant.xml;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

public class XmlJob
{
  private static final Logger _LOG = Logger.getLogger(XmlJob.class);

  protected Map<String, String> _properties;
  protected List<XmlChange> _timeline;

  public XmlJob(Element headElement) throws Exception
  {
    _properties = new HashMap<String, String>();
    _timeline = new ArrayList<XmlChange>();

    NodeList tempList = headElement.getElementsByTagName("property");
    _LOG.debug("number of properties: " + tempList.getLength());
    for(int a = 0; a<tempList.getLength(); a++)
    {
      Element childNode = (Element) tempList.item(a);
      String name = childNode.getAttribute("name");
      String value = childNode.getAttribute("value");

      if(name == null || name.equals(""))
      {
        throw new Exception("job element - " + a + "th attribute - name is null or empty");
      }
      else
      {
        _properties.put(name, value);
        _LOG.debug("property: name=" + name + " value=" + value);
      }
    }

    tempList = headElement.getElementsByTagName("timeline");
    if(tempList.getLength() > 0)
    {
      Element timelineElement = (Element) tempList.item(0);
      tempList = timelineElement.getElementsByTagName("change");
      _LOG.debug("number of changes: " + tempList.getLength());

      for(int a =0; a<tempList.getLength(); a++)
      {
        Element childNode = (Element) tempList.item(a);
        _LOG.debug("processing change-" + a);
        _timeline.add(new XmlChange(childNode));
      }

      Collections.sort(_timeline);
    }
  }

  public Map<String, String> getProperties()
  {
    return _properties;
  }

  public List<XmlChange> getTimeline()
  {
    return _timeline;
  }

  public String toString()
  {
    StringBuilder sb = new StringBuilder();

    sb.append("Job Properties:");
    Iterator<String> itr = _properties.keySet().iterator();
    while(itr.hasNext())
    {
      String name = itr.next();
      String val = _properties.get(name);

      sb.append("\n");
      sb.append(name + ":" + val);
    }

    if(_timeline.size() > 0)
    {
      sb.append("\nTimeline:");
      for(int a = 0; a<_timeline.size(); a++)
      {
        sb.append("\n");
        sb.append(_timeline.get(a).toString());
      }
    }

    return sb.toString();
  }
}
