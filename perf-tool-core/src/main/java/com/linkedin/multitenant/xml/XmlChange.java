/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.multitenant.xml;

import org.apache.log4j.Logger;
import org.w3c.dom.Element;

public class XmlChange implements Comparable<XmlChange>
{
  private static final Logger _LOG = Logger.getLogger(XmlChange.class);

  protected int _time;
  protected double _targetThr;

  public XmlChange(Element headElement) throws Exception
  {
    String at = headElement.getAttribute("at");
    String to = headElement.getAttribute("to");

    if(at == null || at.equals(""))
    {
      throw new Exception("Changing time is not specified in change element");
    }
    if(to == null || to.equals(""))
    {
      throw new Exception("Target throuhgput is not specified in change element");
    }

    _time = Integer.parseInt(at);
    _LOG.debug("Changing at: " + _time);

    _targetThr = Double.parseDouble(to);
    _LOG.debug("Changing to: " + _targetThr);
  }

  public int getTime()
  {
    return _time;
  }

  public double getTargetThr()
  {
    return _targetThr;
  }

  public int compareTo(XmlChange rhs)
  {
    if(_time < rhs.getTime())
      return -1;
    else if(_time > rhs.getTime())
      return 1;
    else
      return 0;
  }

  public String toString()
  {
    StringBuilder sb = new StringBuilder();

    sb.append("Change at ");
    sb.append(_time);
    sb.append(" sec to ");
    sb.append(_targetThr);
    sb.append(" opt/sec");

    return sb.toString();
  }
}
