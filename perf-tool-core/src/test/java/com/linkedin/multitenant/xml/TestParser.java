/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.multitenant.xml;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.junit.Test;

public class TestParser
{
  @Test
  public void testFilePath()
  {
    try
    {
      InputStream in = getClass().getResourceAsStream("/workPlan.xsd");
      BufferedReader input = new BufferedReader(new InputStreamReader((in)));

      while(input.ready())
      {
        System.out.println(input.readLine());
      }
    }
    catch(Exception e)
    {
      e.printStackTrace();
    }
  }

  @Test
  public void testValidation()
  {
    String inputData =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<workPlan>" +
              "<property name=\"main.runtime\" value=\"100\"/>" +
              "<property name=\"main.gran\" value=\"10\"/>" +
              "<job>" +
                "<property name=\"job.param1\" value=\"1\"/>" +
                "<property name=\"job.param2\" value=\"2\"/>" +
              "</job>" +
              "<job>" +
                "<property name=\"job.param3\" value=\"3\"/>" +
                "<property name=\"job.param4\" value=\"4\"/>" +
                "<timeline>" +
                  "<change at=\"120\" to=\"40\"/>" +
                  "<change at=\"150\" to=\"30\"/>" +
                "</timeline>" +
              "</job>" +
            "</workPlan>";

    byte[] workPlanData = inputData.getBytes();

    try
    {
      XmlWorkPlan ret = XmlParser.parseWorkPlan(workPlanData);
      if(ret == null)
        System.out.println("parsing returned null");
      else
        System.out.println(ret.toString());
    }
    catch(Exception e)
    {
      e.printStackTrace();
    }
  }
}
