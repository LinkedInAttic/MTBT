/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.multitenant.db;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.linkedin.multitenant.common.Query;
import com.linkedin.multitenant.common.Query.QueryType;
import com.linkedin.multitenant.db.Database.DatabaseResult;
import com.linkedin.multitenant.xml.XmlJob;
import com.linkedin.multitenant.xml.XmlParser;
import com.linkedin.multitenant.xml.XmlWorkPlan;

public class TestProxyDatabase
{
  protected static Database m_db;

  @BeforeClass
  public static void initDB()
  {
    String inputData =
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
            "<workPlan>" +
              "<property name=\"work.host\" value=\"localhost\"/>" +
              "<property name=\"work.port\" value=\"3306\"/>" +
              "<property name=\"work.runTime\" value=\"100\"/>" +
              "<property name=\"work.gran\" value=\"10\"/>" +
              "<property name=\"work.status.period\" value=\"2\"/>" +
              "<job>" +
                "<property name=\"job.name\" value=\"job-1\"/>" +
                "<property name=\"proxy.dbName\" value=\"db1\"/>" +
                "<property name=\"proxy.tableName\" value=\"tab1\"/>" +
                "<property name=\"proxy.keyCol\" value=\"keyCol\"/>" +
                "<property name=\"proxy.valCol\" value=\"valCol\"/>" +
                "<property name=\"job.threads\" value=\"2\"/>" +
                "<property name=\"job.targetThr\" value=\"5\"/>" +
                "<property name=\"job.rowCount\" value=\"10\"/>" +
                "<property name=\"job.valueSize\" value=\"10\"/>" +
                "<timeline>" +
                  "<change at=\"120\" to=\"40\"/>" +
                  "<change at=\"150\" to=\"30\"/>" +
                "</timeline>" +
              "</job>" +
            "</workPlan>";

    byte[] workPlanData = inputData.getBytes();
    XmlWorkPlan xmlWork;

    try
    {
      xmlWork = XmlParser.parseWorkPlan(workPlanData);
      if(xmlWork == null)
        System.out.println("parsing returned null");
      else
        System.out.println(xmlWork.toString());
    }
    catch(Exception e)
    {
      e.printStackTrace();
      return;
    }

    XmlJob xmlJob = xmlWork.getJobList().get(0);

    m_db = new ProxyDatabase();
    try
    {
      DatabaseResult res = m_db.init(xmlWork.getProperties(), xmlJob.getProperties());
      if(res == DatabaseResult.FAIL)
      {
        System.out.println("Database init failed");
        return;
      }
    }
    catch (Exception e)
    {
      e.printStackTrace();
      return;
    }
  }

  @AfterClass
  public static void closeDB()
  {
    m_db.close();
  }

  @Test
  public void testOperations()
  {
    try
    {
      String key1 = "crazy";
      byte[] val1 = "pete".getBytes();
      byte[] val2 = "notsofun".getBytes();

      Query q1 = new Query(key1, val1, QueryType.INSERT);
      DatabaseResult res = m_db.doInsert(q1);
      System.out.println("Result of insert: " + res);

      Query q2 = new Query(key1, null, QueryType.READ);
      res = m_db.doRead(q2);
      System.out.println("Result of read: " + res);

      Query q3 = new Query(key1, val2, QueryType.UPDATE);
      res = m_db.doUpdate(q3);
      System.out.println("Result of update: " + res);

      Query q4 = new Query(key1, null, QueryType.DELETE);
      res = m_db.doDelete(q4);
      System.out.println("Result of delete: " + res);

      Query q5 = new Query(key1, null, QueryType.READ);
      res = m_db.doRead(q5);
      System.out.println("Result of read: " + res);
    }
    catch (Exception e)
    {
      System.out.println("Could not execute mysql tests");
      e.printStackTrace();
    }
  }
}
