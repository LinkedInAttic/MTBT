/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.multitenant.xml;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Source;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.apache.log4j.Logger;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

public class XmlParser
{
  private static final Logger _LOG = Logger.getLogger(XmlParser.class);

  public static XmlWorkPlan parseWorkPlan(byte workPlanData[]) throws Exception
  {
    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
    DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
    InputStream inStr = new ByteArrayInputStream(workPlanData);
    Document doc = dBuilder.parse(inStr);

    XmlWorkPlan ret = null;

    if(isValid(workPlanData))
    {
      Element workPlanElement = doc.getDocumentElement();
      ret = new XmlWorkPlan(workPlanElement);
    }

    return ret;
  }

  private static boolean isValid(byte[] workPlanData) throws Exception
  {
    InputStream schemaInStr = XmlParser.class.getResourceAsStream("/workPlan.xsd");
    Source schemaSrc = new StreamSource(schemaInStr);

    SchemaFactory factory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
    Schema schema = factory.newSchema(schemaSrc);
    Validator val = schema.newValidator();

    InputStream inStr = new ByteArrayInputStream(workPlanData);
    Source src = new StreamSource(inStr);

    boolean rtn;
    try
    {
      val.validate(src);
      rtn = true;
    }
    catch(Exception e)
    {
      _LOG.error("Cannot validate xml file", e);
      rtn = false;
    }

    return rtn;
  }
}
