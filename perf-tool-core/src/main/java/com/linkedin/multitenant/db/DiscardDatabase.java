/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.multitenant.db;

import java.util.Map;

import org.apache.log4j.Logger;

import com.linkedin.multitenant.common.Query;

public class DiscardDatabase implements Database
{
  private static final Logger _LOG = Logger.getLogger(DiscardDatabase.class);

  @Override
  public DatabaseResult init(Map<String, String> workPlanProperties, Map<String, String> jobProperties) throws Exception
  {
    _LOG.warn("This Database class does not do anything...");

    return DatabaseResult.OK;
  }

  @Override
  public DatabaseResult doInsert(Query q)
  {
    return DatabaseResult.OK;
  }

  @Override
  public DatabaseResult doRead(Query q)
  {
    return DatabaseResult.OK;
  }

  @Override
  public DatabaseResult doDelete(Query q)
  {
    return DatabaseResult.OK;
  }

  @Override
  public DatabaseResult doUpdate(Query q)
  {
    return DatabaseResult.OK;
  }

  @Override
  public DatabaseResult close()
  {
    return DatabaseResult.OK;
  }
}
