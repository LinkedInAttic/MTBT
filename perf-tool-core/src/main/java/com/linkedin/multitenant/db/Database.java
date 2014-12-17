/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.multitenant.db;

import java.util.Map;

import com.linkedin.multitenant.common.Query;

public interface Database
{
  public enum DatabaseResult
  {
    OK, FAIL
  }

  /**
   * Initialize any parameters, connections.
   * @param workPlanProperties General work plan information (e.g., database to connect to)
   * @param jobProperties Job information
   * @return success:DatabaseResult.OK<br>
   * fail:DatabaseResult.FAIL
   */
  public DatabaseResult init(Map<String, String> workPlanProperties, Map<String, String> jobProperties) throws Exception;

  /**
   * Perform an insert operation on the database.
   * @param q Query instance to be put
   * @return success:DatabaseResult.OK<br>
   * fail:DatabaseResult.FAIL
   */
  public DatabaseResult doInsert(Query q);

  /**
   * Perform a read operation on the database.
   * @param q Query instance to be read.
   * @return success:DatabaseResult.OK<br>
   * fail:DatabaseResult.FAIL
   */
  public DatabaseResult doRead(Query q);

  /**
   * Perform a delete operation on the database.
   * @param q Query instance to be deleted.
   * @return success:DatabaseResult.OK<br>
   * fail:DatabaseResult.FAIL
   */
  public DatabaseResult doDelete(Query q);

  /**
   * Perform an update operation on the database.
   * @param q Query instance to be put
   * @return success:DatabaseResult.OK<br>
   * fail:DatabaseResult.FAIL
   */
  public DatabaseResult doUpdate(Query q);

  /**
   * Close any open connection/file before quitting.
   * @return success:DatabaseResult.OK<br>
   * fail:DatabaseResult.FAIL
   */
  public DatabaseResult close();
}
