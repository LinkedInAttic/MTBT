/**
 * Copyright 2014 LinkedIn Corp. Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 */
package com.linkedin.multitenant.common;

public class Query
{
  public enum QueryType
  {
    INSERT, READ, DELETE, UPDATE, NA
  }

  protected String _key;
  protected byte[] _value;
  protected QueryType _type;

  public Query()
  {
    this(null, null, QueryType.NA);
  }

  public Query(String key, byte[] value, QueryType type)
  {
    _key = key;
    _value = value;
    _type = type;
  }

  public String getKey()
  {
    return _key;
  }

  public void setKey(String key)
  {
    _key = key;
  }

  public byte[] getValue()
  {
    return _value;
  }

  public void setValue(byte[] value)
  {
    _value = value;
  }

  public QueryType getType()
  {
    return _type;
  }

  public void setType(QueryType type)
  {
    _type = type;
  }

  public String toString()
  {
    StringBuilder sb = new StringBuilder();

    sb.append("type=");
    sb.append(_type);

    if(_key != null)
    {
      sb.append(" key=");
      sb.append(_key);
    }

    if(_value != null)
    {
      sb.append(" valLen=");
      sb.append(_value.length);
    }

    return sb.toString();
  }
}
