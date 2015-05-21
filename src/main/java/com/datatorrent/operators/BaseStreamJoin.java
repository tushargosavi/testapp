package com.datatorrent.operators;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultOutputPort;

public class BaseStreamJoin<T> extends BaseOperator
{
  public DefaultOutputPort<T> out = new DefaultOutputPort<T>();

  protected void processTuple(T tuple) {
    out.emit(tuple);
  }
}
