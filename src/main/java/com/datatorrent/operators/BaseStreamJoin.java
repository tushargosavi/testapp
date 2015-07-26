package com.datatorrent.operators;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

public class BaseStreamJoin<T> extends BaseOperator
{
  public DefaultOutputPort<T> out = new DefaultOutputPort<T>();

  protected void processTuple(T tuple) {
    out.emit(tuple);
  }
}
