package com.datatorrent.operators;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;

public class BaseStreamJoin<T> extends BaseOperator
{
  @OutputPortFieldAnnotation(optional = true)
  public DefaultOutputPort<T> out = new DefaultOutputPort<T>();

  protected void processTuple(T tuple) {
    if (out.isConnected())
      out.emit(tuple);
  }
}
