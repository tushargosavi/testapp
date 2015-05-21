package com.datatorrent.operators;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

public class StreamJoin3<T> extends BaseStreamJoin<T>
{
  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort<T> in1 = new DefaultInputPort<T>()
  {
    @Override public void process(T tuple)
    {
      processTuple(tuple);
    }
  };

  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort<T> in2 = new DefaultInputPort<T>()
  {
    @Override public void process(T tuple)
    {
      processTuple(tuple);
    }
  };

  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort<T> in3 = new DefaultInputPort<T>()
  {
    @Override public void process(T tuple)
    {
      processTuple(tuple);
    }
  };
}
