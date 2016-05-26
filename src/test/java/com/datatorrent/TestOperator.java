package com.datatorrent;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.operators.BaseTestOperator;

public class TestOperator<T> extends BaseTestOperator<T>
{
  public transient DefaultInputPort<T> in1 = new DefaultInputPort<T>() {
    @Override
    public void process(T o)
    {
      processTuple(this, o);
    }
  };

  public transient DefaultInputPort<T> in2 = new DefaultInputPort<T>() {
    @Override
    public void process(T o)
    {
      processTuple(this, o);
    }
  };

  public transient DefaultOutputPort<T> out1 = new DefaultOutputPort<T>();
}
