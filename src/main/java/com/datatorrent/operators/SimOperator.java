package com.datatorrent.operators;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

public class SimOperator extends BaseTestOperator
{
  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort in1 = new DefaultInputPort()
  {
    @Override
    public void process(Object o)
    {
      processTuple(this, o);
    }
  };

  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort in2 = new DefaultInputPort()
  {
    @Override
    public void process(Object o)
    {
      processTuple(this, o);
    }
  };

  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort in3 = new DefaultInputPort()
  {
    @Override
    public void process(Object o)
    {
      processTuple(this, o);
    }
  };

  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort in4 = new DefaultInputPort()
  {
    @Override
    public void process(Object o)
    {
      processTuple(this, o);
    }
  };

  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort in5 = new DefaultInputPort()
  {
    @Override
    public void process(Object o)
    {
      processTuple(this, o);
    }
  };

  @OutputPortFieldAnnotation(optional = true)
  public transient DefaultOutputPort out1 = new DefaultOutputPort<>();
  @OutputPortFieldAnnotation(optional = true)
  public transient DefaultOutputPort out2 = new DefaultOutputPort<>();
  @OutputPortFieldAnnotation(optional = true)
  public transient DefaultOutputPort out3 = new DefaultOutputPort<>();
  @OutputPortFieldAnnotation(optional = true)
  public transient DefaultOutputPort out4 = new DefaultOutputPort<>();
  @OutputPortFieldAnnotation(optional = true)
  public transient DefaultOutputPort out5 = new DefaultOutputPort<>();

}
