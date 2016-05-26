package com.datatorrent.operators;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.generator.ByteArrayGenerator;

public class BaseStreamJoin extends BaseOperator
{
  @OutputPortFieldAnnotation(optional = true)
  public DefaultOutputPort<byte[]> out = new DefaultOutputPort<byte[]>();

  private ByteArrayGenerator gen;
  private int outputScale;

  protected void processTuple(byte[] tuple) {
    if (out.isConnected())
      out.emit(tuple);
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
  }

  public ByteArrayGenerator getGen()
  {
    return gen;
  }

  public void setGen(ByteArrayGenerator gen)
  {
    this.gen = gen;
  }

  public int getOutputScale()
  {
    return outputScale;
  }

  public void setOutputScale(int outputScale)
  {
    this.outputScale = outputScale;
  }
}
