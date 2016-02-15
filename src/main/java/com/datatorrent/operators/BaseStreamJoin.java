package com.datatorrent.operators;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.utils.ByteDataGenerator;
import com.datatorrent.utils.OutputController;

public class BaseStreamJoin extends BaseOperator
{
  @OutputPortFieldAnnotation(optional = true)
  public DefaultOutputPort<byte[]> out = new DefaultOutputPort<byte[]>();

  private transient OutputController oc;
  private ByteDataGenerator gen;
  private int outputScale;

  protected void processTuple(byte[] tuple) {
    if (out.isConnected())
      out.emit(tuple);
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    oc = new OutputController(out, gen, outputScale);
  }

  public ByteDataGenerator getGen()
  {
    return gen;
  }

  public void setGen(ByteDataGenerator gen)
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
