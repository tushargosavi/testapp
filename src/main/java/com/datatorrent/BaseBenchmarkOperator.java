package com.datatorrent;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;

public abstract class BaseBenchmarkOperator extends BaseOperator
{
  public transient DefaultInputPort<byte[]> input = new DefaultInputPort<byte[]>()
  {
    @Override public void process(byte[] tuple)
    {
      processTuple(tuple);
    }
  };

  public abstract void processTuple(byte[] data);
  protected abstract void emitTuple(byte[] data);
}
