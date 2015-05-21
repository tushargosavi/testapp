package com.datatorrent.operators;

import com.datatorrent.BaseBenchmarkOperator;
import com.datatorrent.api.DefaultOutputPort;

public class PassthroughOperator extends BaseBenchmarkOperator
{
  public transient DefaultOutputPort<byte[]> out = new DefaultOutputPort<byte[]>();

  @Override public void processTuple(byte[] data)
  {
    emitTuple(data);
  }

  @Override protected void emitTuple(byte[] data)
  {
    out.emit(data);
  }
}
