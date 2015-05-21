package com.datatorrent.operators;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.utils.ByteDataGenerator;

public class SinglePortInputOutputOperator extends BaseOperator
{
  private int outputScaleFactor = 1;
  private ByteDataGenerator gen = new ByteDataGenerator(1024, 1024);

  public transient DefaultOutputPort<byte[]> out = new DefaultOutputPort<byte[]>();

  public transient DefaultInputPort<byte[]> input = new DefaultInputPort<byte[]>()
  {
    @Override public void process(byte[] tuple)
    {
      processTuple(tuple);
    }
  };
  private int count;

  public void processTuple(byte[] data) {
    if (outputScaleFactor >= 1) {
      for (int i = 0; i < outputScaleFactor; i++) {
        emitTuple(gen.generateData());
      }
    } else if (outputScaleFactor < 0) {
      count++;
      if (count % Math.abs(outputScaleFactor) == 0) {
        emitTuple(gen.generateData());
      }
    }
  }

  protected void emitTuple(byte[] data) {
    out.emit(data);
  }

  public int getOutputScaleFactor()
  {
    return outputScaleFactor;
  }

  public void setOutputScaleFactor(int factor)
  {
    this.outputScaleFactor = factor;
  }

  public ByteDataGenerator getGen()
  {
    return gen;
  }

  public void setGen(ByteDataGenerator gen)
  {
    this.gen = gen;
  }

}
