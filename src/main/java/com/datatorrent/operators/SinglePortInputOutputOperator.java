package com.datatorrent.operators;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.utils.ByteDataGenerator;

/**
 * Single port input to output operator.
 *
 * This operator emits number of tuples proportional to number
 * of input tuples.
 *
 *
 * @displayName Single port input / output operator.
 * @category benchmark
 * @tags operator filter scale
 */
public class SinglePortInputOutputOperator extends BaseOperator
{
  private int outputScaleFactor = 1;
  private ByteDataGenerator gen = new ByteDataGenerator(1024, 1024);

  @OutputPortFieldAnnotation(optional = true)
  public transient DefaultOutputPort<byte[]> out = new DefaultOutputPort<byte[]>();

  @InputPortFieldAnnotation(optional = true)
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
