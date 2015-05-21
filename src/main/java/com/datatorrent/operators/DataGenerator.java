package com.datatorrent.operators;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.utils.ByteDataGenerator;

public class DataGenerator extends TimedRateLimitInputOperator<byte[]>
{
  /**
   * The output port on which byte arrays are emitted.
   */
  public final transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<byte[]>();

  /**
   * The random object use to generate the tuples.
   */
  private ByteDataGenerator gen;

  @Override protected byte[] generateTuple()
  {
    return gen.generateData();
  }

  @Override protected void emitTuple(byte[] tuple)
  {
    output.emit(tuple);
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

