package com.datatorrent.operators;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.utils.ByteDataGenerator;

/**
 * Data generator operator.
 *
 * @displayName Random byte array data generator.
 * @category benchmark
 * @tags input
 */
public class ByteArrayDataGeneratorOperator extends TimedRateLimitInputOperator<byte[]>
{
  /**
   * The output port on which byte arrays are emitted.
   */
  @OutputPortFieldAnnotation(optional = true)
  public final transient DefaultOutputPort<byte[]> output = new DefaultOutputPort<byte[]>();

  /**
   * The random object use to generate the tuples.
   */
  private ByteDataGenerator gen = new ByteDataGenerator(100, 1024);

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

