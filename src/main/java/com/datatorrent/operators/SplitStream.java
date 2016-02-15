package com.datatorrent.operators;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;

/**
 * This operator do nothing with input tuple, it just passes the input tuple to
 * output port.
 *
 * The main use of this operator is to test the platform serialization and deserialization
 * overhead.
 *
 * @displayName Passthrough operator
 * @category benchmark
 * @tags operator
 */
public class SplitStream extends SinglePortInputOutputOperator
{
  @OutputPortFieldAnnotation(optional = true)
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
