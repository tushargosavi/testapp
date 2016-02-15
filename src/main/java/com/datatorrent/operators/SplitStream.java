package com.datatorrent.operators;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.utils.OutputController;

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
  public transient DefaultOutputPort<byte[]> out1 = new DefaultOutputPort<byte[]>();

  private int out1ScaleFactor = 1;
  private transient OutputController out1Controller = null;

  @Override public void processTuple(byte[] data)
  {
    super.processTuple(data);
    out1Controller.process();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    out1Controller = new OutputController(out1, gen, out1ScaleFactor);
  }
}
