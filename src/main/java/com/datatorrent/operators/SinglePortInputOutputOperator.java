package com.datatorrent.operators;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.utils.ByteDataGenerator;
import com.datatorrent.utils.OutputController;

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
  private int outScaleFactor = 1;
  protected ByteDataGenerator gen = new ByteDataGenerator(1024, 1024);

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
  private transient OutputController outController = null;

  public void processTuple(byte[] data) {
    outController.process();
  }

  protected void emitTuple(byte[] data) {
    out.emit(data);
  }

  public int getOutScaleFactor()
  {
    return outScaleFactor;
  }

  public void setOutScaleFactor(int factor)
  {
    this.outScaleFactor = factor;
  }

  public ByteDataGenerator getGen()
  {
    return gen;
  }

  public void setGen(ByteDataGenerator gen)
  {
    this.gen = gen;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    outController = new OutputController(out, gen, outScaleFactor);
  }
}
