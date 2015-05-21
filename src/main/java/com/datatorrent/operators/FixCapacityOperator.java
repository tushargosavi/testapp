package com.datatorrent.operators;

import com.datatorrent.BaseBenchmarkOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.utils.ByteDataGenerator;
import com.datatorrent.utils.RateLimitter;

/* This operator has capacity to process x items per second
 */
public class FixCapacityOperator extends BaseBenchmarkOperator
{
  public transient DefaultOutputPort<byte[]> out = new DefaultOutputPort<byte[]>();

  private int capacity;
  private transient RateLimitter rt;
  private ByteDataGenerator gen = new ByteDataGenerator(1024, 1024);

  @Override public void processTuple(byte[] data)
  {
    if (rt.get()) {
      emitTuple(gen.generateData());
    }
  }

  @Override protected void emitTuple(byte[] data)
  {
    out.emit(data);
  }

  public int getCapacity()
  {
    return capacity;
  }

  public void setCapacity(int capacity)
  {
    this.capacity = capacity;
  }

  @Override public void setup(Context.OperatorContext context)
  {
    rt = new RateLimitter();
    rt.setCount(capacity);
    rt.start();
    super.setup(context);
  }

  public ByteDataGenerator getGen()
  {
    return gen;
  }

  public void setGen(ByteDataGenerator gen)
  {
    this.gen = gen;
  }

  @Override public void teardown()
  {
    rt.stop();
    super.teardown();
  }
}
