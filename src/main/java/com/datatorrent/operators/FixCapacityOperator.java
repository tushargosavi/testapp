package com.datatorrent.operators;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.utils.ByteDataGenerator;
import com.datatorrent.utils.RateLimitter;

/* This operator has capacity to process x items per second
 */
public class FixCapacityOperator extends SinglePortInputOutputOperator
{

  private int capacity;
  private transient RateLimitter rt;
  private ByteDataGenerator gen = new ByteDataGenerator(1024, 1024);

  @Override public void processTuple(byte[] data)
  {
    if (rt.get()) {
      super.processTuple(data);
    }
  }

  public int getCapacity()
  {
    return capacity;
  }

  public void setCapacity(int capacity)
  {
    this.capacity = capacity;
    if (rt != null)
      rt.setCount(capacity);
  }

  @Override public void setup(Context.OperatorContext context)
  {
    rt = new RateLimitter();
    rt.setCount(capacity);
    rt.start();
    super.setup(context);
  }

  @Override public void teardown()
  {
    rt.stop();
    super.teardown();
  }
}
