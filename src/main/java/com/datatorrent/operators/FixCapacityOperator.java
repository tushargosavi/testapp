package com.datatorrent.operators;

import java.util.Map;

import com.google.common.collect.Maps;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.utils.ByteDataGenerator;
import com.datatorrent.utils.RateLimitter;

/**
 *  This operator has capacity to process x items per second
 * @displayName Fixed capacity operator
 * @category benchmark
 * @tags operator
 */
public class FixCapacityOperator extends SinglePortInputOutputOperator
{

  private int capacity;
  private transient RateLimitter rt;
  private ByteDataGenerator gen = new ByteDataGenerator(1024, 1024);
  transient Map<String, Port> ports = Maps.newHashMap();

  @Override public void processTuple(String id, byte[] data)
  {
    if (capacity == 0 || rt.get()) {
      super.processTuple(id, data);
    }
  }

  public int getCapacity()
  {
    return capacity;
  }

  public void setCapacity(int capacity)
  {
    this.capacity = capacity;
    // for allowing dynamic change while application is running.
    if (rt != null)
      rt.setCount(capacity);
  }

  @Override public void setup(Context.OperatorContext context)
  {
    if (capacity != 0) {
      rt = new RateLimitter();
      rt.setCount(capacity);
      rt.start();
    }
    super.setup(context);
  }

  @Override public void teardown()
  {
    if (rt != null) {
      rt.stop();
    }
    super.teardown();
  }
}
