package com.datatorrent.operators;

import com.datatorrent.api.DefaultInputPort;

/**
 * Join two stream together.
 *
 * @displayName Stream join 2.
 * @category benchmark
 * @tags join
 */
public class StreamJoin2<T> extends BaseStreamJoin<T>
{
  public transient DefaultInputPort<T> in1 = new DefaultInputPort<T>()
  {
    @Override public void process(T tuple)
    {
      processTuple(tuple);
    }
  };

  public transient DefaultInputPort<T> in2 = new DefaultInputPort<T>()
  {
    @Override public void process(T tuple)
    {
      processTuple(tuple);
    }
  };
}
