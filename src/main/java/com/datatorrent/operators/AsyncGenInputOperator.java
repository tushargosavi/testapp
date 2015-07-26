package com.datatorrent.operators;

import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class AsyncGenInputOperator<T> extends BaseOperator implements InputOperator
{
  ArrayBlockingQueue<T> queue;
  private int maxQueueSize = 100;
  private int maxTuplesPerWindow = 1000;
  private int maxTuplesPerSecond = 1000;

  private int availableTuplesPerWindow = 0;
  private AtomicInteger availableTuplesPerSecond;

  @Override public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    availableTuplesPerWindow = maxTuplesPerWindow;
  }

  @Override public void emitTuples()
  {
    int count = queue.size();
    count = Math.min(count, maxTuplesPerWindow);
    if (maxTuplesPerSecond > 0)
      count = Math.min(count, availableTuplesPerWindow);

    if (count <= 0)
      return;

    for(int i = 0; i < count; i++) {
      T item = queue.poll();
      emitTuple(item);
    }

    availableTuplesPerWindow -= count;
    availableTuplesPerSecond.addAndGet(-count);
  }

  @Override public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    this.queue = new ArrayBlockingQueue<T>(maxQueueSize);
    availableTuplesPerSecond = new AtomicInteger(0);
    startRateLimitThread();
  }

  private static final long ONE_SECOND = 1000;
  private transient Timer timer = null;
  void startRateLimitThread() {
    timer = new Timer();
    timer.scheduleAtFixedRate(new TimerTask()
    {
      @Override public void run()
      {
        availableTuplesPerSecond.set(maxTuplesPerSecond);
      }
    }, ONE_SECOND, ONE_SECOND);
  }

  @Override public void teardown()
  {
    super.teardown();
    if (timer != null)
      timer.cancel();
  }

  protected abstract void emitTuple(T tuple);

  protected void addTuple(T tuple) {
    queue.add(tuple);
  }
}
