package com.datatorrent;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

public abstract class TimedRateLimitInputOperator<T> extends BaseOperator implements InputOperator
{
  private static final long ONE_SECOND = 1000;
  private transient Context.OperatorContext context;
  protected int batchSize = 100;

  @Override public void emitTuples()
  {
    int count = Math.min(availables.get(), batchSize);

    if (count <= 0)
      return;

    for(int i = 0; i < count; i++) {
      T item = generateTuple();
      emitTuple(item);
    }
    availables.addAndGet(-count);
  }

  @Override
  public void setup(Context.OperatorContext context) {
    this.context = context;
    availables = new AtomicInteger();
    startRateLimitThread();
  }

  private transient Timer timer = null;
  private transient AtomicInteger availables = null;
  private int ratePerSecond = 1;

  void startRateLimitThread() {
    timer = new Timer();
    timer.scheduleAtFixedRate(new TimerTask()
    {
      @Override public void run()
      {
        availables.set(ratePerSecond);
      }
    }, ONE_SECOND, ONE_SECOND);
  }

  protected abstract T generateTuple();
  protected abstract void emitTuple(T tuple);

  public int getRatePerSecond()
  {
    return ratePerSecond;
  }

  public void setRatePerSecond(int ratePerSecond)
  {
    this.ratePerSecond = ratePerSecond;
  }
}
