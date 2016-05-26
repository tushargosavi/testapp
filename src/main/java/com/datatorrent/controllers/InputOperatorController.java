package com.datatorrent.controllers;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.generator.DataGenerator;

public class InputOperatorController<T> implements AsyncController
{
  private DefaultOutputPort<T> output;
  private static final long ONE_SECOND = 1000;
  protected int batchSize = 100;
  protected DataGenerator<T> gen;

  @Override
  public void idleTimeout()
  {

  }

  @Override
  public void emitTuples()
  {
    int count = Math.min(availables.get(), batchSize);

    if (count <= 0)
      return;

    for(int i = 0; i < count; i++) {
      T item = gen.generateData();
      output.emit(item);
    }
    availables.addAndGet(-count);
  }

  @Override
  public void endWindow()
  {

  }

  @Override
  public void setup()
  {
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

  public int getBatchSize()
  {
    return batchSize;
  }

  public void setBatchSize(int batchSize)
  {
    this.batchSize = batchSize;
  }

  public DataGenerator<T> getGen()
  {
    return gen;
  }

  public void setGen(DataGenerator<T> gen)
  {
    this.gen = gen;
  }

  public int getRatePerSecond()
  {
    return ratePerSecond;
  }

  public void setRatePerSecond(int ratePerSecond)
  {
    this.ratePerSecond = ratePerSecond;
  }
}
