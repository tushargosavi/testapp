package com.datatorrent.utils;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Very simple rate limiter class.
 */
public class RateLimitter
{
  private transient Timer timer;
  private transient AtomicLong availables;
  private long count;
  private volatile boolean stopped;

  public void start() {
    timer = new Timer();
    final Object obj = this;
    availables = new AtomicLong();
    timer.scheduleAtFixedRate(new TimerTask() {
      @Override public void run()
      {
        availables.set(count);
        synchronized (obj) {
          obj.notify();
        }
      }
    }, 1000, 1000);
  }

  public boolean get() {
    while (!stopped) {
      long old = availables.getAndDecrement();
      if (old > 0)
        return true;
      try {
        synchronized(this) {
          this.wait();
        }
      } catch (InterruptedException e) {
        return false;
      }
    }
    return false;
  }

  public void stop() {
    stopped = true;
    if (timer != null) {
      timer.cancel();
      synchronized (this) {
        this.notifyAll();
      }
    }
  }

  public long getCount()
  {
    return count;
  }

  public void setCount(long count)
  {
    this.count = count;
  }

  public boolean isStopped() {
    return stopped;
  }

  public long getAvailables() {
    return availables.get();
  }

  public static void main(String[] args) throws InterruptedException
  {
    final RateLimitter limitter = new RateLimitter();
    limitter.setCount(100);
    limitter.start();

    Thread t = new Thread() {
      public void run() {
        int count = 0;
        while(!limitter.isStopped()) {
          if (limitter.get()) {
            count++;
          }
        }
      }
    };
    t.start();
    Thread.sleep(10000);
    limitter.stop();
    t.join();
  }
}
