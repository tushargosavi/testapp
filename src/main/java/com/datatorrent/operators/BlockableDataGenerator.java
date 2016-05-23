package com.datatorrent.operators;

import java.util.Random;

import com.datatorrent.api.Context;

public class BlockableDataGenerator extends ByteArrayDataGeneratorOperator
{

  private int slowness = 0;
  private Random rand;
  @Override public void emitTuples()
  {
    try {
      if (slowness > 0)
        Thread.sleep(rand.nextInt(slowness) * 100);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    super.emitTuples();
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    rand = new Random();
  }

  public int getSlowness()
  {
    return slowness;
  }

  public void setSlowness(int slowness)
  {
    this.slowness = slowness;
  }
}
