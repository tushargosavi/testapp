package com.datatorrent.operators;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;

public class MultiPortOperator2 extends FixCapacityOperator
{
  public transient DefaultOutputPort out1 = new DefaultOutputPort();
  ByteArrayDataGeneratorOperator gen2 = new ByteArrayDataGeneratorOperator();

  @Override
  public void processTuple(String id, byte[] data)
  {
    super.processTuple(id, data);
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
  }
}
