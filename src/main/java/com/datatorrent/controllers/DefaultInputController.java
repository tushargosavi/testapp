package com.datatorrent.controllers;

import java.util.ArrayList;

import com.datatorrent.api.Operator;
import com.datatorrent.utils.RateLimitter;

public class DefaultInputController<T> implements Controller<T>
{
  private ArrayList<Controller<T>> outputs = new ArrayList<>();
  private transient RateLimitter rt;
  private String name;
  public long delay;
  public long capacity;

  @Override
  public void processTuple(T tuple)
  {
    if (capacity != 0 && rt != null)
      rt.get();

    if (delay != 0) {
      try {
        Thread.sleep(delay);
      } catch (InterruptedException e) {
      }
    }

    for (Controller<T> oc : outputs) {
      oc.processTuple(tuple);
    }
  }

  @Override
  public void setup(Operator o, Operator.Port port)
  {
    for (Controller oc : outputs) {
      oc.setup(o, oc.getPort(o));
    }

    if (capacity != 0) {
      rt = new RateLimitter();
      rt.setCount(capacity);
      rt.start();
    }
  }

  public String getName()
  {
    return name;
  }

  public void setName(String name)
  {
    this.name = name;
  }

  @Override
  public Operator.Port getPort(Operator o)
  {
    return null;
  }
}
