package com.datatorrent.controllers;

import java.lang.reflect.Field;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;
import com.datatorrent.generator.DataGenerator;

/**
 * controls the amount of data send on the output port of an
 * operator.
 *
 * scale determines number of tuples sent for each invocation of process
 * by the controller. Most of the operator will call process when a
 * tuple is received by it. In some instance process can be called through
 * an timer task, to emit at steady rate.
 * @param <T>
 */
public class DefaultOutputController<T> implements Controller<T>
{
  private transient DefaultOutputPort<T> port;
  String name;
  private int count = 0;
  private int scale = 1;
  DataGenerator<T> gen;

  public DefaultOutputController() { }

  @Override
  public void processTuple(T tuple) {
    if (scale >= 1) {
      for (int i = 0; i < scale; i++) {
        port.emit(gen.generateData());
      }
    } else if (scale < 0) {
      count++;
      if (count % Math.abs(scale) == 0) {
        port.emit(gen.generateData());
        count = 0;
      }
    }
  }

  public DataGenerator<T> getGen()
  {
    return gen;
  }

  public void setGen(DataGenerator<T> gen)
  {
    this.gen = gen;
  }

  public int getCount()
  {
    return count;
  }

  public void setCount(int count)
  {
    this.count = count;
  }

  public int getScale()
  {
    return scale;
  }

  public void setScale(int scale)
  {
    this.scale = scale;
  }

  @Override
  public void setup(Operator op, Operator.Port port)
  {
    this.port = (DefaultOutputPort)port;
  }

  @Override
  public Operator.Port getPort(Operator o)
  {
    try {
      Field f = o.getClass().getField(name);
      return (Operator.Port)f.get(o);
    } catch (Throwable th) {
      return null;
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
}
