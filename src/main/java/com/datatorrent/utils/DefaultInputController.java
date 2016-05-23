package com.datatorrent.utils;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Maps;

import com.datatorrent.api.Operator;
import com.datatorrent.utils.OperatorConf.InputConf;
import com.datatorrent.utils.OperatorConf.OutputConf;

public class DefaultInputController<T> implements Controller<T>
{
  private List<DefaultOutputController<T>> outputs = new ArrayList<>();
  private transient InputConf conf;
  private transient RateLimitter rt;

  public DefaultInputController(Operator.InputPort port, Operator o, InputConf conf) {
    this.conf = conf;

    Map<String, Field> outPortFields = Maps.newHashMap();

    try {
      Class clazz = o.getClass();
      for (Field f : clazz.getFields()) {
        if (Operator.OutputPort.class.isAssignableFrom(f.getType())) {
          System.out.println("The name of the object is " + f.getName());
          outPortFields.put(f.getName(), f);
        }
      }


    // populate output controllers.
      for (OutputConf oconf : conf.outputs) {
        if (outPortFields.containsKey(oconf.name)) {
          Field f = outPortFields.get(oconf.name);
          DefaultOutputController<T> oc = new DefaultOutputController<T>(f.get(o), oconf);
          outputs.add(oc);
        } else {
          throw new IllegalArgumentException("Output port not defined " + oconf.name);
        }
      }
    } catch (Throwable ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void processTuple(T tuple)
  {
    if (conf.capacity != 0 && rt != null)
      rt.get();

    if (conf.delay != 0) {
      try {
        Thread.sleep(conf.delay);
      } catch (InterruptedException e) {
      }
    }

    for (DefaultOutputController<T> oc : outputs) {
      oc.processTuple(tuple);
    }
  }

  @Override
  public void setup()
  {
    if (conf.capacity != 0) {
      rt = new RateLimitter();
      rt.setCount(conf.capacity);
      rt.start();
    }
  }
}
