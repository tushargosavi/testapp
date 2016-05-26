package com.datatorrent.operators;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;

import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;

import com.google.common.collect.Maps;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.controllers.AsyncController;
import com.datatorrent.controllers.Controller;
import com.datatorrent.controllers.DefaultInputController;
import com.datatorrent.utils.OperatorConf;
import com.datatorrent.utils.OperatorConf.InputConf;
import com.datatorrent.utils.OperatorConf.AsyncOutputConf;

public class BaseTestOperator<T> extends BaseOperator implements InputOperator
{
  public transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T bytes)
    {
      processTuple(this, bytes);
    }
  };

  public transient DefaultOutputPort<T> out1 = new DefaultOutputPort<T>();

  private String configuration;

  public String getConfiguration()
  {
    return configuration;
  }

  public void setConfiguration(String configuration)
  {
    this.configuration = configuration;
  }

  protected void processTuple(InputPort port, T data) {
    Controller ic = controllers.get(port);
    if (ic != null) {
      ic.processTuple(data);
    }
  }

  Map<InputPort, Controller> controllers = Maps.newHashMap();
  Map<OutputPort, AsyncController> asyncControllers = Maps.newHashMap();

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    try {
      OperatorConf conf = getOperatorConfig(configuration);
      Class clazz = this.getClass();
      for (InputConf iconf : conf.inputs) {
        Field field = clazz.getField(iconf.name);
        InputPort port = (InputPort)field.get(this);
        controllers.put(port, new DefaultInputController(port, this, iconf));
      }

      for (AsyncOutputConf oconf : conf.outputs) {
        Field field = clazz.getField(oconf.name);
        OutputPort port = (OutputPort)field.get(this);
        asyncControllers.put(port, oconf.controller);
      }
    } catch (IOException e) {
      throw new RuntimeException("Can not configuration operator ", e);
    } catch (NoSuchFieldException e) {
      throw new RuntimeException(e);
    } catch (IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  private OperatorConf getOperatorConfig(String configuration) throws IOException
  {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL, JsonTypeInfo.As.PROPERTY);
    mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_CONCRETE_AND_ARRAYS, JsonTypeInfo.As.PROPERTY);
    ObjectReader reader = mapper.reader(OperatorConf.class);
    OperatorConf conf = reader.readValue(configuration);
    return conf;
  }

  @Override
  public void emitTuples()
  {
    for (AsyncController ac : asyncControllers.values()) {
      ac.emitTuples();
    }
  }

  @Override
  public void endWindow()
  {
    for (AsyncController ac : asyncControllers.values()) {
      ac.endWindow();
    }
  }
}
