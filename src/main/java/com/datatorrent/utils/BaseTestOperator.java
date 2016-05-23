package com.datatorrent.utils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;

import com.google.common.collect.Maps;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.utils.OperatorConf.InputConf;

public class BaseTestOperator<T> extends BaseOperator
{
  public transient DefaultInputPort<T> input = new DefaultInputPort<T>()
  {
    @Override
    public void process(T bytes)
    {
      processTuple(this, bytes);
    }
  };

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
    ObjectReader reader = mapper.reader(OperatorConf.class);

    OperatorConf conf = reader.readValue(configuration);
    return conf;
  }
}
