package com.datatorrent;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;

import org.codehaus.jackson.annotate.JsonTypeInfo;
import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;

import com.datatorrent.utils.OperatorConf;

public class ConfigureOperator
{
  @Test
  public void configure() throws IOException, JSONException
  {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_FINAL, JsonTypeInfo.As.PROPERTY);
    mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_CONCRETE_AND_ARRAYS, JsonTypeInfo.As.PROPERTY);

    ObjectReader reader = mapper.reader(OperatorConf.class);
    OperatorConf conf = reader.readValue(ConfigureOperator.class.getClassLoader().getResourceAsStream("opdesc.json"));
    System.out.println(conf);
  }

  @Test
  public void configureBaseOperator() throws IOException
  {
    TestOperator op = new TestOperator();
    ByteArrayOutputStream bao = new ByteArrayOutputStream();
    IOUtils.copy(ConfigureOperator.class.getClassLoader().getResourceAsStream("opdesc.json"), bao);
    String str = new String(bao.toByteArray());
    op.setConfiguration(str);
    op.setup(null);
    System.out.println(str);
  }
}
