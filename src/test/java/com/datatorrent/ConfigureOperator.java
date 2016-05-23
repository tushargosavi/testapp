package com.datatorrent;

import java.io.IOException;

import org.codehaus.jackson.map.DeserializationConfig;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectReader;
import org.junit.Test;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;

import com.datatorrent.utils.OperatorConf;

public class ConfigureOperator
{
  @Test
  public void configure() throws IOException
  {
    ObjectMapper mapper = new ObjectMapper();
    mapper.configure(DeserializationConfig.Feature.FAIL_ON_UNKNOWN_PROPERTIES, false);
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
