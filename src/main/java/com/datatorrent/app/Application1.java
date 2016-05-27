package com.datatorrent.app;

import java.io.IOException;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.operators.SimOperator;

@ApplicationAnnotation(name = "Application1")
public class Application1 implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    try {
      SimOperator input = dag.addOperator("Input", new SimOperator());
      ByteArrayOutputStream bao = new ByteArrayOutputStream();
      IOUtils.copy(Application1.class.getClassLoader().getResourceAsStream("inputop.json"), bao);
      String str = new String(bao.toByteArray());
      input.setConfiguration(str);

      ConsoleOutputOperator out = dag.addOperator("Out", new ConsoleOutputOperator());
      dag.addStream("s1", input.out1, out.input);

    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }
}
