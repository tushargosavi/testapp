package com.datatorrent.app;

import java.io.IOException;

import org.apache.commons.compress.utils.IOUtils;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.lib.stream.DevNullCounter;
import com.datatorrent.operators.BaseTestOperator;
import com.datatorrent.operators.SimOperator;

@ApplicationAnnotation(name = "Application2")
public class Application2 implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    try {
      SimOperator input = dag.addOperator("Input", new SimOperator());
      ByteArrayOutputStream bao = new ByteArrayOutputStream();
      IOUtils.copy(Application2.class.getClassLoader().getResourceAsStream("inputop.json"), bao);
      String str = new String(bao.toByteArray());
      input.setConfiguration(str);

      SimOperator out = dag.addOperator("Op1", new SimOperator());
      bao = new ByteArrayOutputStream();
      IOUtils.copy(Application2.class.getClassLoader().getResourceAsStream("op2.json"), bao);
      str = new String(bao.toByteArray());
      out.setConfiguration(str);

      DevNullCounter count1 = dag.addOperator("C1", new DevNullCounter());
      DevNullCounter count2 = dag.addOperator("C2", new DevNullCounter());

      dag.addStream("s1", input.out1, out.in1);
      dag.addStream("s2", out.out1, count1.data);
      dag.addStream("s3", out.out2, count2.data);

    } catch (IOException ex) {
      ex.printStackTrace();
    }
  }
}
