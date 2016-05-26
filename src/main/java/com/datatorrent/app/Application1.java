package com.datatorrent.app;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.operators.BaseTestOperator;

@ApplicationAnnotation(name = "Application1")
public class Application1 implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    BaseTestOperator input = dag.addOperator("Input", new BaseTestOperator<>());
    ConsoleOutputOperator out = dag.addOperator("Out", new ConsoleOutputOperator());

    dag.addStream("s1", input.out1, out.input);

  }
}
