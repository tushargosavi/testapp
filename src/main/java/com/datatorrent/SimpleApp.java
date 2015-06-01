package com.datatorrent;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;
import com.datatorrent.operators.BrustInputOperator;
import com.datatorrent.utils.ByteDataGenerator;
import org.apache.hadoop.conf.Configuration;

@ApplicationAnnotation(name = "SimpleApp")
public class SimpleApp implements StreamingApplication
{
  @Override public void populateDAG(DAG dag, Configuration conf)
  {
    BrustInputOperator gen = dag.addOperator("Gen", new BrustInputOperator());
    gen.setGen(new ByteDataGenerator(10, 10));
    gen.setBurstWindows(5);
    gen.setIdleWindows(60);
    gen.setRatePerSecond(100);

    ConsoleOutputOperator cout = new ConsoleOutputOperator();
    dag.addOperator("Operator1", cout);

    dag.addStream("s1", gen.output, cout.input);
  }
}
