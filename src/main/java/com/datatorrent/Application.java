/**
 * Put your copyright and license info here.
 */
package com.datatorrent;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.partitioner.StatelessPartitioner;
import com.datatorrent.lib.stream.DevNullCounter;
import com.datatorrent.operators.FixCapacityOperator;
import com.datatorrent.operators.PassthroughOperator;
import com.datatorrent.utils.ByteDataGenerator;
import org.apache.hadoop.conf.Configuration;


@ApplicationAnnotation(name="MyFirstApplication")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    DataGenerator gen = dag.addOperator("Input", new DataGenerator());
    gen.setGen(new ByteDataGenerator(800, 1024));
    dag.setAttribute(gen, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<Operator>(2));

    FixCapacityOperator oper = dag.addOperator("Operator", new FixCapacityOperator());
    oper.setGen(new ByteDataGenerator(10, 20));
    oper.setCapacity(100);
    dag.setAttribute(oper, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<Operator>(4));

    DevNullCounter<byte[]> counter = dag.addOperator("Counter", new DevNullCounter<byte[]>());
    dag.addStream("seeddata", gen.output, oper.input);
    dag.addStream("counterdata", oper.out, counter.data);
  }
}
