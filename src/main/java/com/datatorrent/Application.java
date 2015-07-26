/**
 * Put your copyright and license info here.
 */
package com.datatorrent;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.lib.stream.DevNullCounter;
import com.datatorrent.operators.*;
import com.datatorrent.utils.ByteDataGenerator;
import org.apache.hadoop.conf.Configuration;


@ApplicationAnnotation(name="BigApplication")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    /* First pipe line */
    BlockableDataGenerator gen1 = dag.addOperator("Input1", new BlockableDataGenerator());
    gen1.setGen(new ByteDataGenerator(5102, 14336));
    gen1.setRatePerSecond(1000);
    dag.setAttribute(gen1, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<Operator>(8));

    SinglePortInputOutputOperator dc1 = dag.addOperator("DC1", new SinglePortInputOutputOperator());
    dc1.setGen(new ByteDataGenerator(160, 160));
    dc1.setOutputScaleFactor(10);
    dag.setInputPortAttribute(dc1.input, Context.PortContext.PARTITION_PARALLEL, true);

    dag.addStream("s1", gen1.output, dc1.input);

    /* second pipeline */
    DataGenerator gen2 = dag.addOperator("Input2", new DataGenerator());
    gen2.setGen(new ByteDataGenerator(5102, 14336));
    gen2.setRatePerSecond(1000);
    dag.setAttribute(gen2, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<Operator>(8));

    SinglePortInputOutputOperator dc2 = dag.addOperator("DC2", new SinglePortInputOutputOperator());
    dc2.setGen(new ByteDataGenerator(160, 160));
    dc2.setOutputScaleFactor(10);
    dag.setInputPortAttribute(dc2.input, Context.PortContext.PARTITION_PARALLEL, true);

    dag.addStream("s2", gen2.output, dc2.input);

    /* third pipeline */
    DataGenerator gen3 = dag.addOperator("Input3", new DataGenerator());
    gen3.setGen(new ByteDataGenerator(5102, 14336));
    gen3.setRatePerSecond(500);
    dag.setAttribute(gen3, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<Operator>(10));

    FixCapacityOperator oper = dag.addOperator("Converter", new FixCapacityOperator());
    oper.setGen(new ByteDataGenerator(3000, 8000));
    oper.setCapacity(2000);
    oper.setOutputScaleFactor(-4);
    dag.setInputPortAttribute(oper.input, Context.PortContext.PARTITION_PARALLEL, true);

    dag.addStream("s3", gen3.output, oper.input);

    SinglePortInputOutputOperator dc3 = dag.addOperator("DC3", new SinglePortInputOutputOperator());
    dc3.setGen(new ByteDataGenerator(160, 160));
    dc3.setOutputScaleFactor(10);
    dag.setInputPortAttribute(dc3.input, Context.PortContext.PARTITION_PARALLEL, true);
    dag.addStream("s4", oper.out, dc3.input);


    /* all joins at store */
    StreamJoin3 last = dag.addOperator("Store", new StreamJoin3());
    last.setPartitionCount(8);

    DevNullCounter<byte[]> counter = dag.addOperator("Counter", new DevNullCounter<byte[]>());
    dag.setInputPortAttribute(counter.data, Context.PortContext.PARTITION_PARALLEL, true);

    /* connections */
    dag.addStream("s5", dc1.out, last.in1);
    dag.addStream("s6", dc2.out, last.in2);
    dag.addStream("s7", dc3.out, last.in3);
    dag.addStream("s8", last.out, counter.data).setLocality(DAG.Locality.THREAD_LOCAL);

  }
}
