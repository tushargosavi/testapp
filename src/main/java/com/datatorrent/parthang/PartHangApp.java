package com.datatorrent.parthang;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.operators.ByteArrayDataGeneratorOperator;
import com.datatorrent.operators.FixCapacityOperator;
import com.datatorrent.utils.ByteDataGenerator;

@ApplicationAnnotation(name = "PartitionHangApp")
public class PartHangApp implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    int timeoutWindows = 30000000;

    ByteArrayDataGeneratorOperator input = dag.addOperator("Input", new ByteArrayDataGeneratorOperator());
    input.setRatePerSecond(200);
    ByteDataGenerator gen = new ByteDataGenerator();
    gen.setMinSize(200);
    gen.setMaxSize(500);
    input.setGen(gen);
    dag.getMeta(input).getAttributes().put(Context.OperatorContext.PARTITIONER, new StatelessPartitioner<Operator>(32));
    dag.getMeta(input).getAttributes().put(Context.OperatorContext.MEMORY_MB, 4096);
    dag.getMeta(input).getAttributes().put(Context.OperatorContext.TIMEOUT_WINDOW_COUNT, timeoutWindows);

    FixCapacityOperator op1 = dag.addOperator("O1", new FixCapacityOperator());
    op1.setOutScaleFactor(40);
    ByteDataGenerator gen1 = new ByteDataGenerator();
    gen1.setMinSize(100);
    gen1.setMaxSize(1000);
    op1.setGen(gen1);
    dag.setInputPortAttribute(op1.input, Context.PortContext.PARTITION_PARALLEL, true);
    dag.getMeta(op1).getAttributes().put(Context.OperatorContext.MEMORY_MB, 10096);
    dag.setUnifierAttribute(op1.out, Context.OperatorContext.MEMORY_MB, 4096);
    dag.setUnifierAttribute(op1.out, Context.OperatorContext.TIMEOUT_WINDOW_COUNT, timeoutWindows);
    dag.getMeta(op1).getAttributes().put(Context.OperatorContext.TIMEOUT_WINDOW_COUNT, timeoutWindows);
    op1.setCapacity(200);
    dag.addStream("s1", input.output, op1.input).setLocality(DAG.Locality.CONTAINER_LOCAL);


    FixCapacityOperator op2 = dag.addOperator("O2", new FixCapacityOperator());
    dag.getMeta(op2).getAttributes().put(Context.OperatorContext.MEMORY_MB, 10096);
    dag.getMeta(op2).getAttributes().put(Context.OperatorContext.PARTITIONER, new StatelessPartitioner<Operator>(32));
    dag.getMeta(op2).getAttributes().put(Context.OperatorContext.TIMEOUT_WINDOW_COUNT, timeoutWindows);
    op2.setCapacity(100 * 50);
    dag.addStream("s2", op1.out, op2.input);

    FixCapacityOperator output = dag.addOperator("Output", new FixCapacityOperator());
    dag.getMeta(output).getAttributes().put(Context.OperatorContext.TIMEOUT_WINDOW_COUNT, timeoutWindows);
    dag.setInputPortAttribute(output.input, Context.PortContext.PARTITION_PARALLEL, true);
    dag.addStream("s3", op2.out, output.input).setLocality(DAG.Locality.CONTAINER_LOCAL);

  }

}
