package com.datatorrent.operators;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class StreamJoin3<T> extends BaseStreamJoin<T> implements Partitioner<StreamJoin3>
{
  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort<T> in1 = new DefaultInputPort<T>()
  {
    @Override public void process(T tuple)
    {
      processTuple(tuple);
    }
  };

  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort<T> in2 = new DefaultInputPort<T>()
  {
    @Override public void process(T tuple)
    {
      processTuple(tuple);
    }
  };

  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort<T> in3 = new DefaultInputPort<T>()
  {
    @Override public void process(T tuple)
    {
      processTuple(tuple);
    }
  };
  private int partitionCount = 1;

  private int getNewPartitionCount(
      Collection<com.datatorrent.api.Partitioner.Partition<StreamJoin3>> partitions,
      com.datatorrent.api.Partitioner.PartitioningContext context)
  {
    return DefaultPartition.getRequiredPartitionCount(context, this.partitionCount);
  }

  @Override public Collection<Partition<StreamJoin3>> definePartitions(Collection<Partition<StreamJoin3>> partitions, PartitioningContext context)
  {
    final int partitionSize = getNewPartitionCount(partitions, context);
    List<Partitioner.Partition<StreamJoin3>> newPartitions =
        new ArrayList<Partitioner.Partition<StreamJoin3>>(partitionSize);

    for (int i = 0; i < partitionSize; i++) {
      StreamJoin3 dso = new StreamJoin3();
      Partitioner.Partition<StreamJoin3> po = new DefaultPartition<StreamJoin3>(dso);
      newPartitions.add(po);
    }
    DefaultPartition.assignPartitionKeys(newPartitions, in1);
    DefaultPartition.assignPartitionKeys(newPartitions, in2);
    DefaultPartition.assignPartitionKeys(newPartitions, in3);

    return newPartitions;
  }

  @Override public void partitioned(Map<Integer, Partition<StreamJoin3>> partitions)
  {

  }

  public int getPartitionCount()
  {
    return partitionCount;
  }

  public void setPartitionCount(int partitionCount)
  {
    this.partitionCount = partitionCount;
  }
}
