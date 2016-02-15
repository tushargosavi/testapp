package com.datatorrent.operators;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultPartition;
import com.datatorrent.api.Partitioner;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;

/**
 * Join two stream together.
 *
 * @displayName Stream join 2.
 * @category benchmark
 * @tags join
 */
public class StreamJoin2 extends BaseStreamJoin implements Partitioner<StreamJoin2>
{
  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort<byte[]> in1 = new DefaultInputPort<byte[]>()
  {
    @Override public void process(byte[] tuple)
    {
      processTuple(tuple);
    }
  };

  @InputPortFieldAnnotation(optional = true)
  public transient DefaultInputPort<byte[]> in2 = new DefaultInputPort<byte[]>()
  {
    @Override public void process(byte[] tuple)
    {
      processTuple(tuple);
    }
  };

  private int partitionCount = 1;
  private boolean split1 = true;
  private boolean split2 = true;

  private int getNewPartitionCount(
    Collection<Partitioner.Partition<StreamJoin2>> partitions,
    com.datatorrent.api.Partitioner.PartitioningContext context)
  {
    return DefaultPartition.getRequiredPartitionCount(context, this.partitionCount);
  }

  @Override public Collection<Partitioner.Partition<StreamJoin2>> definePartitions(Collection<Partitioner.Partition<StreamJoin2>> partitions, Partitioner.PartitioningContext context)
  {
    final int partitionSize = getNewPartitionCount(partitions, context);
    List<Partitioner.Partition<StreamJoin2>> newPartitions =
      new ArrayList<>(partitionSize);

    for (int i = 0; i < partitionSize; i++) {
      StreamJoin2 dso = new StreamJoin2();
      Partitioner.Partition<StreamJoin2> po = new DefaultPartition<>(dso);
      newPartitions.add(po);
    }

    if (split1) DefaultPartition.assignPartitionKeys(newPartitions, in1);
    if (split2) DefaultPartition.assignPartitionKeys(newPartitions, in2);

    return newPartitions;
  }

  @Override
  public void partitioned(Map<Integer, Partition<StreamJoin2>> map)
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

  public boolean isSplit1()
  {
    return split1;
  }

  public void setSplit1(boolean split1)
  {
    this.split1 = split1;
  }

  public boolean isSplit2()
  {
    return split2;
  }

  public void setSplit2(boolean split2)
  {
    this.split2 = split2;
  }
}
