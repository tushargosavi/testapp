package com.datatorrent.utils;

import java.util.List;

public class OperatorConf
{
  public static class CheckpointConf
  {
    public long size;
    public long warmup;

    @Override
    public String toString()
    {
      return "CheckpointConf{" +
        "size=" + size +
        ", warmup=" + warmup +
        '}';
    }
  }

  public static class OutputConf
  {
    public String name;
    public int scale;
    public int sizeMin;
    public int sizeMax;

    @Override
    public String toString()
    {
      return "OutputConf{" +
        "name='" + name + '\'' +
        ", scale=" + scale +
        ", sizeMin=" + sizeMin +
        ", sizeMax=" + sizeMax +
        '}';
    }
  }

  public static class InputConf
  {
    public String name;
    public long delay;
    public long capacity;
    public List<OutputConf> outputs;

    @Override
    public String toString()
    {
      return "InputConf{" +
        "name='" + name + '\'' +
        ", delay=" + delay +
        ", outputs=" + outputs +
        '}';
    }
  }

  public CheckpointConf checkpoint;
  public long setupDelay;
  public List<InputConf> inputs;

  @Override
  public String toString()
  {
    return "OperatorConf{" +
      "checkpoint=" + checkpoint +
      ", setupDelay=" + setupDelay +
      ", inputs=" + inputs +
      '}';
  }
}
