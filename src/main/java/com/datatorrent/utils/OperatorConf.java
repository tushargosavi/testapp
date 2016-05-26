package com.datatorrent.utils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.datatorrent.controllers.AsyncController;
import com.datatorrent.controllers.Controller;

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
    public Controller controller;

    @Override
    public String toString()
    {
      return "OutputConf{" +
        "name='" + name + '\'' +
        ", controller=" + controller +
        '}';
    }
  }

  public static class AsyncOutputConf
  {
    public String name;
    public AsyncController controller;

    @Override
    public String toString()
    {
      return "OutputConf{" +
        "name='" + name + '\'' +
        ", controller=" + controller +
        '}';
    }
  }

  public static class InputConf
  {
    public String name;
    public long delay;
    public long capacity;
    public ArrayList<OutputConf> outputs;

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
  public ArrayList<InputConf> inputs;
  public ArrayList<AsyncOutputConf> outputs;

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
