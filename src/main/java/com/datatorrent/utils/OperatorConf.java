package com.datatorrent.utils;

import java.util.ArrayList;

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

  public CheckpointConf checkpoint;
  public long setupDelay;
  public ArrayList<Controller> inputs;
  public ArrayList<AsyncController> outputs;

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
