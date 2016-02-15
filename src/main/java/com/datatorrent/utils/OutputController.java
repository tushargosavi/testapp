package com.datatorrent.utils;

import com.datatorrent.api.DefaultOutputPort;

public class OutputController
{
  private transient final ByteDataGenerator gen;
  private final int scale;
  private transient DefaultOutputPort<byte[]> port;
  private int count = 0;

  public OutputController(DefaultOutputPort<byte[]> port, ByteDataGenerator gen, int scale)
  {
    this.port = port;
    this.gen = gen;
    this.scale = scale;
  }

  public void process() {
    if (scale >= 1) {
      for (int i = 0; i < scale; i++) {
        port.emit(gen.generateData());
      }
    } else if (scale < 0) {
      count++;
      if (count % Math.abs(scale) == 0) {
        port.emit(gen.generateData());
        count = 0;
      }
    }
  }
}
