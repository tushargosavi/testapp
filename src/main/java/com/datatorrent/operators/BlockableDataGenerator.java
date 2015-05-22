package com.datatorrent.operators;

import java.io.File;

public class BlockableDataGenerator extends DataGenerator
{
  @Override public void emitTuples()
  {
    try {
      File f = new File("/tmp/block");
      if (f.exists())
        Thread.sleep(120000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    super.emitTuples();
  }
}
