package com.datatorrent;

import org.junit.Test;

import java.io.IOException;

public class SimpleTest
{
  @Test
  public void testRun() throws IOException
  {
    Runtime.getRuntime().exec("/tmp/test.sh");
  }
}
