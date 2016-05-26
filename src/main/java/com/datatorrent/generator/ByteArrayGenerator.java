package com.datatorrent.generator;

import java.util.Random;

import com.datatorrent.generator.DataGenerator;

public class ByteArrayGenerator implements DataGenerator<byte[]>
{
  private int maxSize;
  private int minSize;
  private int sizeDiff;

  private Random random = new Random();

  public ByteArrayGenerator() { }

  public ByteArrayGenerator(int min, int max) {
    maxSize = max;
    minSize = min;
    sizeDiff = max - min;
  }

  @Override
  public byte[] generateData() {
    int size = (sizeDiff <= 0)? minSize : minSize + random.nextInt(sizeDiff);
    byte[] bytes = new byte[size];
    random.nextBytes(bytes);
    return bytes;
  }

  public int getMaxSize()
  {
    return maxSize;
  }

  public void setMaxSize(int maxSize)
  {
    this.maxSize = maxSize;
  }

  public int getMinSize()
  {
    return minSize;
  }

  public void setMinSize(int minSize)
  {
    this.minSize = minSize;
  }
}
