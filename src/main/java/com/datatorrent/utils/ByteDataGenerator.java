package com.datatorrent.utils;

import java.util.Random;

import com.datatorrent.generator.DataGenerator;

public class ByteDataGenerator implements DataGenerator<byte[]>
{
  private int maxSize;
  private int minSize;
  private int sizeDiff;

  private Random random = new Random();

  public ByteDataGenerator() { }

  public ByteDataGenerator(int min, int max) {
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
