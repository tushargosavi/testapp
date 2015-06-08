package com.datatorrent.store;

import com.datatorrent.common.util.Slice;

public class MutableKeyValPair
{
  public Slice key;
  public Slice value;

  public MutableKeyValPair(Slice keySlice, Slice valSlice)
  {
    this.key = keySlice;
    this.value = valSlice;
  }
}

