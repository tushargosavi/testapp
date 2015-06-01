package com.datatorrent.store;

import com.datatorrent.common.util.Slice;
import org.apache.hadoop.io.WritableComparator;

import java.util.Comparator;

public class SliceComparator implements Comparator<Slice>
{
  @Override public int compare(Slice o1, Slice o2)
  {
    return WritableComparator.compareBytes(o1.buffer, o1.offset, o1.length, o2.buffer, o2.offset, o2.length);
  }
}
