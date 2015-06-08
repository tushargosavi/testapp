package com.datatorrent.store;

import com.datatorrent.common.util.Slice;
import java.util.Comparator;

public class KeyRange
{
  public final Slice start;
  public final Slice end;

  public KeyRange(Slice start, Slice end)
  {
    this.start = start;
    this.end = end;
  }

  @Override public String toString()
  {
    return "KeyRange{" +
        "start=" + start +
        ", end=" + end +
        '}';
  }

  public KeyRange merge(Comparator<Slice> cmp, KeyRange other) {
    // this object is not initialize
    if (start == null) return other;
    // other object is null
    if (other.start == null) return this;
    return new KeyRange(
        cmp.compare(start, other.start) < 0? start :  other.start,
        cmp.compare(end, other.end) < 0? other.end : end);
  }

  @Override public boolean equals(Object o)
  {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    KeyRange keyRange = (KeyRange) o;

    if (start != null ? !start.equals(keyRange.start) : keyRange.start != null)
      return false;
    return !(end != null ? !end.equals(keyRange.end) : keyRange.end != null);

  }

  @Override public int hashCode()
  {
    int result = start != null ? start.hashCode() : 0;
    result = 31 * result + (end != null ? end.hashCode() : 0);
    return result;
  }
}
