package com.datatorrent.store.support;

import com.datatorrent.store.Index;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class PrintIndex
{
  public static void main(String[] args) throws IOException
  {
    Index idx = new Index();
    String baseDir = args[0];
    Path p = new Path(baseDir);
    FileSystem fs = FileSystem.newInstance(p.toUri(), new Configuration());
    FSDataInputStream in = fs.open(new Path(baseDir + "/INDEX"));
    idx.load(in);
    in.close();
    idx.print();
  }
}
