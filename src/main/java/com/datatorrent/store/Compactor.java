package com.datatorrent.store;

import com.datatorrent.common.util.Slice;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.file.tfile.TFile;

import java.io.IOException;
import java.util.*;

/* Return edits to be done to metadata after compaction is over */
public interface Compactor
{
  VersionEdit compact();
}

interface CompactionStrategy {
  List<FileInfo> level0Files();
}

abstract class BaseCompator implements Compactor {
  protected final Comparator cmp;
  protected final FileSystem fs;

  protected BaseCompator(FileSystem fs, Comparator cmp)
  {
    this.fs = fs;
    this.cmp = cmp;
  }

  public void merge(List<FileInfo> lst1) throws IOException
  {
    Map<Slice, Slice> data = new TreeMap<Slice, Slice>(cmp);
    for(FileInfo file : lst1) {
      System.out.println("reading file " + file);
      TFileScanner s = createScanner(file);
      readAll(createScanner(file), data);
      s.close();
    }

    //System.out.println("flushing combined data for level " + level);
    //flushData(getNextFile(level+1), data);
  }

  protected void readAll(TFileScanner scanner, Map<Slice, Slice> data) throws IOException
  {
    scanner.rewind();
    int count = 0;
    while(!scanner.atEnd()) {
      System.out.println("reading key " + count);
      //TFile.Reader.Scanner.Entry e1 = scanner.entry();
      //in.put(getKeySlice(e1), getValSlice(e1));
      count++;
      scanner.advance();
    }
    System.out.println("reading done , read " + count);
  }

  private TFileScanner createScanner(FileInfo file) throws IOException
  {
    return new TFileScanner(fs, file);
  }
}

class Level0Compactor extends BaseCompator
{
  Index meta;
  VersionEdit edits;
  List<FileInfo> level0Files;
  List<FileInfo> level1Files;
  KeyRange range;

  public Level0Compactor(Index meta, FileSystem fs, Comparator cmp) {
    super(fs, cmp);
    this.meta = meta;
    edits = new VersionEdit();
    range = new KeyRange(null, null);
  }

  /* get first file */
  List<FileInfo> getLeve0FilesToCompact()
  {
    FileInfo finfo = meta.get(0).get(0);
    List<FileInfo> lst  = new ArrayList();
    range.merge(cmp, new KeyRange(finfo.startKey, finfo.lastKey));
    lst.add(finfo);
    return lst;
  }

  List<FileInfo> getLevel1FilesForCompact()
  {
    //level1Files = meta.get(1).getFilesInRange(cmp, range);
    //range = range.merge(cmp, getRange(level1Files));
    return level1Files;
  }

  private KeyRange getRange(List<FileInfo> level0Files)
  {
    KeyRange range = new KeyRange(null, null);
    for(FileInfo f : level0Files) {
    //  range = range.merge(cmp, new KeyRange(f.startKey, f.lastKey));
    }
    return range;
  }

  @Override public VersionEdit compact()
  {
    level0Files = getLeve0FilesToCompact();
    range = getRange(level0Files);
    level1Files = getLevel1FilesForCompact();

    //FileInfo merged = mergeFiles(level0Files);

    return edits;
  }
}
