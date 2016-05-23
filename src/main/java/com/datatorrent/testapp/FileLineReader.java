package com.datatorrent.testapp;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.hadoop.fs.Path;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator;

public class FileLineReader extends AbstractFileInputOperator<String>
{
  private transient BufferedReader br;

  @Override
  protected InputStream openFile(Path path) throws IOException
  {
    InputStream is = super.openFile(path);
    br = new BufferedReader(new InputStreamReader(is));
    return is;
  }

  @Override
  protected void closeFile(InputStream is) throws IOException
  {
    br.close();
    super.closeFile(is);
  }

  @Override
  protected String readEntity() throws IOException
  {
    return br.readLine();
  }

  public transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

  @Override
  protected void emit(String s)
  {
    output.emit(s);
  }

  public void reset() {
    pendingFiles.clear();
    processedFiles.clear();
  }

  public void setRestart(boolean restart) {
    reset();
  }

  public boolean getRestart() {
    return false;
  }

  private int stage = 0;
  public void reTrigger() throws IOException
  {
    if (fs.exists(new Path("/user/tushar/DONEFILE")) && stage != 1) {
      stage = 1;
      reset();
    }
  }

  @Override
  public void endWindow()
  {
    super.endWindow();
    try {
      reTrigger();
    } catch (IOException ex) {
      //logger.info("Exxception while checking for file ", ex);
    }
  }


}
