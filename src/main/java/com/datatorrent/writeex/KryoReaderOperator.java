package com.datatorrent.writeex;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.Path;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator;

public class KryoReaderOperator<T> extends AbstractFileInputOperator<T>
{

  private transient Input kin;
  private transient Kryo kryo = new Kryo();
  Class clazz;

  @Override
  protected InputStream openFile(Path path) throws IOException
  {
    InputStream is = super.openFile(path);
    kin = new Input(is);
    return is;
  }

  @Override
  protected void closeFile(InputStream is) throws IOException
  {
    kin.close();
    super.closeFile(is);
  }

  @Override
  protected T readEntity() throws IOException
  {
    if(clazz != null)
      return (T)kryo.readObject(kin, clazz);
    else
      return (T)kryo.readClassAndObject(kin);
  }

  public transient final DefaultOutputPort<T> output = new DefaultOutputPort<>();

  @Override
  protected void emit(T o)
  {
    output.emit(o);
  }
}
