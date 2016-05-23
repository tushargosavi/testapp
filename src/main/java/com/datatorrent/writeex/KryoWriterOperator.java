package com.datatorrent.writeex;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;

import com.datatorrent.api.Context;
import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

public class KryoWriterOperator<T> extends AbstractFileOutputOperator<T> {

    transient Kryo kryo = new Kryo();
    private int id;
    public String baseName;
    private Output kout;

    @Override
    protected FSDataOutputStream openStream(Path filepath, boolean append) throws IOException {
        FSDataOutputStream out = super.openStream(filepath, append);
        kout = new Output(out);
        return out;
    }

    @Override
    protected void closeStream(AbstractFileOutputOperator.FSFilterStreamContext streamContext) throws IOException {
        kout.close();
        super.closeStream(streamContext);
    }

    @Override
    protected String getFileName(T tuple) {
        return getBaseName() + "_" + getId();
    }

    @Override
    protected byte[] getBytesForTuple(T tuple) {
        kryo.writeClassAndObject(kout, tuple);
        // return empty byte array, writing empty byte array, should not cause any
        // problem.
        return new byte[0];
    }

    @Override
    public void setup(Context.OperatorContext context) {
        super.setup(context);
        this.id = context.getId();
    }

    public int getId() {
        return id;
    }

    public String getBaseName() {
        return baseName;
    }

    public void setBaseName(String baseName) {
        this.baseName = baseName;
    }
}
