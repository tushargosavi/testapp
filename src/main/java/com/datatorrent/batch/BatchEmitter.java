package com.datatorrent.batch;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.util.KeyHashValPair;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public abstract class BatchEmitter<K, V> {

    private final Map<K, ? extends Collection<V>> map;
    private final DefaultOutputPort<KeyHashValPair<K, V>> out;
    private final int emitRate;
    private final boolean delete;

    private Iterator<K> keyIter = null;
    private Iterator<V> valIter = null;
    private boolean done = false;
    private K key = null;

    public BatchEmitter(DefaultOutputPort<KeyHashValPair<K, V>> out,
                        Map<K, ? extends Collection<V>> map,
                        int emitRate,
                        boolean delete)
    {
        this.map = map;
        this.out = out;
        this.emitRate = emitRate;
        this.delete = delete;
    }

    /**
     * Stay of current batch or move to next. If we have processed
     * all the data then set done to true and return;
     * @return true if there is data available to send.
     */
    private boolean nextBatch() {

        if (done)
            return false;

        if (keyIter != null && valIter != null)
            return true;

        if (keyIter == null)
            keyIter = map.keySet().iterator();

        if (valIter == null) {
            if (keyIter.hasNext()) {
                key = keyIter.next();
                valIter = map.get(key).iterator();
                return true;
            }else {
                done = true;
                return false;
            }
        }
        return true;
    }

    void emit() {

        if (!nextBatch())
            return;

        int count = emitRate;
        while (count > 0) {
            if (valIter.hasNext()) {
                V val = valIter.next();
                if (delete) valIter.remove();
                KeyHashValPair<K, V> pair = new KeyHashValPair(key, val);
                out.emit(pair);
                count--;
            } else {
                valIter = null;
                if (delete) keyIter.remove();
                break;
            }
        }
    }

    public abstract Iterator<V> getBatch(K key);
}
