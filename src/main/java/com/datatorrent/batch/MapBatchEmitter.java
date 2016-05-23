package com.datatorrent.batch;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.util.KeyHashValPair;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

public class MapBatchEmitter<K, V> extends BatchEmitter<K, V> {

    private final Map<K, ? extends Collection<V>> map;

    public MapBatchEmitter(Map<K, ? extends Collection<V>> map, DefaultOutputPort<KeyHashValPair<K, V>> out, int emitRate, boolean delete) {
        super(out, emitRate, delete);
        this.map = map;
    }

    @Override
    public Iterator<K> getKeysIterator() {
        return map.keySet().iterator();
    }

    @Override
    public Iterator<V> getBatchIterator(K key) {
        return map.get(key).iterator();
    }
}
