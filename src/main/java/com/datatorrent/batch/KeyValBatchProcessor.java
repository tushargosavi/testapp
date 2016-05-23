package com.datatorrent.batch;

public interface KeyValBatchProcessor<K,V> {
    /**
     * start current batch with key as key.
     * @param key
     */
    public void startBatch(K key);

    /**
     * process an item in a batch
     * @param key
     * @param val
     */
    public void processItem(K key, V val);

    /**
     * end current batch
     */
    public void endBatch(K key);
}
