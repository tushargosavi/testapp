package com.datatorrent.batch;

public class BatchProcessor extends AbstractKeyValBatchProcessor<String, Integer> {


    @Override
    public void startBatch(String key) {
        System.out.println("starting batch " + key);
    }

    @Override
    public void processItem(String key, Integer val) {
        System.out.println("processing item " + key + " + val " + val);

    }

    @Override
    public void endBatch(String key) {
        System.out.println("ending batch " + key);
    }
}
