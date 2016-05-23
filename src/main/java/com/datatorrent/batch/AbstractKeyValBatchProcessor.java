package com.datatorrent.batch;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.KeyHashValPair;

public abstract class AbstractKeyValBatchProcessor<K,V> extends BaseOperator implements KeyValBatchProcessor<K,V> {

    private K activeKey = null;

    DefaultInputPort<KeyHashValPair<K,V>> input = new DefaultInputPort<KeyHashValPair<K, V>>() {
        @Override
        public void process(KeyHashValPair<K, V> kvPair) {
            if (kvPair.getClass() == activeKey) {
                processItem(kvPair.getKey(), kvPair.getValue());
            } else {
                if (activeKey != null) {
                    endBatch(activeKey);
                }
                activeKey = kvPair.getKey();
                startBatch(kvPair.getKey());
                processItem(kvPair.getKey(), kvPair.getValue());
            }
        }
    };

}
