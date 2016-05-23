package com.datatorrent.batch;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.KeyHashValPair;

import java.util.*;

public class BatchGenerator  extends BaseOperator implements InputOperator {

    private BatchEmitter<String, Integer> emitter;
    public transient DefaultOutputPort<KeyHashValPair<String, Integer>> out = new DefaultOutputPort<>();

    @Override
    public void setup(Context.OperatorContext context) {
        Map<String, List<Integer>> map = new LinkedHashMap<>();
        List<Integer> lst1 = new LinkedList<>(); lst1.addAll(Arrays.asList(1, 2, 3, 4, 5));
        List<Integer> lst2 = new LinkedList<>(); lst2.addAll(Arrays.asList(6, 7, 8, 9, 10));
        List<Integer> lst3 = new LinkedList<>(); lst3.addAll(Arrays.asList(11, 12, 13, 14, 15));
        List<Integer> lst4 = new LinkedList<>(); lst4.addAll(Arrays.asList(16, 17, 18, 19, 20));
        map.put("one", lst1);
        map.put("two", lst2);
        map.put("three", lst3);
        map.put("four", lst4);

        emitter = new MapBatchEmitter<>(map, out, 3, true);
    }

    @Override
    public void beginWindow(long windowId) {
        if (emitter != null)
            emitter.beginWindow();
    }

    @Override
    public void emitTuples() {
        emitter.emit();
    }
}
