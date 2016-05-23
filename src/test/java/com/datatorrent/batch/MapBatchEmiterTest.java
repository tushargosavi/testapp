package com.datatorrent.batch;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.lib.util.KeyHashValPair;
import org.junit.Test;

import java.util.*;

public class MapBatchEmiterTest {

    @Test
    public void testBatchEmitts() {
        Map<String, List<Integer>> map = new LinkedHashMap<>();
        map.put("one", Arrays.asList(1, 2, 3, 4, 5));
        map.put("two", Arrays.asList(6, 7, 8, 9, 10));
        map.put("three", Arrays.asList(11, 12, 13, 14, 15));
        map.put("four", Arrays.asList(16, 17, 18, 19, 20));

        BatchEmitter<String, Integer> emitter = new MapBatchEmitter<>(map, new MyPort<KeyHashValPair<String, Integer>>(),
                3, false);

        for(int i = 0; i < 10; i++) {
            System.out.println("window start " + i);
            emitter.emit();
            System.out.println("window end " + i);
        }
    }


    @Test
    public void testBatchEmitts1() {
        Map<String, List<Integer>> map = new LinkedHashMap<>();
        List<Integer> lst1 = new LinkedList<>(); lst1.addAll(Arrays.asList(1, 2, 3, 4, 5));
        List<Integer> lst2 = new LinkedList<>(); lst2.addAll(Arrays.asList(6, 7, 8, 9, 10));
        List<Integer> lst3 = new LinkedList<>(); lst3.addAll(Arrays.asList(11, 12, 13, 14, 15));
        List<Integer> lst4 = new LinkedList<>(); lst4.addAll(Arrays.asList(16, 17, 18, 19, 20));
        map.put("one", lst1);
        map.put("two", lst2);
        map.put("three", lst3);
        map.put("four", lst4);

        BatchEmitter<String, Integer> emitter = new MapBatchEmitter<>(map, new MyPort<KeyHashValPair<String, Integer>>(),
                3, true);

        for(int i = 0; i < 10; i++) {
            System.out.println("window start " + i);
            emitter.emit();
            System.out.println("window end " + i + " size " + map.size());
        }
        System.out.println("size of map is " + map.size());
    }


    static class MyPort<T> extends DefaultOutputPort<T> {
        @Override
        public void emit(T tuple) {
            System.out.println(tuple);
        }
    }

    @Test
    public void getFileName() {
        String accId = "62110664";
        int code = accId.hashCode() & 0x1F;
        System.out.println("hashcode of account id is " + accId.hashCode() + "  hex " + Integer.toHexString(accId.hashCode()));
        System.out.println("code is " + code);
    }
}
