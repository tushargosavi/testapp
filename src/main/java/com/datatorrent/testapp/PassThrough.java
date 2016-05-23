package com.datatorrent.testapp;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

public class PassThrough extends BaseOperator {

    public transient DefaultInputPort<Integer> input = new DefaultInputPort<Integer>() {
        @Override
        public void process(Integer integer) {
            output.emit(integer);
        }
    };

    public transient DefaultOutputPort<Integer> output = new DefaultOutputPort<Integer>();
}
