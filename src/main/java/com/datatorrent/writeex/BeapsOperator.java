package com.datatorrent.writeex;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

import java.util.Timer;
import java.util.TimerTask;

public class BeapsOperator extends BaseOperator implements InputOperator {

    public transient DefaultOutputPort<Integer> beap = new DefaultOutputPort<>();
    private int count;
    public static final int ONE_SECOND = 1000;
    private transient Timer timer;

    @Override
    public void emitTuples() {

    }

    @Override
    public void setup(Context.OperatorContext context) {
        timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask()
        {
            @Override public void run()
            {
                count++;
                beap.emit(count);
            }
        }, ONE_SECOND, ONE_SECOND);
    }
}
