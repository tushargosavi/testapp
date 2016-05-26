package com.datatorrent.controllers;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

public interface AsyncController
{
  void setup();

  /** called when operator is idle */
  void idleTimeout();

  /** called at input operator to emit the tuples, not applicable for other operators */
  void emitTuples();

  /** called at endWindow to emit tuples */
  void endWindow();

  /** set output port */
  void setOutputPort(DefaultOutputPort port);
}
