package com.datatorrent.controllers;

public interface AsyncController
{
  void setup();

  /** called when operator is idle */
  void idleTimeout();

  /** called at input operator to emit the tuples, not applicable for other operators */
  void emitTuples();

  /** called at endWindow to emit tuples */
  void endWindow();
}
