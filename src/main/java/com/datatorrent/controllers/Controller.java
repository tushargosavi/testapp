package com.datatorrent.controllers;

import com.datatorrent.api.Operator;

/**
 * Any tuple received by the operator goes through this controller.
 * this also has methods of endWindow processing.
 */
public interface Controller<T>
{
  /**
   * called when an tuple is received, the id is the port id on which
   * tuple is received.
   * @param tuple
   */
  void processTuple(T tuple);

  /**
   * called to setup the input controller.
   */
  void setup(Operator op, Operator.Port port);

  Operator.Port getPort(Operator o);
}
