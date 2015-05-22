package com.datatorrent.decorators;

import com.datatorrent.api.Context;
import com.datatorrent.api.InputOperator;

public class InputOperatorDecorator implements InputOperator
{
  InputOperator in;

  public InputOperator getIn()
  {
    return in;
  }

  public void setIn(InputOperator in)
  {
    this.in = in;
  }

  /* for kryo */
  private InputOperatorDecorator() { }

  public InputOperatorDecorator(InputOperator in)
  {
    this.in = in;
  }

  @Override public void emitTuples()
  {
    in.emitTuples();
  }

  @Override public void beginWindow(long windowId)
  {
    in.beginWindow(windowId);
  }

  @Override public void endWindow()
  {
    in.endWindow();
  }

  @Override public void setup(Context.OperatorContext context)
  {
    in.setup(context);
  }

  @Override public void teardown()
  {
    in.teardown();
  }
}
