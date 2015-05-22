package com.datatorrent.decorators;

import com.datatorrent.api.Context;
import com.datatorrent.api.Operator;

public class OperatorDecorator implements Operator
{
  private Operator operator;
  public Operator getOperator() { return operator; }
  public void setOperator(Operator operator) { this.operator = operator;}

  /* for kryo */
  private OperatorDecorator() {};

  public OperatorDecorator(Operator operator) {
    this.operator = operator;
  }

  @Override public void setup(Context.OperatorContext context)
  {
    operator.setup(context);
  }

  @Override public void endWindow()
  {
    operator.endWindow();
  }

  @Override public void teardown()
  {
    operator.teardown();
  }

  @Override public void beginWindow(long windowId)
  {
    operator.beginWindow(windowId);
  }
}
