package com.datatorrent.operators;

/* Input operator which generate burst of data */
public class BrustInputOperator extends DataGenerator
{
  /* generate burst for n windows, followed by m idle windows */
  private int burstWindows;
  private int idleWindows;
  private transient boolean emitInThisWindow = false;

  @Override public void emitTuples()
  {
    if (emitInThisWindow)
      super.emitTuples();
  }

  @Override public void beginWindow(long windowId)
  {
    if ((windowId % (burstWindows + idleWindows) < burstWindows))
      emitInThisWindow = true;
    else
      emitInThisWindow = false;
    super.beginWindow(windowId);
  }

  public int getBurstWindows()
  {
    return burstWindows;
  }

  public void setBurstWindows(int burstWindows)
  {
    this.burstWindows = burstWindows;
  }

  public int getIdleWindows()
  {
    return idleWindows;
  }

  public void setIdleWindows(int idleWindows)
  {
    this.idleWindows = idleWindows;
  }
}
