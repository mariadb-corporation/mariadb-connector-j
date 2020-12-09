package org.mariadb.jdbc.util;

public class MutableInt {
  private int value;

  public MutableInt(int value) {
    this.value = value;
  }

  public void set(int value) {
    this.value = value;
  }

  public int get() {
    return this.value;
  }

  public int incrementAndGet() {
    return ++value;
  }
}
