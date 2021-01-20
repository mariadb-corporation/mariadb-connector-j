package org.mariadb.jdbc.util;

public class MutableInt {
  private byte value = 0;

  public void set(byte value) {
    this.value = value;
  }

  public byte get() {
    return this.value;
  }

  public byte incrementAndGet() {
    return ++value;
  }
}
