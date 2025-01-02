// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.client.util;

/** Mutable int */
public class MutableInt {
  private int value;

  public MutableInt() {
    this.value = -1;
  }

  public MutableInt(int value) {
    this.value = value;
  }

  /**
   * Set new sequence value
   *
   * @param value new value
   */
  public void set(int value) {
    this.value = value;
  }

  /**
   * Get current sequence value
   *
   * @return value
   */
  public int get() {
    return this.value;
  }

  /**
   * Increment sequence and get new value
   *
   * @return new value
   */
  public int incrementAndGet() {
    return ++value;
  }
}
