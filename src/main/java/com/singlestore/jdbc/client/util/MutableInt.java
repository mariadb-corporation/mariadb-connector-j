// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.client.util;

/** Mutable int, permitting to update packet sequence */
public class MutableInt {
  private byte value = -1;

  /**
   * Set new sequence value
   *
   * @param value new value
   */
  public void set(byte value) {
    this.value = value;
  }

  /**
   * Get current sequence value
   *
   * @return value
   */
  public byte get() {
    return this.value;
  }

  /**
   * Increment sequence and get new value
   *
   * @return new value
   */
  public byte incrementAndGet() {
    return ++value;
  }
}
