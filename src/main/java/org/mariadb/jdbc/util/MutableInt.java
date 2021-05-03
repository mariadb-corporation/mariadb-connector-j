// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.util;

public class MutableInt {
  private byte value = -1;

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
