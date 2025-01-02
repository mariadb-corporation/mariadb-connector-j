// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.client.util;

import java.util.concurrent.locks.ReentrantLock;

/** Utility class to permit use AutoClosable, ensuring proper lock release. */
public final class ClosableLock extends ReentrantLock implements AutoCloseable {
  private static final long serialVersionUID = -8041187539350329669L;

  /**
   * Default constructor, retaining lock.
   *
   * @return Closable lock
   */
  public ClosableLock closeableLock() {
    this.lock();
    return this;
  }

  @Override
  public void close() {
    this.unlock();
  }
}
