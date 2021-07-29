// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.pool;

import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class PoolThreadFactory implements java.util.concurrent.ThreadFactory {

  // start from DefaultThread factory to get security groups and what not
  private final java.util.concurrent.ThreadFactory parentFactory = Executors.defaultThreadFactory();
  private final AtomicInteger threadId = new AtomicInteger();
  private final String threadName;

  public PoolThreadFactory(String threadName) {
    this.threadName = threadName;
  }

  @Override
  public Thread newThread(Runnable runnable) {
    Thread result = parentFactory.newThread(runnable);
    result.setName(threadName + "-" + threadId.incrementAndGet());
    result.setDaemon(true); // set as daemon so that mariaDb won't hold up shutdown

    return result;
  }
}
