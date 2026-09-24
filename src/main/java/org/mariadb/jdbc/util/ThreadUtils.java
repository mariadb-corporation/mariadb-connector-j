// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.util;

import java.util.concurrent.Callable;
import javax.security.auth.Subject;

public class ThreadUtils {
  @SuppressWarnings("deprecation")
  public static long getId(Thread thread) {
    // Thread.threadId() needs Java 19: getId() stays while Java 17 is the minimum
    return thread.getId();
  }

  @SuppressWarnings({"deprecation", "removal"})
  public static void callAs(
      final Subject subject, final Callable<java.security.PrivilegedExceptionAction<Void>> action)
      throws Exception {
    Subject.doAs(subject, action.call());
    // Subject.callAs(subject, action) needs Java 18: doAs stays while Java 17 is the minimum
  }
}
