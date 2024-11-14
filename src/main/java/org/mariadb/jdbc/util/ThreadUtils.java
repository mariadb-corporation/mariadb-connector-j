// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.util;

import java.util.concurrent.Callable;
import javax.security.auth.Subject;

public class ThreadUtils {
  @SuppressWarnings("deprecation")
  public static long getId(Thread thread) {
    // must be return thread.threadId() for java 19+,
    // but since we support java 8, cannot be removed for now
    return thread.getId();
  }

  @SuppressWarnings("deprecation")
  public static void callAs(
      final Subject subject, final Callable<java.security.PrivilegedExceptionAction<Void>> action)
      throws Exception {
    Subject.doAs(subject, action.call());
    // must be for java 18+, but since we support java 8, cannot be removed for now
    // Subject.callAs(subject, action);
  }
}
