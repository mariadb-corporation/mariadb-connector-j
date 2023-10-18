//  SPDX-License-Identifier: LGPL-2.1-or-later
//  Copyright (c) 2012-2014 Monty Program Ab
//  Copyright (c) 2023 MariaDB Corporation Ab

package org.mariadb.jdbc.util;
import java.security.PrivilegedExceptionAction;
import java.util.concurrent.Callable;
import javax.security.auth.Subject;

public class ThreadUtils {
    public static long getId(Thread thread) {
        return thread.threadId();
    }

    public static void callAs(final Subject subject,
                              final Callable<PrivilegedExceptionAction<Void>> action)
            throws Exception {
        Subject.callAs(subject, action);
    }

}
