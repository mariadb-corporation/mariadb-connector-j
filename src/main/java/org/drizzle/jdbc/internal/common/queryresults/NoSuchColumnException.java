/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.queryresults;

/**
 * Created by IntelliJ IDEA. User: marcuse Date: Mar 27, 2009 Time: 9:41:53 AM To change this template use File |
 * Settings | File Templates.
 */
public class NoSuchColumnException extends Exception {
    public NoSuchColumnException(final String reason) {
        super(reason);
    }
}
