/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.query;

/**
 * .
 * User: marcuse
 * Date: Mar 18, 2009
 * Time: 10:11:54 PM
 */
public class IllegalParameterException extends Exception {
    public IllegalParameterException(String s) {
        super(s);
    }
}
