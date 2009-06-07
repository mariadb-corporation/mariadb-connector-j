/*
 * Drizzle JDBC
 *
 *  Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 *  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common;

/**
 * Serves out new parameterized batch handlers
 */
public interface ParameterizedBatchHandlerFactory {
    /**
     * returns a parameterized batch handler
     * @return a parameterized batch handler
     */
    ParameterizedBatchHandler get();
}
