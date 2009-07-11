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
     * returns a parameterized batch handler. Called every
     * time prepareStatement is called on the Connection.
     * @param query the query to create the handler for
     * @return a parameterized batch handler
     */
    ParameterizedBatchHandler get(String query, Protocol protocol);
}
