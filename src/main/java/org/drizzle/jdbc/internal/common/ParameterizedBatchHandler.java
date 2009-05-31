/*
 * Drizzle JDBC
 *
 *  Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 *  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common;

import org.drizzle.jdbc.internal.common.query.ParameterizedQuery;

/**
 * Interface that defines a parameterized batch handler.
 * Implement this interface and set it as a parameterized batch handler on the connection like this:
 * <code>
 * if(connection.isWrapperFor(DrizzleConnection.class)) {
 *    DrizzleConnection dc = connection.unwrap(DrizzleConnection.class);
 *    dc.setBatchQueryHandler(VerrrryFastBatchHandler.class);
 * }
 * Note: implementations currently need a default no-args constructor.
 *</code>
 */
public interface ParameterizedBatchHandler {
    /**
     * called when a set of parameters are added to a batch.
     * @param query the parameterized query.
     */
    void addToBatch(ParameterizedQuery query);

    /**
     * execute the batch using protocol. Return an array of update counts
     * or -2 (Statement.SUCCESS_NO_INFO) if the update count is unknown.
     * @param protocol the protocol to use.
     * @return a list of update counts
     * @throws QueryException if something goes wrong executing the query.
     */
    int [] executeBatch(Protocol protocol) throws QueryException;
}