/*
 * Drizzle JDBC
 *
 *  Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 *  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.
 */

package org.mariadb.jdbc;

import org.mariadb.jdbc.internal.common.ParameterizedBatchHandlerFactory;
import org.mariadb.jdbc.internal.common.ParameterizedBatchHandler;
import org.mariadb.jdbc.internal.common.Protocol;
import org.mariadb.jdbc.internal.common.QueryException;
import org.mariadb.jdbc.internal.common.query.ParameterizedQuery;


public class NoopBatchHandlerFactory implements ParameterizedBatchHandlerFactory {
    public ParameterizedBatchHandler get(final String sql, final Protocol protocol) {
        return new TestNoopBatchHandler();
    }


   public class TestNoopBatchHandler implements ParameterizedBatchHandler {

        public void addToBatch(ParameterizedQuery query) {
        }

        public int[] executeBatch() throws QueryException {
            return new int[0];
        }
    }
}
