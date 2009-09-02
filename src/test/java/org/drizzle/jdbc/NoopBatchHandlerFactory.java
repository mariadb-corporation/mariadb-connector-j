/*
 * Drizzle JDBC
 *
 *  Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 *  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc;

import org.drizzle.jdbc.internal.common.ParameterizedBatchHandlerFactory;
import org.drizzle.jdbc.internal.common.ParameterizedBatchHandler;
import org.drizzle.jdbc.internal.common.Protocol;
import org.drizzle.jdbc.internal.common.QueryException;
import org.drizzle.jdbc.internal.common.query.ParameterizedQuery;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jun 7, 2009
 * Time: 12:39:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class NoopBatchHandlerFactory implements ParameterizedBatchHandlerFactory {
    public ParameterizedBatchHandler get(final String sql, final Protocol protocol) {
        return new TestNoopBatchHandler();
    }


   public class TestNoopBatchHandler implements ParameterizedBatchHandler {

        public void addToBatch(ParameterizedQuery query) {
            //To change body of implemented methods use File | Settings | File Templates.
        }

        public int[] executeBatch() throws QueryException {
            return new int[0];  //To change body of implemented methods use File | Settings | File Templates.
        }
    }
}
