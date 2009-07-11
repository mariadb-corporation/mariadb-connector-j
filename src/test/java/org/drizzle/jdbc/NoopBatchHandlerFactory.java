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

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jun 7, 2009
 * Time: 12:39:30 PM
 * To change this template use File | Settings | File Templates.
 */
public class NoopBatchHandlerFactory implements ParameterizedBatchHandlerFactory {
    public ParameterizedBatchHandler get(String sql, Protocol protocol) {
        return new TestNoopBatchHandler();
    }
}
