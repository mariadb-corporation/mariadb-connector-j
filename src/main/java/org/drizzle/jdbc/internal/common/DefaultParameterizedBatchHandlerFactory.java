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
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Jun 7, 2009
 * Time: 12:36:12 PM
 * To change this template use File | Settings | File Templates.
 */
final public class DefaultParameterizedBatchHandlerFactory implements ParameterizedBatchHandlerFactory {

    /**
     * Creates a parameterized batch handler
     *
     * In this implementation the query is ignored, taken in the addParameters call instead
     *
     * @param query the query to create the handler for
     * @return
     */
    public ParameterizedBatchHandler get(String query) {
        return new DefaultParameterizedBatchHandler();
    }
}
