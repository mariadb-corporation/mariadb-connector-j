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
public class DefaultParameterizedBatchHandlerFactory implements ParameterizedBatchHandlerFactory {
    public ParameterizedBatchHandler get() {
        return new DefaultParameterizedBatchHandler();
    }
}
