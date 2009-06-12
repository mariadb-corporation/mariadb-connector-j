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
 * Date: Jun 9, 2009
 * Time: 4:22:28 PM
 * To change this template use File | Settings | File Templates.
 */
public interface DataType {
    public Class getJavaType();
    public int getSqlType();
}
