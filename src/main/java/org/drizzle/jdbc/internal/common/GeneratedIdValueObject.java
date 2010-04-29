/*
 * Drizzle JDBC
 *
 *  Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 *  All rights reserved.
 *
 *  Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common;

import org.drizzle.jdbc.internal.mysql.MySQLType;

import java.text.ParseException;

/**
 * Created by IntelliJ IDEA. User: marcuse Date: Jun 12, 2009 Time: 4:16:09 PM To change this template use File |
 * Settings | File Templates.
 */
public class GeneratedIdValueObject extends AbstractValueObject {
    public GeneratedIdValueObject(final long insertId) {
        super(String.valueOf(insertId).getBytes(), new DataType() {
            public Class getJavaType() {
                return Long.class;
            }

            public int getSqlType() {
                return java.sql.Types.INTEGER;
            }

            public String getTypeName() {
                return "INTEGER";
            }

            public MySQLType.Type getType() {
                return MySQLType.Type.LONG;
            }
        });
    }

    public Object getObject() throws ParseException {
        return getLong();
    }
}
