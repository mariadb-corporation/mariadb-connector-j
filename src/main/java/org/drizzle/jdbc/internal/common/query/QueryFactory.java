/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.query;

/**
 * . User: marcuse Date: Mar 18, 2009 Time: 10:06:11 PM
 */
public interface QueryFactory {
    Query createQuery(String query);
    Query createQuery(byte[] query);
    
    ParameterizedQuery createParameterizedQuery(String query);

    ParameterizedQuery createParameterizedQuery(ParameterizedQuery dQuery);
}
