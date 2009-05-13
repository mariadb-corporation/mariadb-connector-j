/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.query;

import org.drizzle.jdbc.internal.common.query.parameters.ParameterHolder;

/**
 .
 * User: marcuse
 * Date: Mar 18, 2009
 * Time: 10:07:57 PM

 */
public interface ParameterizedQuery extends Query {

    int getParamCount();
    void clearParameters();
    void setParameter(int position, ParameterHolder parameter) throws IllegalParameterException;
}
