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
 * . User: marcuse Date: Mar 18, 2009 Time: 10:07:57 PM
 */
public interface ParameterizedQuery extends Query {
    /**
     * get the number of parameters in this query.
     *
     * @return number of parameters
     */
    int getParamCount();

    /**
     * clears the parameters.
     */
    void clearParameters();

    /**
     * Sets a parameter at a position. The positions start at 0.
     *
     * @param position  the position to set it at
     * @param parameter the parameter to set
     * @throws IllegalParameterException if, for example, the position is out of bounds
     */
    void setParameter(int position, ParameterHolder parameter)
            throws IllegalParameterException;

    ParameterHolder[] getParameters();

    String getQuery();

    byte[][] getQueryPartsArray();
}
