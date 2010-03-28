/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.query;

import org.drizzle.jdbc.internal.common.QueryException;
import static org.drizzle.jdbc.internal.common.Utils.createQueryParts;

import org.drizzle.jdbc.internal.common.query.parameters.ParameterHolder;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;
import java.util.logging.Logger;

/**
 * . User: marcuse Date: Feb 18, 2009 Time: 10:13:42 PM
 */
public class DrizzleParameterizedQuery implements ParameterizedQuery {
    private final static Logger log = Logger.getLogger(DrizzleParameterizedQuery.class.getName());
    private ParameterHolder[] parameters;
    private final int paramCount;
    private final String query;
    private List<String> queryParts;
    public DrizzleParameterizedQuery(final String query) {
        this.query = query;
        queryParts = createQueryParts(query);
        paramCount = queryParts.size() - 1;
        parameters = new ParameterHolder[paramCount];
    }

    public DrizzleParameterizedQuery(final ParameterizedQuery query) {
        this.query = query.getQuery();
        this.queryParts = query.getQueryParts();
        paramCount = queryParts.size() - 1;
        parameters = new ParameterHolder[paramCount];
    }

    public void setParameter(final int position, final ParameterHolder parameter) throws IllegalParameterException {
        if (position >= 0 && position < paramCount) {
            parameters[position] = parameter;
        } else {
            throw new IllegalParameterException("No '?' on that position");
        }
    }

    public ParameterHolder[] getParameters() {
        return parameters;
    }

    public void clearParameters() {
        this.parameters = new ParameterHolder[paramCount];
    }

    public int length() throws QueryException {
        if(containsNull(parameters)) {
            throw new QueryException("You need to set exactly " + paramCount + " parameters on the prepared statement");
        }
        int length = 0;
        for(String s : queryParts) {
            length += s.length();
        }

        for(ParameterHolder ph : parameters) {
            length += ph.length();
        }
        return length;
    }

    public void writeTo(final OutputStream os) throws IOException, QueryException {
        if(containsNull(parameters)) {
            throw new QueryException("You need to set exactly " + paramCount + " parameters on the prepared statement");
        }
        if(queryParts.size() == 0) {
            throw new AssertionError("Invalid query, queryParts was empty");
        }
        os.write(queryParts.get(0).getBytes());
        for(int i = 1; i<queryParts.size(); i++) {
            parameters[i-1].writeTo(os);
            String queryPart = queryParts.get(i);
            os.write(queryPart.getBytes());            
        }
    }

    private boolean containsNull(ParameterHolder[] parameters) {
        for(ParameterHolder ph : parameters) {
            if(ph == null) {
                return true;
            }
        }
        return false;
    }

    public String getQuery() {
        return query;
    }

    public List<String> getQueryParts() {
        return queryParts;
    }

    public QueryType getQueryType() {
        return QueryType.classifyQuery(query);
    }

    public int getParamCount() {
        return paramCount;
    }

}
