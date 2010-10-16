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
    private final byte[][] queryPartsArray;

    public DrizzleParameterizedQuery(final String query) {
        this.query = query;
        List<String> queryParts = createQueryParts(query);
        queryPartsArray = new byte[queryParts.size()][];
        for(int i=0;i < queryParts.size(); i++) {
            queryPartsArray[i] = queryParts.get(i).getBytes();
        }
        paramCount = queryParts.size() - 1;
        parameters = new ParameterHolder[paramCount];
    }

    public DrizzleParameterizedQuery(final ParameterizedQuery paramQuery) {
        this.query = paramQuery.getQuery();
        this.queryPartsArray = paramQuery.getQueryPartsArray();
        paramCount = queryPartsArray.length - 1;
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
        for(byte[] s : queryPartsArray) {
            length += s.length;
        }

        for(ParameterHolder ph : parameters) {
            try {
                length += ph.length();
            } catch (IOException e) {
                throw new QueryException("Could not calculate length of parameter: "+e.getMessage());
            }
        }
        return length;
    }

    public void writeTo(final OutputStream os) throws IOException, QueryException {
        if(containsNull(parameters)) {
            throw new QueryException("You need to set exactly " + paramCount + " parameters on the prepared statement");
        }
        if(queryPartsArray.length == 0) {
            throw new AssertionError("Invalid query, queryParts was empty");
        }
        os.write(queryPartsArray[0]);
        for(int i = 1; i<queryPartsArray.length; i++) {
            parameters[i-1].writeTo(os);
            if(queryPartsArray[i].length != 0)
                os.write(queryPartsArray[i]);
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

    public byte[][] getQueryPartsArray() {
        return queryPartsArray;
    }

    public QueryType getQueryType() {
        return QueryType.classifyQuery(query);
    }

    public int getParamCount() {
        return paramCount;
    }

    @Override
    public void writeTo(OutputStream ostream, int offset, int packLength)
            throws IOException
    {
    }

}
