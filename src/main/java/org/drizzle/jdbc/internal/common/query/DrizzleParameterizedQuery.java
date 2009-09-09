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
import static org.drizzle.jdbc.internal.common.Utils.countChars;
import org.drizzle.jdbc.internal.common.query.parameters.ParameterHolder;

import java.io.IOException;
import java.io.OutputStream;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * . User: marcuse Date: Feb 18, 2009 Time: 10:13:42 PM
 */
public class DrizzleParameterizedQuery implements ParameterizedQuery {
    private final static Logger log = Logger.getLogger(DrizzleParameterizedQuery.class.getName());
    private final Map<Integer, ParameterHolder> parameters;
    private final int paramCount;
    //    private final String query;
    private final String query;

    public DrizzleParameterizedQuery(final String query) {
        this.query = query;
        this.paramCount = countChars(this.query, '?');
        parameters = new HashMap<Integer, ParameterHolder>(this.paramCount);
    }

    public DrizzleParameterizedQuery(final ParameterizedQuery query) {
        this.paramCount = query.getParamCount();
        this.query = query.getQuery();
        parameters = new HashMap<Integer, ParameterHolder>(this.paramCount);
    }

    public void setParameter(final int position, final ParameterHolder parameter) throws IllegalParameterException {
        if (position >= 0 && position < paramCount) {
            parameters.put(position, parameter);
        } else {
            throw new IllegalParameterException("No '?' on that position");
        }
    }

    public Map<Integer, ParameterHolder> getParameters() {
        // TODO: defensive copy perhaps?
        return parameters;
    }

    public void clearParameters() {
        this.parameters.clear();
    }

    public int length() {
        int length = query.length() - paramCount; // remove the ?s

        for (final Map.Entry<Integer, ParameterHolder> param : parameters.entrySet()) {
            length += param.getValue().length();
        }

        return length;
    }

    public void writeTo(final OutputStream os) throws IOException, QueryException {
        if (paramCount != this.parameters.size()) {
            throw new QueryException("You need to set exactly " + paramCount + " parameters on the prepared statement");
        }
        final StringReader strReader = new StringReader(query);
        int ch;
        int paramCounter = 0;
        boolean isWithinQuotes = false;
        boolean isWithinDoubleQuotes = false;
        while ((ch = strReader.read()) != -1) {
            if (ch == '"' && !isWithinQuotes && !isWithinDoubleQuotes) {
                isWithinDoubleQuotes = true;
            } else if (ch == '"' && !isWithinQuotes) {
                isWithinDoubleQuotes = false;
            }

            if (ch == '\'' && !isWithinQuotes && !isWithinDoubleQuotes) {
                isWithinQuotes = true;
            } else if (ch == '\'' && !isWithinDoubleQuotes) {
                isWithinQuotes = false;
            }


            if (ch == '?' && !isWithinDoubleQuotes && !isWithinQuotes) {
                parameters.get(paramCounter++).writeTo(os);
            } else {
                os.write(ch);
            }
        }
    }

    public String getQuery() {
        return query;
    }

    public QueryType getQueryType() {
        return QueryType.classifyQuery(query);
    }

    public int getParamCount() {
        return paramCount;
    }

}
