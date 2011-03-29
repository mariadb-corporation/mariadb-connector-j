/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.query;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;

/**
 * . User: marcuse Date: Feb 20, 2009 Time: 10:43:58 PM
 */
public class DrizzleQuery implements Query {

    private final String query;
    private final byte[] queryToSend;

    public DrizzleQuery(final String query) {
        this.query = query;
        try {
            queryToSend = query.getBytes("UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unsupported encoding: " + e.getMessage(), e);
        }
    }

    public DrizzleQuery(final byte[] query) {
        queryToSend = query;
        try {
            this.query = new String(query, "UTF-8");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException("Unsupported encoding: " + e.getMessage(), e);
        }
    }

    public int length() {
        return queryToSend.length;
    }

    public void writeTo(final OutputStream os) throws IOException {
        os.write(queryToSend, 0, queryToSend.length);
    }

    public String getQuery() {
        return query;
    }

    public QueryType getQueryType() {
        return QueryType.classifyQuery(query);
    }

    @Override
    public boolean equals(final Object otherObj) {
        return otherObj instanceof DrizzleQuery && (((DrizzleQuery) otherObj).query).equals(query);
    }

    public void writeTo(OutputStream ostream, int offset, int packLength) throws IOException
    {
        ostream.write(queryToSend, offset, packLength);
    }


}
