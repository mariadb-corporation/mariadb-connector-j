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
import java.util.logging.Logger;

/**
 * .
 * User: marcuse
 * Date: Feb 20, 2009
 * Time: 10:43:58 PM
 */
public class DrizzleQuery implements Query {
    private final static Logger log = Logger.getLogger(DrizzleQuery.class.getName());
    protected final String query;

    public DrizzleQuery(String query) {
        this.query = query;
    }

    public int length() {
        return query.length();
    }

    public void writeTo(OutputStream os) throws IOException {
        byte[] b = query.getBytes();
        os.write(b, 0, length());
    }

    public String getQuery() {
        return query;
    }

}
