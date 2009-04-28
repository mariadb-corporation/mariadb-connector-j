/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.query.drizzle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.drizzle.jdbc.internal.common.query.Query;

import java.io.OutputStream;
import java.io.IOException;

/**
 .
 * User: marcuse
 * Date: Feb 20, 2009
 * Time: 10:43:58 PM

 */
public class DrizzleQuery implements Query {
    private final static Logger log = LoggerFactory.getLogger(DrizzleQuery.class);
    protected final String query;
    public DrizzleQuery(String query) {
        this.query=query;
        log.info("Creating query: "+query);
    }

    public int length() {
        return query.length();
    }

    public void writeTo(OutputStream os) throws IOException {
        log.info("writing query to outputstream");
/*        StringReader strReader = new StringReader(query);
        int ch;*/
        byte [] b = query.getBytes();
        os.write(b,0,length());
        /*while((ch=strReader.read())!=-1) {
            os.write(ch);
        }*/
    }

    public String getQuery() {
        return query;
    }

}
