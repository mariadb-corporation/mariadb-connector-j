package org.drizzle.jdbc.internal.query.drizzle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.drizzle.jdbc.internal.query.Query;

import java.io.OutputStream;
import java.io.IOException;
import java.io.StringReader;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Feb 20, 2009
 * Time: 10:43:58 PM
 * To change this template use File | Settings | File Templates.
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
        StringReader strReader = new StringReader(query);
        int ch;
        while((ch=strReader.read())!=-1) {
            os.write(ch);
        }
    }

    public String getQuery() {
        return query;
    }

}
