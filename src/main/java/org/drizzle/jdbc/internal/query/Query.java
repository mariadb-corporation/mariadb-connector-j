package org.drizzle.jdbc.internal.query;

import java.io.OutputStream;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Feb 20, 2009
 * Time: 10:48:45 PM
 * To change this template use File | Settings | File Templates.
 */
public interface Query {
    int length();

    void writeTo(OutputStream os) throws IOException;
    String getQuery();
}
