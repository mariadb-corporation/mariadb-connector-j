package org.drizzle.jdbc.internal.query;

import java.io.OutputStream;
import java.io.IOException;

/**
 .
 * User: marcuse
 * Date: Feb 20, 2009
 * Time: 10:48:45 PM

 */
public interface Query {
    int length();

    void writeTo(OutputStream os) throws IOException;
    String getQuery();
}
