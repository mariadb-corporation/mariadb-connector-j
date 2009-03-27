package org.drizzle.jdbc.internal.query;

import java.io.OutputStream;
import java.io.IOException;

/**
 .
 * User: marcuse
 * Date: Feb 19, 2009
 * Time: 8:29:14 PM

 */
public interface ParameterHolder {
    public void writeTo(OutputStream os) throws IOException;
    public long length();
}
