package org.drizzle.jdbc.internal.query;

import java.io.OutputStream;
import java.io.IOException;

/**
 * Created by IntelliJ IDEA.
 * User: marcuse
 * Date: Feb 19, 2009
 * Time: 8:29:14 PM
 * To change this template use File | Settings | File Templates.
 */
public interface ParameterHolder {
//    public byte read();
    public void writeTo(OutputStream os) throws IOException;
    public long length();
}
