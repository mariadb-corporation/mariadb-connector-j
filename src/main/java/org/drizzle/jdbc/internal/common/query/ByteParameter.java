/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.query;

import java.io.OutputStream;
import java.io.IOException;

/**
 .
 * User: marcuse
 * Date: Feb 27, 2009
 * Time: 9:56:17 PM

 */
public class ByteParameter implements ParameterHolder {
    private final byte[] bytes;

    public ByteParameter(byte[] x) {
        this.bytes=x;
    }


    public void writeTo(OutputStream os) throws IOException {
        for (byte aByte : bytes) {
            os.write(aByte);
        }
    }

    public long length() {
        return bytes.length;
    }
}
