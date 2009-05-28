/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.query.parameters;

import java.io.IOException;
import java.io.OutputStream;

/**
 * .
 * User: marcuse
 * Date: Feb 19, 2009
 * Time: 8:50:52 PM
 */
public class LongParameter implements ParameterHolder {
    private final byte[] byteRepresentation;
    private byte bytePointer = 0;

    public LongParameter(long theLong) {
        byteRepresentation = String.valueOf(theLong).getBytes();
    }

    public void writeTo(OutputStream os) throws IOException {
        for (byte b : byteRepresentation)
            os.write(b);
    }

    public long length() {
        return byteRepresentation.length;
    }
}
