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
 * . User: marcuse Date: Feb 27, 2009 Time: 10:00:38 PM
 */
public class DoubleParameter implements ParameterHolder {
    private final byte[] byteRepresentation;

    public DoubleParameter(final double x) {
        byteRepresentation = String.valueOf(x).getBytes();
    }

    public int writeTo(final OutputStream os, int offset, int maxWriteSize) throws IOException {
        int bytesToWrite = Math.min(byteRepresentation.length - offset, maxWriteSize);
        os.write(byteRepresentation, offset, bytesToWrite);
        return bytesToWrite;
    }

    public long length() {
        return byteRepresentation.length;
    }
}
