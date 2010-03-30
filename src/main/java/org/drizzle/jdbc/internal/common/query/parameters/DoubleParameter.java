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
    private final byte[] rawBytes;

    public DoubleParameter(final double x) {
        rawBytes = String.valueOf(x).getBytes();
    }

    public void writeTo(final OutputStream os) throws IOException {
        os.write(rawBytes);
    }

    public long length() {
        return rawBytes.length;
    }
}
