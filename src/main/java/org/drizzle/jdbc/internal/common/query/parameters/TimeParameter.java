/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.query.parameters;

import org.drizzle.jdbc.internal.common.Utils;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Since drizzle has no time datatype, jdbc time is stored in a packed integer
 *
 * @see Utils#packTime(long)
 */
public class TimeParameter implements ParameterHolder {
    private final byte[] byteRepresentation;

    public TimeParameter(final long timestamp) {
        final int packedTime = Utils.packTime(timestamp);
        byteRepresentation = String.valueOf(packedTime).getBytes();
    }

    public void writeTo(final OutputStream os) throws IOException {
        os.write(byteRepresentation);
    }

    public long length() {
        return byteRepresentation.length;
    }
}