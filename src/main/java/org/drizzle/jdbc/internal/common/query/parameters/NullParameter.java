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
 * . User: marcuse Date: Feb 19, 2009 Time: 9:35:15 PM
 */
public class NullParameter implements ParameterHolder {
    private final byte[] byteRepresentation;

    public NullParameter() {
        this.byteRepresentation = "NULL".getBytes();
    }

    public void writeTo(final OutputStream os) throws IOException {
        os.write(byteRepresentation);
    }

    public long length() {
        return byteRepresentation.length;
    }
}
