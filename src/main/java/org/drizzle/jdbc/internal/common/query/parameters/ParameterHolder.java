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
 * . User: marcuse Date: Feb 19, 2009 Time: 8:29:14 PM
 */
public interface ParameterHolder {
    /**
     * Write at most maxWriteSize, return the amont actually written
     * @param os the stream to write to
     * @param offset where to start writing
     * @param maxWriteSize  max number of bytes to write
     * @return the number of bytes written (either maxWriteSize or the length of the parameter)
     * @throws IOException when everything goes wrong
     */
    int writeTo(OutputStream os, int offset, int maxWriteSize) throws IOException;

    long length() throws IOException;
}
