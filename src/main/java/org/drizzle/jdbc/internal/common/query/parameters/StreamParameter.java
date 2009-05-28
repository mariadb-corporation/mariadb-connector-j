/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */
package org.drizzle.jdbc.internal.common.query.parameters;

import static org.drizzle.jdbc.internal.common.Utils.needsEscaping;

import java.io.InputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Holds a stream parameter.
 * User: marcuse
 * Date: Feb 19, 2009
 * Time: 8:53:14 PM
 */
public class StreamParameter implements ParameterHolder {
    /**
     * the length of the parameter to send.
     */
    private final long length;
    /**
     * the actual bytes to send.
     */
    private final byte [] buffer;

    /**
     * Create a new StreamParameter.
     * @param is the input stream to create the parameter from
     * @param readLength the length to read
     * @throws IOException if we cannot read the stream
     */
    public StreamParameter(final InputStream is, final long readLength)
            throws IOException {
        buffer = new byte[(int) (readLength * 2) + 2];
        int pos = 0;
        buffer[pos++] = '"';
        for (int i = 0; i < readLength; i++) {
            byte b = (byte) is.read();
            if (needsEscaping(b)) {
                buffer[pos++] = '\\';
            }
            buffer[pos++] = b;
        }
        buffer[pos++] = '"';
        this.length = pos;
    }

    /**
     * Writes the parameter to an outputstream.
     * @param os the outputstream to write to
     * @throws IOException if we cannot write to the stream
     */
    @Override
    public final void writeTo(final OutputStream os) throws IOException {
        os.write(buffer, 0, (int) length);
    }

    /**
     * Returns the length of the parameter - this is the total
     * amount of bytes that will be sent.
     * @return the length of the parameter
     */
    @Override
    public final long length() {
        return length;
    }
}
