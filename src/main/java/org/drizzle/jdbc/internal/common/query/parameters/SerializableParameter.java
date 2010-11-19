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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.util.Arrays;

/**
 * User: marcuse Date: Feb 19, 2009 Time: 8:53:14 PM
 */
public class SerializableParameter implements ParameterHolder {
    private final byte[] rawBytes;
    private final int length;

    public SerializableParameter(final Object object) throws IOException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(object);
        rawBytes = escapeBytes(baos.toByteArray());
        length = rawBytes.length;
    }

    public int writeTo(final OutputStream os, int offset, int maxWriteSize) throws IOException {
       int bytesToWrite = (int) Math.min(length - offset, maxWriteSize);
       os.write(rawBytes, offset, bytesToWrite);
       return bytesToWrite;
   }


    public long length() {
        return length;
    }

    private static byte[] escapeBytes(final byte[] input) {
        final byte[] buffer = new byte[input.length * 2 + 2];
        int i = 0;
        buffer[i++] = '\"';
        for (final byte b : input) {
            if (Utils.needsEscaping(b)) {
                buffer[i++] = '\\';
            }
            buffer[i++] = b;
        }
        buffer[i++] = '\"';
        return Arrays.copyOf(buffer, i);
    }
}