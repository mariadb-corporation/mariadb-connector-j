/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.common.packet.buffer;

import org.drizzle.jdbc.internal.common.packet.RawPacket;

import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.nio.ByteBuffer;

/**
 * . User: marcuse Date: Jan 16, 2009 Time: 8:27:38 PM
 */
public final class ReadUtil {
    private ReadUtil() {

    }

    /**
     * Read a number of bytes from the stream and store it in the buffer, and fix the problem with "incomplete" reads by
     * doing another read if we don't have all of the data yet.
     *
     * @param inputStream the input stream to read from
     * @param buffer      where to store the data
     * @return the number of bytes read (should be == length if we didn't hit EOF)
     * @throws java.io.IOException if an error occurs while reading the stream
     */
    public static int safeRead(final InputStream inputStream, final byte[] buffer) throws IOException {
        int offset = 0;
        int left = buffer.length;
        do {
            try {
                final int nr = inputStream.read(buffer, offset, left);
                if (nr == -1) {
                    return nr;
                }
                offset += nr;
                left -= nr;
            } catch (InterruptedIOException exp) {
                /* Ignore, just retry */
            }
        } while (left > 0);

        return buffer.length;
    }

    /**
     * Checks whether the next packet is EOF. 
     * @param rawPacket the raw packet
     * @return true if the packet is an EOF packet
     */
    public static boolean eofIsNext(final RawPacket rawPacket) {
        final ByteBuffer buf = rawPacket.getByteBuffer();
        return (buf.get(0) == (byte) 0xfe && buf.capacity() < 9);

    }
}