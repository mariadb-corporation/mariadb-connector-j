/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.drizzle.packet;

import java.io.InputStream;
import java.io.IOException;

/**
 * User: marcuse
 * Date: Apr 1, 2009
 * Time: 7:11:40 AM
 */
public class ReadAheadInputStream extends InputStream {
    private final static int BUFFER_SIZE=4096;
    private final static byte[] buffer = new byte[BUFFER_SIZE];
    private int bufferPosition = 0;
    private int endOfBuffer = 0;

    private final InputStream inputStream;

    public ReadAheadInputStream(InputStream is) {
        this.inputStream = is;
    }

    public int read() throws IOException {
        if(bufferPosition >= endOfBuffer)
            fillBuffer();
        return buffer[bufferPosition++];
    }

    private void fillBuffer() throws IOException {
        bufferPosition = 0;
        endOfBuffer = 0;
        int available = inputStream.available();
        if(available > 0) {
            int readBytes = inputStream.read(buffer,0,Math.min(available,BUFFER_SIZE));
            endOfBuffer+=readBytes;
        } else {
            buffer[endOfBuffer++] = (byte) inputStream.read();
        }
    }
    @Override
    public void close() throws IOException {
        this.inputStream.close();
    }
}
