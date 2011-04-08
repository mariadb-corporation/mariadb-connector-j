/*
 * Drizzle-JDBC
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without modification, are permitted provided that the following
 * conditions are met:
 *
 *  Redistributions of source code must retain the above copyright notice, this list of conditions and the following disclaimer.
 *  Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
 *   disclaimer in the documentation and/or other materials provided with the distribution.
 *  Neither the name of the driver nor the names of its contributors may be used to endorse or promote products derived
 *   from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING,
 * BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO
 * EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR
 * TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package org.drizzle.jdbc.internal.common.query.parameters;

import static org.drizzle.jdbc.internal.common.Utils.needsEscaping;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/**
 * Holds a stream parameter. User: marcuse Date: Feb 19, 2009 Time: 8:53:14 PM
 */
public class StreamParameter implements ParameterHolder {
    /**
     * the length of the parameter to send.
     */
    private final long length;
    /**
     * the actual bytes to send.
     */
    private final byte[] buffer;

    /**
     * Create a new StreamParameter.
     *
     * @param is         the input stream to create the parameter from
     * @param readLength the length to read
     * @throws IOException if we cannot read the stream
     */
    public StreamParameter(final InputStream is, final long readLength)
            throws IOException {
        buffer = new byte[(int) (readLength * 2) + 2];
        int pos = 0;
        buffer[pos++] = '"';
        for (int i = 0; i < readLength; i++) {
            final byte b = (byte) is.read();
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
     *
     * @param os the outputstream to write to
     * @throws IOException if we cannot write to the stream
     */
    public int writeTo(final OutputStream os, int offset, int maxWriteSize) throws IOException {
       int bytesToWrite = (int) Math.min(length - offset, maxWriteSize);
       os.write(buffer, offset, bytesToWrite);
       return bytesToWrite;
   }


    /**
     * Returns the length of the parameter - this is the total amount of bytes that will be sent.
     *
     * @return the length of the parameter
     */
    public final long length() {
        return length;
    }
}
