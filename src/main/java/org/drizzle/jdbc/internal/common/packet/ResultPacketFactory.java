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

package org.drizzle.jdbc.internal.common.packet;

import java.io.IOException;

/**
 * Creates result packets only handles error, ok, eof and result set packets since field and row packets require a
 * previous result set packet User: marcuse Date: Jan 16, 2009 Time: 1:12:23 PM
 */
public class ResultPacketFactory {
    private final static byte ERROR = (byte) 0xff;
    private final static byte OK = (byte) 0x00;
    private final static byte EOF = (byte) 0xfe;

    private ResultPacketFactory() {

    }

    //    private static EOFPacket eof = new EOFPacket();
    public static ResultPacket createResultPacket(final RawPacket rawPacket) throws IOException {
        byte b = rawPacket.getByteBuffer().get(0);
        switch (b) {

            case ERROR:
                return new ErrorPacket(rawPacket);
            case OK:
                return new OKPacket(rawPacket);
            case EOF:
                return new EOFPacket(rawPacket);
            default:
                return new ResultSetPacket(rawPacket);
        }
    }


}
