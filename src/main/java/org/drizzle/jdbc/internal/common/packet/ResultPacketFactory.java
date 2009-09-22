/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
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
        switch (rawPacket.getByteBuffer().get(0)) {
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
