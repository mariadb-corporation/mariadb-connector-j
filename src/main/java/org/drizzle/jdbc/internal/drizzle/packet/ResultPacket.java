/*
 * Drizzle JDBC
 *
 * Copyright (C) 2009 Marcus Eriksson (krummas@gmail.com)
 * All rights reserved.
 *
 * Use and distribution licensed under the BSD license.
 */

package org.drizzle.jdbc.internal.drizzle.packet;

/**
 .
 * User: marcuse
 * Date: Jan 16, 2009
 * Time: 4:24:44 PM

 */
public abstract class ResultPacket {

    public abstract ResultType getResultType();

    public abstract byte getPacketSeq();

    public enum ResultType {
        OK,ERROR,EOF,RESULTSET,FIELD
    }
}
