package org.drizzle.jdbc.internal.packet;

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
