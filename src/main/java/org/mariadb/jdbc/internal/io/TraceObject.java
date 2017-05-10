package org.mariadb.jdbc.internal.io;

public class TraceObject {

    public static final int NOT_COMPRESSED = 0;
    public static final int COMPRESSED_PROTOCOL_COMPRESSED_PACKET = 1;
    public static final int COMPRESSED_PROTOCOL_NOT_COMPRESSED_PACKET = 2;

    private boolean send;
    private int indicatorFlag;
    private byte[][] buf;

    /**
     * Permit to store MySQL packets.
     *
     * @param send          was packet send or received
     * @param indicatorFlag indicator. can be NOT_COMPRESSED, COMPRESSED_PROTOCOL_COMPRESSED_PACKET or
     *                      COMPRESSED_PROTOCOL_NOT_COMPRESSED_PACKET
     * @param buf           buffers
     */
    public TraceObject(boolean send, int indicatorFlag, byte[]... buf) {
        this.send = send;
        this.indicatorFlag = indicatorFlag;
        this.buf = buf;
    }

    public void remove() {
        for (byte[] b : buf) b = null;
        buf = null;
    }

    public boolean isSend() {
        return send;
    }

    public int getIndicatorFlag() {
        return indicatorFlag;
    }

    public byte[][] getBuf() {
        return buf;
    }
}
