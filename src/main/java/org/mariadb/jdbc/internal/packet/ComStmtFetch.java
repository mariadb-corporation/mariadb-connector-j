package org.mariadb.jdbc.internal.packet;

import org.mariadb.jdbc.internal.stream.PacketOutputStream;

import java.io.IOException;
import java.io.OutputStream;

public class ComStmtFetch {

    private int statementId;

    public ComStmtFetch(int statementId) {
        this.statementId = statementId;
    }

    /**
     * Send a COM_STMT_FETCH  statement binary stream.
     *
     * @param os        database socket
     * @param fetchSize fetch size
     * @throws IOException if a connection error occur
     */
    public void send(final OutputStream os, int fetchSize) throws IOException {
        PacketOutputStream pos = (PacketOutputStream) os;
        pos.startPacket(0, true);
        pos.buffer.put(Packet.COM_STMT_FETCH);
        pos.buffer.putInt(statementId);
        pos.buffer.putInt(fetchSize);
        pos.finishPacketWithoutRelease(true);
        pos.releaseBuffer();
    }

}
