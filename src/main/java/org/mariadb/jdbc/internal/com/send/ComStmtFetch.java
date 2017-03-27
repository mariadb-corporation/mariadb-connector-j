package org.mariadb.jdbc.internal.com.send;

import org.mariadb.jdbc.internal.com.Packet;
import org.mariadb.jdbc.internal.io.output.PacketOutputStream;

import java.io.IOException;

public class ComStmtFetch {

    private int statementId;

    public ComStmtFetch(int statementId) {
        this.statementId = statementId;
    }

    /**
     * Send a COM_STMT_FETCH  statement binary stream.
     *
     * @param pos        database socket
     * @param fetchSize fetch size
     * @throws IOException if a connection error occur
     */
    public void send(final PacketOutputStream pos, int fetchSize) throws IOException {
        pos.startPacket(0);
        pos.write(Packet.COM_STMT_FETCH);
        pos.writeInt(statementId);
        pos.writeInt(fetchSize);
        pos.flush();
    }

}
