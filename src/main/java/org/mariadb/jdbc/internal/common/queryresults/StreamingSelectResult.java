package org.mariadb.jdbc.internal.common.queryresults;

import org.mariadb.jdbc.internal.common.PacketFetcher;
import org.mariadb.jdbc.internal.common.QueryException;
import org.mariadb.jdbc.internal.common.ServerStatus;
import org.mariadb.jdbc.internal.common.ValueObject;
import org.mariadb.jdbc.internal.common.packet.*;
import org.mariadb.jdbc.internal.common.packet.buffer.ReadUtil;
import org.mariadb.jdbc.internal.mysql.MySQLColumnInformation;
import org.mariadb.jdbc.internal.mysql.MySQLProtocol;
import org.mariadb.jdbc.internal.mysql.packet.MySQLBinaryRowPacket;
import org.mariadb.jdbc.internal.mysql.packet.MySQLRowPacket;

import java.io.IOException;

public class StreamingSelectResult extends SelectQueryResult {
    public ValueObject[] values;
    PacketFetcher packetFetcher;
    MySQLProtocol protocol;
    boolean isEOF;
    boolean beforeFirst;
    boolean binaryProtocol;


    private StreamingSelectResult(MySQLColumnInformation[] info, MySQLProtocol protocol, PacketFetcher fetcher, boolean binaryProtocol) throws QueryException {
        this.columnInformation = info;
        this.protocol = protocol;
        this.packetFetcher = fetcher;
        this.beforeFirst = true;
        this.isEOF = false;
        this.binaryProtocol = binaryProtocol;
        protocol.activeResult = this;
    }

    /**
     * @param packet         the result set packet from the server
     * @param packetFetcher  packetfetcher
     * @param protocol       the current connection protocol class
     * @param binaryProtocol is the mysql protocol binary
     * @return a StreamingQueryResult
     * @throws IOException    when something goes wrong while reading/writing from the server
     * @throws QueryException if there is an actual active result on the current connection
     */
    public static StreamingSelectResult createStreamingSelectResult(
            ResultSetPacket packet, PacketFetcher packetFetcher, MySQLProtocol protocol, boolean binaryProtocol)
            throws IOException, QueryException {

        if (protocol.activeResult != null) {
            throw new QueryException("There is an active result set on the current connection, " +
                    "which must be closed prior to opening a new one");
        }
        long fieldCount = packet.getFieldCount();
        MySQLColumnInformation[] ci = new MySQLColumnInformation[(int) fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            final RawPacket rawPacket = packetFetcher.getRawPacket();

            // We do not expect an error packet, but check it just for safety
            if (ReadUtil.isErrorPacket(rawPacket)) {
                ErrorPacket errorPacket = new ErrorPacket(rawPacket);
                throw new QueryException("error when reading field packet " + errorPacket.getMessage(),
                        errorPacket.getErrorNumber(), errorPacket.getSqlState());
            }
            // We do not expect OK or EOF packets either
            byte b = rawPacket.getByteBuffer().get(0);
            if (b == 0 || b == (byte) 0xfe) {
                throw new QueryException("Packets out of order when trying to read field packet - " +
                        "got packet starting with byte " + b + "packet content (hex) = "
                        + MySQLProtocol.hexdump(rawPacket.getByteBuffer(), 0));
            }

            try {
                ci[i] = new MySQLColumnInformation(rawPacket);
            } catch (Exception e) {
                throw new QueryException("Error when trying to parse field packet : " + e + ",packet content (hex) = " +
                        MySQLProtocol.hexdump(rawPacket.getByteBuffer(), 0), 0, "HY000", e);
            }
        }
        RawPacket fieldEOF = packetFetcher.getRawPacket();
        if (!ReadUtil.eofIsNext(fieldEOF)) {
            throw new QueryException("Packets out of order when reading field packets, expected was EOF packet. " +
                    "Packet contents (hex) = " + MySQLProtocol.hexdump(fieldEOF.getByteBuffer(), 0));
        }
        return new StreamingSelectResult(ci, protocol, packetFetcher, binaryProtocol);

    }

    @Override
    public boolean next() throws IOException, QueryException {
        if (isEOF) return false;

        RawPacket rawPacket = packetFetcher.getRawPacket();

        if (ReadUtil.isErrorPacket(rawPacket)) {
            protocol.activeResult = null;
            protocol.moreResults = false;
            ErrorPacket errorPacket = (ErrorPacket) ResultPacketFactory.createResultPacket(rawPacket);
            throw new QueryException(errorPacket.getMessage(), errorPacket.getErrorNumber(), errorPacket.getSqlState());
        }

        if (ReadUtil.eofIsNext(rawPacket)) {
            final EOFPacket eofPacket = (EOFPacket) ResultPacketFactory.createResultPacket(rawPacket);
            protocol.activeResult = null;
            protocol.moreResults = ((eofPacket.getStatusFlags() & ServerStatus.MORE_RESULTS_EXISTS) != 0);
            warningCount = eofPacket.getWarningCount();
            protocol.hasWarnings = (warningCount > 0);
            isEOF = true;
            values = null;
            return false;
        }

        if (binaryProtocol) {
            values = new MySQLBinaryRowPacket(rawPacket, columnInformation, protocol.getOptions()).getRow(packetFetcher);
        } else {
            values = new MySQLRowPacket(rawPacket, columnInformation, protocol.getOptions()).getRow(packetFetcher);
        }
        return true;
    }

    public void close() {
        super.close();
        if (protocol != null && protocol.activeResult == this) {
            try {
                for (; ; ) {
                    try {
                        if (protocol.activeResult == null) {
                            return;
                        }
                        if (!next())
                            return;
                    } catch (QueryException qe) {
                        return;
                    } catch (IOException ioe) {
                        return;
                    }
                }
            } finally {
                protocol.activeResult = null;
                protocol = null;
                packetFetcher = null;
            }
        }
    }

    /**
     * gets the value at position i in the result set. i starts at zero!
     *
     * @param i index, starts at 0
     * @return the value
     */
    @Override
    public ValueObject getValueObject(int i) throws NoSuchColumnException {
        return values[i];
    }

    public int getRows() {
        return -1;
    }

    public boolean isBeforeFirst() {
        return beforeFirst;
    }

    public boolean isAfterLast() {
        return isEOF;
    }

    public boolean isBinaryProtocol() {
        return binaryProtocol;
    }

}
