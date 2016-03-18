package org.mariadb.jdbc.internal.queryresults;

import org.mariadb.jdbc.internal.util.buffer.Buffer;
import org.mariadb.jdbc.internal.packet.read.ReadPacketFetcher;
import org.mariadb.jdbc.internal.packet.read.Packet;
import org.mariadb.jdbc.internal.packet.result.EndOfFilePacket;
import org.mariadb.jdbc.internal.packet.result.ErrorPacket;
import org.mariadb.jdbc.internal.packet.dao.ColumnInformation;
import org.mariadb.jdbc.internal.protocol.AbstractConnectProtocol;
import org.mariadb.jdbc.internal.protocol.AbstractQueryProtocol;
import org.mariadb.jdbc.internal.protocol.MasterProtocol;
import org.mariadb.jdbc.internal.packet.result.BinaryRowPacket;
import org.mariadb.jdbc.internal.packet.result.TextRowPacket;
import org.mariadb.jdbc.internal.packet.result.RowPacket;
import org.mariadb.jdbc.internal.util.constant.ServerStatus;
import org.mariadb.jdbc.internal.util.dao.QueryException;

import java.io.IOException;

public class StreamingSelectResult extends SelectQueryResult {
    public ValueObject[] values;
    private ReadPacketFetcher packetFetcher;
    private AbstractConnectProtocol protocol;
    private boolean isEof;
    private boolean beforeFirst;
    private boolean binaryProtocol;
    private RowPacket rowPacket;


    /**
     * Create Streaming resultset.
     * @param info column information
     * @param protocol protocol information
     * @param fetcher stream fetcher
     * @param binaryProtocol is binary protocol ?
     */
    public StreamingSelectResult(ColumnInformation[] info, AbstractConnectProtocol protocol, ReadPacketFetcher fetcher, boolean binaryProtocol) {
        this.columnInformation = info;
        this.columnInformationLength = info.length;
        this.protocol = protocol;
        this.packetFetcher = fetcher;
        this.beforeFirst = true;
        this.isEof = false;
        this.binaryProtocol = binaryProtocol;
        protocol.activeResult = this;
        if (binaryProtocol) {
            rowPacket = new BinaryRowPacket(columnInformation, protocol.getOptions(), columnInformationLength);
        } else {
            rowPacket = new TextRowPacket(columnInformation, protocol.getOptions(), columnInformationLength);
        }
    }

    @Override
    public void addResult(AbstractQueryResult other) {

    }

    /**
     * Create streaming resultset.
     * @param fieldCount     fieldCount
     * @param packetFetcher  packetfetcher
     * @param protocol       the current connection protocol class
     * @param binaryProtocol is the mysql protocol binary
     * @return a StreamingQueryResult
     * @throws IOException    when something goes wrong while reading/writing from the server
     * @throws QueryException if there is an actual active result on the current connection
     */
    public static StreamingSelectResult createStreamingSelectResult(
            long fieldCount, ReadPacketFetcher packetFetcher, AbstractQueryProtocol protocol, boolean binaryProtocol)
            throws IOException, QueryException {

        if (protocol.activeResult != null) {
            throw new QueryException("There is an active result set on the current connection, "
                    + "which must be closed prior to opening a new one");
        }
        ColumnInformation[] ci = new ColumnInformation[(int) fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            final Buffer buffer = packetFetcher.getPacket();
            try {
                ci[i] = new ColumnInformation(buffer);
            } catch (Exception e) {
                throw new QueryException("Error when trying to parse field stream : " + e + ",stream content (hex) = "
                        + MasterProtocol.hexdump(buffer.buf, 0), 0, "HY000", e);
            }
        }
        Buffer bufferEof = packetFetcher.getReusableBuffer();
        if ((bufferEof.getByteAt(0) != (byte) 0xfe || bufferEof.limit >= 9)) {
            throw new QueryException("Packets out of order when reading field packets, expected was EOF stream. "
                    + "Packet contents (hex) = " + MasterProtocol.hexdump(bufferEof.buf, 0));
        }
        return new StreamingSelectResult(ci, protocol, packetFetcher, binaryProtocol);

    }

    @Override
    public boolean next() throws IOException, QueryException {
        if (isEof) {
            return false;
        }

        Buffer buffer = packetFetcher.getPacket();
        byte initialByte = buffer.getByteAt(0);

        //is error Packet
        if (initialByte == Packet.ERROR) {
            protocol.activeResult = null;
            protocol.moreResults = false;
            ErrorPacket errorPacket = new ErrorPacket(buffer);
            throw new QueryException(errorPacket.getMessage(), errorPacket.getErrorNumber(), errorPacket.getSqlState());
        }

        //is EOF stream
        if ((initialByte == Packet.EOF && buffer.remaining() < 9)) {
            final EndOfFilePacket endOfFilePacket = new EndOfFilePacket(buffer);
            protocol.activeResult = null;
            protocol.moreResults = ((endOfFilePacket.getStatusFlags() & ServerStatus.MORE_RESULTS_EXISTS) != 0);
            warningCount = endOfFilePacket.getWarningCount();
            protocol.hasWarnings = (warningCount > 0);
            isEof = true;
            values = null;
            return false;
        }

        values = rowPacket.getRow(packetFetcher, buffer);
        return true;
    }

    /**
     * Close resultset.
     */
    public void close() {
        super.close();
        if (protocol != null && protocol.activeResult == this) {
            try {
                for (; ; ) {
                    try {
                        if (protocol.activeResult == null) {
                            return;
                        }
                        if (!next()) {
                            return;
                        }
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
     * Gets the value at position i in the result set. i starts at zero.
     *
     * @param index index, starts at 0
     * @return the value
     */
    @Override
    public ValueObject getValueObject(int index) throws NoSuchColumnException {
        return values[index];
    }

    public int getRows() {
        return -1;
    }

    public boolean isBeforeFirst() {
        return beforeFirst;
    }

    public boolean isAfterLast() {
        return isEof;
    }

    public boolean isBinaryProtocol() {
        return binaryProtocol;
    }

}
