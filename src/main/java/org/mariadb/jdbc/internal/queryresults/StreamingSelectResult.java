package org.mariadb.jdbc.internal.queryresults;

import org.mariadb.jdbc.internal.util.buffer.ReadUtil;
import org.mariadb.jdbc.internal.packet.read.RawPacket;
import org.mariadb.jdbc.internal.packet.read.ReadPacketFetcher;
import org.mariadb.jdbc.internal.packet.read.ReadResultPacketFactory;
import org.mariadb.jdbc.internal.packet.result.EndOfFilePacket;
import org.mariadb.jdbc.internal.packet.result.ErrorPacket;
import org.mariadb.jdbc.internal.packet.result.ResultSetPacket;
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
import java.nio.ByteBuffer;

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

    public void addResult(AbstractQueryResult other) {
        return;
    }

    /**
     * Create streaming resultset.
     * @param packet         the result set stream from the server
     * @param packetFetcher  packetfetcher
     * @param protocol       the current connection protocol class
     * @param binaryProtocol is the mysql protocol binary
     * @return a StreamingQueryResult
     * @throws IOException    when something goes wrong while reading/writing from the server
     * @throws QueryException if there is an actual active result on the current connection
     */
    public static StreamingSelectResult createStreamingSelectResult(
            ResultSetPacket packet, ReadPacketFetcher packetFetcher, AbstractQueryProtocol protocol, boolean binaryProtocol)
            throws IOException, QueryException {

        if (protocol.activeResult != null) {
            throw new QueryException("There is an active result set on the current connection, "
                    + "which must be closed prior to opening a new one");
        }
        long fieldCount = packet.getFieldCount();
        ColumnInformation[] ci = new ColumnInformation[(int) fieldCount];

        for (int i = 0; i < fieldCount; i++) {
            final RawPacket rawPacket = packetFetcher.getRawPacket();
            //   byte b = rawPacket.getByteBuffer().get(0);

            // We do not expect an error stream, but check it just for safety
//            if (b == (byte) 0xff) {
//                ErrorPacket errorPacket = new ErrorPacket(rawPacket.getByteBuffer());
//                throw new QueryException("error when reading field stream " + errorPacket.getMessage(),
//                        errorPacket.getErrorNumber(), errorPacket.getSqlState());
//            }
//            // We do not expect OK or EOF packets either
//            if (b == 0 || b == (byte) 0xfe) {
//                throw new QueryException("Packets out of order when trying to read field stream - " +
//                        "got stream starting with byte " + b + "stream content (hex) = "
//                        + MasterProtocol.hexdump(rawPacket.getByteBuffer(), 0));
//            }

            try {
                ci[i] = new ColumnInformation(rawPacket.getByteBuffer());
            } catch (Exception e) {
                throw new QueryException("Error when trying to parse field stream : " + e + ",stream content (hex) = "
                        + MasterProtocol.hexdump(rawPacket.getByteBuffer(), 0), 0, "HY000", e);
            }
        }
        ByteBuffer bufferEof = packetFetcher.getReusableBuffer();
        if (!ReadUtil.eofIsNext(bufferEof)) {
            throw new QueryException("Packets out of order when reading field packets, expected was EOF stream. "
                    + "Packet contents (hex) = " + MasterProtocol.hexdump(bufferEof, 0));
        }
        return new StreamingSelectResult(ci, protocol, packetFetcher, binaryProtocol);

    }

    @Override
    public boolean next() throws IOException, QueryException {
        if (isEof) {
            return false;
        }

        ByteBuffer buffer = packetFetcher.getReusableBuffer();
        byte initialByte = buffer.get(0);

        //is error Packet
        if (initialByte == (byte) 0xff) {
            protocol.activeResult = null;
            protocol.moreResults = false;
            ErrorPacket errorPacket = (ErrorPacket) ReadResultPacketFactory.createResultPacket(buffer);
            throw new QueryException(errorPacket.getMessage(), errorPacket.getErrorNumber(), errorPacket.getSqlState());
        }

        //is EOF stream
        if ((initialByte == (byte) 0xfe && buffer.limit() < 9)) {
            final EndOfFilePacket endOfFilePacket = (EndOfFilePacket) ReadResultPacketFactory.createResultPacket(buffer);
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
