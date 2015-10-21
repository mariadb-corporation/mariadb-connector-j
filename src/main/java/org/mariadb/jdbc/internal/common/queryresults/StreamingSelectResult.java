package org.mariadb.jdbc.internal.common.queryresults;

import org.mariadb.jdbc.internal.common.*;
import org.mariadb.jdbc.internal.common.packet.*;
import org.mariadb.jdbc.internal.common.packet.buffer.ReadUtil;
import org.mariadb.jdbc.internal.mysql.ColumnInformation;
import org.mariadb.jdbc.internal.mysql.MariaDbProtocol;
import org.mariadb.jdbc.internal.mysql.packet.BinaryRowPacket;
import org.mariadb.jdbc.internal.mysql.packet.TextRowPacket;
import org.mariadb.jdbc.internal.mysql.packet.RowPacket;

import java.io.IOException;
import java.nio.ByteBuffer;

public class StreamingSelectResult extends SelectQueryResult {
    public ValueObject[] values;
    PacketFetcher packetFetcher;
    MariaDbProtocol protocol;
    Options options;
    boolean isEof;
    boolean beforeFirst;
    boolean binaryProtocol;
    RowPacket rowPacket;


    /**
     * Create Streaming resultset.
     * @param info column information
     * @param protocol protocol information
     * @param fetcher packet fetcher
     * @param binaryProtocol is binary protocol ?
     */
    public StreamingSelectResult(ColumnInformation[] info, MariaDbProtocol protocol, PacketFetcher fetcher, boolean binaryProtocol) {
        this.columnInformation = info;
        this.columnInformationLength = info.length;
        this.protocol = protocol;
        this.options = protocol.getOptions();
        this.packetFetcher = fetcher;
        this.beforeFirst = true;
        this.isEof = false;
        this.binaryProtocol = binaryProtocol;
        protocol.activeResult = this;
        if (binaryProtocol) {
            rowPacket = new BinaryRowPacket(columnInformation, options, columnInformationLength);
        } else {
            rowPacket = new TextRowPacket(columnInformation, options, columnInformationLength);
        }
    }

    /**
     * Create streaming resultset.
     * @param packet         the result set packet from the server
     * @param packetFetcher  packetfetcher
     * @param protocol       the current connection protocol class
     * @param binaryProtocol is the mysql protocol binary
     * @return a StreamingQueryResult
     * @throws IOException    when something goes wrong while reading/writing from the server
     * @throws QueryException if there is an actual active result on the current connection
     */
    public static StreamingSelectResult createStreamingSelectResult(
            ResultSetPacket packet, PacketFetcher packetFetcher, MariaDbProtocol protocol, boolean binaryProtocol)
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

            // We do not expect an error packet, but check it just for safety
//            if (b == (byte) 0xff) {
//                ErrorPacket errorPacket = new ErrorPacket(rawPacket.getByteBuffer());
//                throw new QueryException("error when reading field packet " + errorPacket.getMessage(),
//                        errorPacket.getErrorNumber(), errorPacket.getSqlState());
//            }
//            // We do not expect OK or EOF packets either
//            if (b == 0 || b == (byte) 0xfe) {
//                throw new QueryException("Packets out of order when trying to read field packet - " +
//                        "got packet starting with byte " + b + "packet content (hex) = "
//                        + MariaDbProtocol.hexdump(rawPacket.getByteBuffer(), 0));
//            }

            try {
                ci[i] = new ColumnInformation(rawPacket.getByteBuffer());
            } catch (Exception e) {
                throw new QueryException("Error when trying to parse field packet : " + e + ",packet content (hex) = "
                        + MariaDbProtocol.hexdump(rawPacket.getByteBuffer(), 0), 0, "HY000", e);
            }
        }
        ByteBuffer bufferEof = packetFetcher.getReusableBuffer();
        if (!ReadUtil.eofIsNext(bufferEof)) {
            throw new QueryException("Packets out of order when reading field packets, expected was EOF packet. "
                    + "Packet contents (hex) = " + MariaDbProtocol.hexdump(bufferEof, 0));
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
            ErrorPacket errorPacket = (ErrorPacket) ResultPacketFactory.createResultPacket(buffer);
            throw new QueryException(errorPacket.getMessage(), errorPacket.getErrorNumber(), errorPacket.getSqlState());
        }

        //is EOF packet
        if ((initialByte == (byte) 0xfe && buffer.limit() < 9)) {
            final EndOfFilePacket endOfFilePacket = (EndOfFilePacket) ResultPacketFactory.createResultPacket(buffer);
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
