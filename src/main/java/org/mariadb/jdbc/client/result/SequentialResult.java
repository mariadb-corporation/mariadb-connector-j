// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.client.result;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.NClob;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.DataType;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.util.ClosableLock;

/**
 * Forward-only result-set reading rows directly from the socket, one column at a time, so that a
 * LOB column can be streamed without ever being loaded in memory. It is selected by a fetch size of
 * {@code Integer.MIN_VALUE} ({@code Statement.setFetchSize(Integer.MIN_VALUE)} or the {@code
 * defaultFetchSize=-2147483648} option) on a forward-only read-only statement.
 *
 * <p>Restrictions compared to a regular forward-only result-set: columns must be read in increasing
 * index order (a column can be read again only if it was not read as a stream), and a stream, Blob,
 * Clob or Reader must be consumed before reading a following column or moving to the next row: it
 * is then invalidated and cannot be read anymore.
 *
 * <p>Row bytes are read from the socket on demand: {@link #next()} only reads the packet header and
 * the first byte (to distinguish a row from the end of the result-set). Reading a column skips the
 * columns before it that were not read, then either loads the column bytes into a small reusable
 * buffer (for all regular getters, decoded by the standard row decoders) or hands out a stream
 * bound to the socket (for {@code getBinaryStream}, {@code getBlob}, {@code getCharacterStream},
 * {@code getClob} ...).
 *
 * <p>When another command has to be executed while this result-set is still being read, {@link
 * #fetchRemaining()} loads the remaining data in memory, like other streaming result-sets, so the
 * result-set stays usable.
 */
public class SequentialResult extends Result {

  private final ClosableLock lock;
  private final boolean binaryProtocol;
  private final PacketRowSource socketSource = new PacketRowSource();

  /** current row source, null when no row is active */
  private RowSource src;

  private boolean rowActive;
  private long rowCount;

  /** next row to serve from {@link #data} once remaining rows have been loaded in memory */
  private int bufferedRowPointer;

  /** 0-based index of the positioned column, -1 when none */
  private int currentCol = -1;

  /** true when the positioned column bytes are loaded in {@link #colBuf} */
  private boolean currentBuffered;

  private int currentLength;
  private int miniRowLimit;
  private SequentialFieldStream activeStream;
  private byte[] colBuf = new byte[64];

  @SuppressWarnings({"this-escape"})
  public SequentialResult(
      Statement stmt,
      boolean binaryProtocol,
      long maxRows,
      ColumnDecoder[] metadataList,
      Reader reader,
      Context context,
      ClosableLock lock,
      int resultSetType,
      boolean closeOnCompletion,
      boolean traceEnable) {
    super(
        stmt,
        binaryProtocol,
        maxRows,
        metadataList,
        reader,
        context,
        resultSetType,
        closeOnCompletion,
        traceEnable,
        false,
        Integer.MIN_VALUE);
    this.lock = lock;
    this.binaryProtocol = binaryProtocol;
    this.data = new byte[0][];
  }

  @Override
  public boolean streaming() {
    return true;
  }

  @Override
  public boolean isBulkResult() {
    return false;
  }

  @Override
  public void setBulkResult() {}

  // *********************************************************************************************
  // row navigation
  // *********************************************************************************************

  @Override
  @SuppressWarnings("try")
  public boolean next() throws SQLException {
    checkClose();
    try (ClosableLock ignore = lock.closeableLock()) {
      try {
        endRow();
        if (bufferedRowPointer < dataSize) {
          // remaining rows were loaded in memory by fetchRemaining()
          byte[] row = data[bufferedRowPointer];
          data[bufferedRowPointer++] = null;
          startRow(new ArrayRowSource(row));
          return true;
        }
        if (loaded) return false;
        if (maxRows > 0 && rowCount >= maxRows) {
          skipRemaining();
          return false;
        }

        int packetLength = reader.readPacketHeader(traceEnable);
        int first = reader.readByte();
        if (first == 0xFF
            || (first == 0xFE
                && ((context.isEofDeprecated() && packetLength < 0xFFFFFF)
                    || (!context.isEofDeprecated() && packetLength < 8)))) {
          // error or end of result-set packet
          byte[] buf = new byte[packetLength];
          buf[0] = (byte) first;
          reader.readFully(buf, 1, packetLength - 1);
          readNext(buf);
          return false;
        }
        socketSource.init(packetLength - 1, packetLength == 0xFFFFFF, first);
        startRow(socketSource);
        return true;
      } catch (IOException ioe) {
        throw ioError(ioe);
      }
    }
  }

  private void startRow(RowSource source) throws IOException {
    src = source;
    rowActive = true;
    currentCol = -1;
    currentBuffered = false;
    rowCount++;
    rowPointer++;
    if (binaryProtocol) {
      src.readByte(); // 0x00 row header
      src.readFully(nullBitmap, 0, nullBitmap.length);
    }
  }

  /** Skip whatever remains of the current row. */
  private void endRow() throws IOException {
    if (!rowActive) return;
    finishCurrentColumn();
    src.skipRest();
    rowActive = false;
    src = null;
    currentCol = -1;
    currentBuffered = false;
  }

  private void finishCurrentColumn() throws IOException {
    if (activeStream != null) {
      long remaining = activeStream.remaining();
      activeStream.detach();
      activeStream = null;
      if (remaining > 0) src.skip(remaining);
    }
    currentBuffered = false;
  }

  /**
   * Load the remaining data in memory: the rest of the current row (so an active stream continues
   * transparently from memory) and the following rows, then read the end of result-set packet.
   */
  @Override
  @SuppressWarnings("try")
  public void fetchRemaining() throws SQLException {
    try (ClosableLock ignore = lock.closeableLock()) {
      try {
        if (rowActive && src == socketSource) {
          src = new ArrayRowSource(socketSource.readRest());
        }
        while (!loaded) {
          readNext(reader.readPacket(traceEnable));
        }
      } catch (IOException ioe) {
        throw ioError(ioe);
      }
    }
  }

  /**
   * After a socket error, or data ending before the announced field length, the position in the
   * stream is unknown: the connection is closed, nothing more can be read from this result-set, and
   * closing it must not try to skip remaining data.
   */
  private SQLException ioError(IOException ioe) {
    invalidate();
    return exceptionFactory.create("Error while streaming resultSet data", "08000", ioe);
  }

  /**
   * A length announced by the server is impossible (field bigger than the row packet, or row or
   * field bigger than maxAllowedPacket): this could be a malicious proxy trying to make the driver
   * allocate memory or to desynchronize the protocol, the connection is closed before any
   * allocation.
   */
  private SQLException protocolError(String msg) {
    invalidate();
    return exceptionFactory.create(msg + ". Possible malicious proxy, connection closed", "08000");
  }

  private void invalidate() {
    if (activeStream != null) {
      activeStream.detach();
      activeStream = null;
    }
    rowActive = false;
    src = null;
    loaded = true;
    dataSize = 0;
    closed = true;
    if (statement != null) {
      try {
        ((org.mariadb.jdbc.Connection) statement.getConnection()).getClient().close();
      } catch (SQLException e) {
        // connection already closed
      }
    }
  }

  /** Discard whatever remains on the socket, without loading it in memory. */
  private void drain() throws IOException, SQLException {
    if (activeStream != null) {
      activeStream.detach();
      activeStream = null;
    }
    if (rowActive) {
      if (src == socketSource) src.skipRest();
      rowActive = false;
      src = null;
    }
    if (!loaded) skipRemaining();
    dataSize = 0;
    bufferedRowPointer = 0;
    colBuf = null;
  }

  @Override
  @SuppressWarnings("try")
  public void close() throws SQLException {
    if (!closed) {
      try (ClosableLock ignore = lock.closeableLock()) {
        try {
          drain();
        } catch (SQLTimeoutException timeout) {
          // eat
        } catch (IOException ioe) {
          throw ioError(ioe);
        }
      }
    }
    super.close();
  }

  @Override
  @SuppressWarnings("try")
  public void closeFromStmtClose(ClosableLock lock) throws SQLException {
    try (ClosableLock ignore = lock.closeableLock()) {
      try {
        drain();
      } catch (IOException ioe) {
        throw ioError(ioe);
      }
      this.closed = true;
    }
  }

  @Override
  public void abort() {
    if (activeStream != null) {
      activeStream.detach();
      activeStream = null;
    }
    super.abort();
  }

  @Override
  public boolean isBeforeFirst() throws SQLException {
    checkClose();
    return rowCount == 0 && !loaded;
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    checkClose();
    return loaded && !rowActive && bufferedRowPointer >= dataSize && rowCount > 0;
  }

  @Override
  public boolean isFirst() throws SQLException {
    checkClose();
    return rowActive && rowCount == 1;
  }

  @Override
  public boolean isLast() throws SQLException {
    checkClose();
    throw exceptionFactory.notSupported(
        "Method ResultSet.isLast() not supported on sequential access resultSet (fetch size"
            + " Integer.MIN_VALUE)");
  }

  private SQLException notPermitted() {
    return exceptionFactory.create(
        "Operation not permit on sequential access resultSet (fetch size Integer.MIN_VALUE)",
        "HY000");
  }

  @Override
  public void beforeFirst() throws SQLException {
    checkClose();
    throw notPermitted();
  }

  @Override
  public void afterLast() throws SQLException {
    checkClose();
    throw notPermitted();
  }

  @Override
  public boolean first() throws SQLException {
    checkClose();
    throw notPermitted();
  }

  @Override
  public boolean last() throws SQLException {
    checkClose();
    throw notPermitted();
  }

  @Override
  public int getRow() throws SQLException {
    checkClose();
    return rowActive ? (int) Math.min(rowCount, Integer.MAX_VALUE) : 0;
  }

  @Override
  public boolean absolute(int row) throws SQLException {
    checkClose();
    throw notPermitted();
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    checkClose();
    if (rows < 0) throw notPermitted();
    boolean res = rowActive;
    for (int i = 0; i < rows; i++) {
      res = next();
      if (!res) break;
    }
    return res;
  }

  @Override
  public boolean previous() throws SQLException {
    checkClose();
    throw notPermitted();
  }

  // *********************************************************************************************
  // column positioning
  // *********************************************************************************************

  /**
   * Called by every standard getter before decoding: load the column bytes in the reusable buffer,
   * shaped as a one-column row so that the standard row decoder can decode it.
   */
  @Override
  protected void checkIndex(int index) throws SQLException {
    checkColumn(index);
    position(index - 1, true);
  }

  private void checkColumn(int index) throws SQLException {
    checkClose();
    if (index < 1 || index > maxIndex) {
      throw new SQLException(
          String.format("Wrong index position. Is %s but must be in 1-%s range", index, maxIndex));
    }
    if (!rowActive) {
      throw new java.sql.SQLDataException("wrong row position", "22023");
    }
  }

  @SuppressWarnings("try")
  private void position(int idx, boolean buffer) throws SQLException {
    if (idx == currentCol) {
      if (currentBuffered) {
        resetMiniRow(idx);
        return;
      }
      throw exceptionFactory.create(
          String.format(
              "Sequential access: column %d has already been read as a stream and cannot be read"
                  + " again",
              idx + 1),
          "HY000");
    }
    if (idx < currentCol) {
      throw exceptionFactory.create(
          String.format(
              "Sequential access: column %d cannot be read after column %d. Columns must be read"
                  + " in increasing index order",
              idx + 1, currentCol + 1),
          "HY000");
    }
    try (ClosableLock ignore = lock.closeableLock()) {
      try {
        finishCurrentColumn();
        for (int i = currentCol + 1; i < idx; i++) {
          skipColumn(i);
        }
        currentCol = idx;
        currentLength = readColumnLength(idx);
        if (buffer || currentLength == NULL_LENGTH) {
          // a NULL value consumes nothing: keep it readable again
          loadColumn(idx, currentLength);
          currentBuffered = true;
          fieldLength.set(currentLength);
        } else {
          fieldIndex.set(idx);
          fieldLength.set(currentLength);
        }
      } catch (IOException ioe) {
        throw ioError(ioe);
      }
    }
  }

  private boolean isNull(int idx) {
    int bit = idx + 2;
    return (nullBitmap[bit / 8] & (1 << (bit % 8))) != 0;
  }

  private static int fixedBinaryLength(DataType type) {
    switch (type) {
      case BIGINT:
      case DOUBLE:
        return 8;
      case INTEGER:
      case MEDIUMINT:
      case FLOAT:
        return 4;
      case SMALLINT:
      case YEAR:
        return 2;
      case TINYINT:
        return 1;
      default:
        return -1;
    }
  }

  private void skipColumn(int idx) throws IOException, SQLException {
    int len = readColumnLength(idx);
    if (len > 0) src.skip(len);
  }

  /**
   * Read the length of the field, positioning the source at its first byte. The length is
   * server-declared: it must fit in what remains of the row packet.
   *
   * @return length in bytes, or NULL_LENGTH
   */
  private int readColumnLength(int idx) throws IOException, SQLException {
    int len;
    if (binaryProtocol && isNull(idx)) return NULL_LENGTH;
    int fixed = binaryProtocol ? fixedBinaryLength(metadataList[idx].getType()) : -1;
    len = fixed > 0 ? fixed : readLengthEncoded();
    if (len > 0) {
      // the length is server-declared and buffered getters allocate from it: it must fit in the
      // row packet, whose size is itself bounded by maxAllowedPacket
      long remaining = src.remainingKnown();
      if (remaining >= 0 && len > remaining) {
        throw protocolError(
            String.format(
                "Invalid length-encoded field length %d: exceeds the %d bytes remaining in row"
                    + " packet",
                len, remaining));
      }
      if (len > reader.getMaxAllowedPacket()) {
        throw protocolError(
            String.format(
                "Invalid length-encoded field length %d: exceeds maxAllowedPacket (%d)",
                len, reader.getMaxAllowedPacket()));
      }
    }
    return len;
  }

  private int readLengthEncoded() throws IOException, SQLException {
    int b = src.readByte();
    if (b < 0) throw new EOFException("row data ended prematurely");
    switch (b) {
      case 0xFB:
        return NULL_LENGTH;
      case 0xFC:
        return src.readByte() | (src.readByte() << 8);
      case 0xFD:
        return src.readByte() | (src.readByte() << 8) | (src.readByte() << 16);
      case 0xFE:
        long len = 0;
        for (int i = 0; i < 8; i++) {
          len |= ((long) src.readByte()) << (8 * i);
        }
        if (len < 0 || len > Integer.MAX_VALUE) {
          throw new SQLException("Invalid length-encoded field length " + len);
        }
        return (int) len;
      default:
        return b;
    }
  }

  /** Load column bytes into colBuf, shaped as a row containing only that column. */
  private void loadColumn(int idx, int len) throws IOException {
    int prefix = binaryProtocol ? 1 + nullBitmap.length : 0;
    boolean lengthEncoded = !binaryProtocol || fixedBinaryLength(metadataList[idx].getType()) < 0;
    int dataLen = Math.max(len, 0);
    int total = prefix + (lengthEncoded ? lengthEncodedSize(len) : 0) + dataLen;
    if (colBuf.length < total) {
      colBuf = new byte[Math.max(total, colBuf.length * 2)];
    }
    int off = 0;
    if (binaryProtocol) {
      colBuf[0] = 0;
      System.arraycopy(nullBitmap, 0, colBuf, 1, nullBitmap.length);
      off = prefix;
    }
    if (lengthEncoded) off = writeLengthEncoded(colBuf, off, len);
    if (dataLen > 0) src.readFully(colBuf, off, dataLen);
    miniRowLimit = total;
    resetMiniRow(idx);
  }

  private void resetMiniRow(int idx) {
    fieldIndex.set(idx - 1);
    // the binary decoder reads the header and null bitmap itself when positioning column 0
    rowBuf.buf(colBuf, miniRowLimit, (binaryProtocol && idx > 0) ? 1 + nullBitmap.length : 0);
  }

  private static int lengthEncodedSize(int len) {
    if (len < 0 || len < 251) return 1;
    if (len < 65536) return 3;
    if (len < 16777216) return 4;
    return 9;
  }

  private static int writeLengthEncoded(byte[] buf, int off, int len) {
    if (len < 0) {
      buf[off] = (byte) 0xFB;
      return off + 1;
    }
    if (len < 251) {
      buf[off] = (byte) len;
      return off + 1;
    }
    if (len < 65536) {
      buf[off] = (byte) 0xFC;
      buf[off + 1] = (byte) len;
      buf[off + 2] = (byte) (len >>> 8);
      return off + 3;
    }
    if (len < 16777216) {
      buf[off] = (byte) 0xFD;
      buf[off + 1] = (byte) len;
      buf[off + 2] = (byte) (len >>> 8);
      buf[off + 3] = (byte) (len >>> 16);
      return off + 4;
    }
    buf[off] = (byte) 0xFE;
    for (int i = 0; i < 8; i++) {
      buf[off + 1 + i] = (byte) (((long) len) >>> (8 * i));
    }
    return off + 9;
  }

  // *********************************************************************************************
  // streaming getters
  // *********************************************************************************************

  private enum StreamKind {
    BYTES,
    BLOB,
    CHARACTERS
  }

  /** Types whose value can be streamed, matching what the corresponding codec accepts. */
  private boolean streamable(int idx, StreamKind kind) {
    ColumnDecoder column = metadataList[idx];
    switch (column.getType()) {
      case BLOB:
      case TINYBLOB:
      case MEDIUMBLOB:
      case LONGBLOB:
        return kind != StreamKind.CHARACTERS || !column.isBinary();
      case STRING:
      case VARCHAR:
      case VARSTRING:
        return true;
      case BIT:
      case GEOMETRY:
        return kind == StreamKind.BLOB;
      default:
        return false;
    }
  }

  /**
   * Position on the column for streaming.
   *
   * @return false when the column must be decoded from the buffer instead (already buffered, or not
   *     a streamable type)
   */
  private boolean positionStream(int columnIndex, StreamKind kind) throws SQLException {
    checkColumn(columnIndex);
    int idx = columnIndex - 1;
    if ((idx == currentCol && currentBuffered) || !streamable(idx, kind)) return false;
    position(idx, false);
    return true;
  }

  private SequentialFieldStream openStream() {
    activeStream = new SequentialFieldStream(this, currentLength);
    return activeStream;
  }

  @Override
  public InputStream getBinaryStream(int columnIndex) throws SQLException {
    if (!positionStream(columnIndex, StreamKind.BYTES)) return super.getBinaryStream(columnIndex);
    return currentLength == NULL_LENGTH ? null : openStream();
  }

  @Override
  public InputStream getAsciiStream(int columnIndex) throws SQLException {
    if (!positionStream(columnIndex, StreamKind.BYTES)) return super.getAsciiStream(columnIndex);
    return currentLength == NULL_LENGTH ? null : openStream();
  }

  @Override
  @Deprecated
  public InputStream getUnicodeStream(int columnIndex) throws SQLException {
    if (!positionStream(columnIndex, StreamKind.BYTES)) return super.getUnicodeStream(columnIndex);
    return currentLength == NULL_LENGTH ? null : openStream();
  }

  @Override
  public Blob getBlob(int columnIndex) throws SQLException {
    if (!positionStream(columnIndex, StreamKind.BLOB)) return super.getBlob(columnIndex);
    return currentLength == NULL_LENGTH ? null : new SequentialBlob(openStream(), currentLength);
  }

  @Override
  public Clob getClob(int columnIndex) throws SQLException {
    if (!positionStream(columnIndex, StreamKind.CHARACTERS)) return super.getClob(columnIndex);
    return currentLength == NULL_LENGTH ? null : new SequentialClob(openStream(), currentLength);
  }

  @Override
  public NClob getNClob(int columnIndex) throws SQLException {
    if (!positionStream(columnIndex, StreamKind.CHARACTERS)) return super.getNClob(columnIndex);
    return currentLength == NULL_LENGTH ? null : new SequentialClob(openStream(), currentLength);
  }

  @Override
  public java.io.Reader getCharacterStream(int columnIndex) throws SQLException {
    if (!positionStream(columnIndex, StreamKind.CHARACTERS)) {
      return super.getCharacterStream(columnIndex);
    }
    return currentLength == NULL_LENGTH ? null : new Utf8StreamReader(openStream());
  }

  @Override
  public java.io.Reader getNCharacterStream(int columnIndex) throws SQLException {
    return getCharacterStream(columnIndex);
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
    if (type == InputStream.class) return (T) getBinaryStream(columnIndex);
    if (type == Blob.class) return (T) getBlob(columnIndex);
    if (type == Clob.class) return (T) getClob(columnIndex);
    if (type == NClob.class) return (T) getNClob(columnIndex);
    if (type == java.io.Reader.class) return (T) getCharacterStream(columnIndex);
    return super.getObject(columnIndex, type);
  }

  // *********************************************************************************************
  // stream callbacks (SequentialFieldStream)
  // *********************************************************************************************

  @SuppressWarnings("try")
  int readStream(SequentialFieldStream stream, byte[] b, int off, int len) throws IOException {
    try (ClosableLock ignore = lock.closeableLock()) {
      checkStream(stream);
      try {
        int n = src.read(b, off, len);
        if (n < 0) throw new EOFException("row data ended before the end of the column");
        return n;
      } catch (IOException ioe) {
        ioError(ioe);
        throw ioe;
      }
    }
  }

  @SuppressWarnings("try")
  int readStreamByte(SequentialFieldStream stream) throws IOException {
    try (ClosableLock ignore = lock.closeableLock()) {
      checkStream(stream);
      try {
        int b = src.readByte();
        if (b < 0) throw new EOFException("row data ended before the end of the column");
        return b;
      } catch (IOException ioe) {
        ioError(ioe);
        throw ioe;
      }
    }
  }

  @SuppressWarnings("try")
  void skipStream(SequentialFieldStream stream, long len) throws IOException {
    try (ClosableLock ignore = lock.closeableLock()) {
      checkStream(stream);
      try {
        src.skip(len);
      } catch (IOException ioe) {
        ioError(ioe);
        throw ioe;
      }
    }
  }

  private void checkStream(SequentialFieldStream stream) throws IOException {
    if (stream != activeStream || closed) {
      throw new IOException(
          "Stream is not readable anymore: result-set has moved to another column or row, or is"
              + " closed");
    }
  }

  // *********************************************************************************************
  // row sources
  // *********************************************************************************************

  /** Bytes of the current row: either still on the socket, or loaded in memory. */
  private interface RowSource {
    /**
     * Read up to len bytes.
     *
     * @return number of bytes read, or -1 at the end of the row
     */
    int read(byte[] b, int off, int len) throws IOException;

    /**
     * Read one byte.
     *
     * @return byte value in 0-255 range, or -1 at the end of the row
     */
    int readByte() throws IOException;

    default void readFully(byte[] b, int off, int len) throws IOException {
      while (len > 0) {
        int n = read(b, off, len);
        if (n < 0) throw new EOFException("row data ended prematurely");
        off += n;
        len -= n;
      }
    }

    void skip(long len) throws IOException;

    void skipRest() throws IOException;

    /**
     * Number of bytes remaining in the row, when known.
     *
     * @return remaining bytes, or -1 when the row continues in packets not yet read
     */
    long remainingKnown();
  }

  /** Row still on the socket, possibly split in several packets of 0xffffff bytes. */
  private final class PacketRowSource implements RowSource {
    private int fragmentRemaining;
    private boolean moreFragments;
    private int pushback = -1;
    private long rowLength;

    void init(int remaining, boolean more, int firstByte) {
      this.fragmentRemaining = remaining;
      this.moreFragments = more;
      this.pushback = firstByte;
      this.rowLength = remaining + 1;
    }

    /**
     * Ensure the current fragment has remaining bytes, reading the next packet header if needed.
     *
     * @return false at the end of the row
     */
    private boolean ensureFragment() throws IOException {
      while (fragmentRemaining == 0) {
        if (!moreFragments) return false;
        int len = reader.readPacketHeader(traceEnable);
        rowLength += len;
        if (rowLength > reader.getMaxAllowedPacket()) {
          // same bound as Reader.readPacket on multi-packet rows
          throw new IOException(
              "received packet size ("
                  + rowLength
                  + ") is greater than maxAllowedPacket ("
                  + reader.getMaxAllowedPacket()
                  + ")");
        }
        fragmentRemaining = len;
        moreFragments = len == 0xFFFFFF;
      }
      return true;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (len == 0) return 0;
      if (pushback >= 0) {
        b[off] = (byte) pushback;
        pushback = -1;
        return 1;
      }
      if (!ensureFragment()) return -1;
      int n = reader.read(b, off, Math.min(len, fragmentRemaining));
      fragmentRemaining -= n;
      return n;
    }

    @Override
    public int readByte() throws IOException {
      if (pushback >= 0) {
        int b = pushback;
        pushback = -1;
        return b;
      }
      if (!ensureFragment()) return -1;
      fragmentRemaining--;
      return reader.readByte();
    }

    @Override
    public void skip(long len) throws IOException {
      if (len > 0 && pushback >= 0) {
        pushback = -1;
        len--;
      }
      while (len > 0) {
        if (!ensureFragment()) throw new EOFException("row data ended prematurely");
        int n = (int) Math.min(len, fragmentRemaining);
        reader.skip(n);
        fragmentRemaining -= n;
        len -= n;
      }
    }

    @Override
    public void skipRest() throws IOException {
      pushback = -1;
      while (ensureFragment()) {
        reader.skip(fragmentRemaining);
        fragmentRemaining = 0;
      }
    }

    @Override
    public long remainingKnown() {
      if (moreFragments) return -1;
      return fragmentRemaining + (pushback >= 0 ? 1 : 0);
    }

    /** Load the rest of the row in memory. */
    byte[] readRest() throws IOException {
      ByteArrayOutputStream out = new ByteArrayOutputStream(Math.max(fragmentRemaining + 1, 16));
      if (pushback >= 0) {
        out.write(pushback);
        pushback = -1;
      }
      while (ensureFragment()) {
        byte[] chunk = new byte[fragmentRemaining];
        reader.readFully(chunk, 0, fragmentRemaining);
        fragmentRemaining = 0;
        out.write(chunk, 0, chunk.length);
      }
      return out.toByteArray();
    }
  }

  /** Row loaded in memory. */
  private static final class ArrayRowSource implements RowSource {
    private final byte[] buf;
    private int pos;

    ArrayRowSource(byte[] buf) {
      this.buf = buf;
    }

    @Override
    public int read(byte[] b, int off, int len) {
      if (len == 0) return 0;
      if (pos >= buf.length) return -1;
      int n = Math.min(len, buf.length - pos);
      System.arraycopy(buf, pos, b, off, n);
      pos += n;
      return n;
    }

    @Override
    public int readByte() {
      if (pos >= buf.length) return -1;
      return buf[pos++] & 0xff;
    }

    @Override
    public void skip(long len) throws IOException {
      if (len > buf.length - pos) throw new EOFException("row data ended prematurely");
      pos += (int) len;
    }

    @Override
    public void skipRest() {
      pos = buf.length;
    }

    @Override
    public long remainingKnown() {
      return buf.length - pos;
    }
  }
}
