// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2023 MariaDB Corporation Ab
package org.mariadb.jdbc.client.result;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantLock;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.socket.Reader;

/**
 * Streaming result-set implementation. Implementation rely on reading as many rows than fetch size
 * required, keeping remaining rows in TCP-IP buffer
 *
 * <p>The server usually expects clients to read off the result set relatively quickly. The
 * net_write_timeout server variable controls this behavior (defaults to 60s).
 *
 * <p>If you don't expect results to be handled in this amount of time there is a different
 * possibility:
 *
 * <ul>
 *   <li>With &gt; MariaDB server, you can use the query "SET STATEMENT net_write_timeout=10000 FOR
 *       XXX" with XXX your "normal" query. This will indicate that specifically for this query,
 *       net_write_timeout will be set to a longer time (10000 in this example).
 *   <li>for non mariadb servers, a specific query will have to temporarily set net_write_timeout
 *       ("SET STATEMENT net_write_timeout=..."), and set it back afterward.
 *   <li>if your application usually uses a lot of long queries with fetch size, the connection can
 *       be set using option "sessionVariables=net_write_timeout=xxx"
 * </ul>
 *
 * <p>Even using setFetchSize, the server will send all results to the client.
 *
 * <p>If another query is executed on the same connection when a streaming result-set has not been
 * fully read, the connector will put the whole remaining streaming result-set in memory in order to
 * execute the next query. This can lead to OutOfMemoryError if not handled.
 */
public class StreamingResult extends Result {

  private final ReentrantLock lock;
  private int dataFetchTime;
  private int fetchSize;

  /**
   * Constructor
   *
   * @param stmt statement that initiate this result
   * @param binaryProtocol is result-set binary encoded
   * @param maxRows maximum row number
   * @param metadataList column metadata
   * @param reader packet reader
   * @param context connection context
   * @param fetchSize fetch size
   * @param lock thread safe locker
   * @param resultSetType result-set type
   * @param closeOnCompletion close statement on completion
   * @param traceEnable can network log be logged
   * @throws SQLException if any error occurs
   */
  @SuppressWarnings({"this-escape"})
  public StreamingResult(
      Statement stmt,
      boolean binaryProtocol,
      long maxRows,
      ColumnDecoder[] metadataList,
      Reader reader,
      Context context,
      int fetchSize,
      ReentrantLock lock,
      int resultSetType,
      boolean closeOnCompletion,
      boolean traceEnable)
      throws SQLException {

    super(
        stmt,
        binaryProtocol,
        maxRows,
        metadataList,
        reader,
        context,
        resultSetType,
        closeOnCompletion,
        traceEnable, false);
    this.lock = lock;
    this.dataFetchTime = 0;
    this.fetchSize = fetchSize;
    this.data = new byte[Math.max(fetchSize, 10)][];

    addStreamingValue();
  }

  @Override
  public boolean streaming() {
    return true;
  }

  /**
   * This permit to replace current stream results by next ones.
   *
   * @throws SQLException if server return an unexpected error
   */
  private void nextStreamingValue() throws SQLException {

    // if resultSet can be back to some previous value
    if (resultSetType == TYPE_FORWARD_ONLY) {
      rowPointer = 0;
      dataSize = 0;
    }

    addStreamingValue();
  }

  private void addStreamingValue() throws SQLException {
    lock.lock();
    try {
      // read only fetchSize values
      int fetchSizeTmp =
          (maxRows <= 0)
              ? fetchSize
              : Math.min(fetchSize, Math.max(0, (int) (maxRows - dataFetchTime * fetchSize)));
      do {
        byte[] buf = reader.readPacket(traceEnable);
        readNext(buf);
        fetchSizeTmp--;
      } while (fetchSizeTmp > 0 && !loaded);
      dataFetchTime++;
      if (maxRows > 0 && (long) dataFetchTime * fetchSize >= maxRows && !loaded) skipRemaining();
    } catch (IOException ioe) {
      throw exceptionFactory.create("Error while streaming resultSet data", "08000", ioe);
    } finally {
      lock.unlock();
    }
  }

  /**
   * When protocol has a current Streaming result (this) fetch all to permit another query is
   * executing.
   *
   * @throws SQLException if any error occur
   */
  public void fetchRemaining() throws SQLException {
    if (!loaded) {
      while (!loaded) {
        addStreamingValue();
      }
      dataFetchTime++;
    }
  }

  @Override
  public boolean next() throws SQLException {
    checkClose();
    if (rowPointer < dataSize - 1) {
      rowPointer++;
      setRow(data[rowPointer]);
      return true;
    } else {
      if (!loaded) {
        lock.lock();
        try {
          if (!loaded) {
            nextStreamingValue();
          }
        } finally {
          lock.unlock();
        }

        if (resultSetType == TYPE_FORWARD_ONLY) {
          // resultSet has been cleared. next value is pointer 0.
          rowPointer = 0;
          if (dataSize > 0) {
            setRow(data[rowPointer]);
            return true;
          }
        } else {
          // cursor can move backward, so driver must keep the results.
          // results have been added to current resultSet
          rowPointer++;
          if (dataSize > rowPointer) {
            setRow(data[rowPointer]);
            return true;
          }
        }
        setNullRowBuf();
        return false;
      }

      // all data are reads and pointer is after last
      rowPointer = dataSize;
      setNullRowBuf();
      return false;
    }
  }

  @Override
  public boolean isAfterLast() throws SQLException {
    checkClose();
    if (rowPointer < dataSize) {
      // has remaining results
      return false;
    } else {
      // has read all data and pointer is after last result
      // so result would have to always be true,
      // but when result contain no row at all jdbc say that must return false
      return dataSize > 0 || dataFetchTime > 1;
    }
  }

  @Override
  public boolean isFirst() throws SQLException {
    checkClose();
    if (resultSetType == TYPE_FORWARD_ONLY) {
      return rowPointer == 0 && dataSize > 0 && dataFetchTime == 1;
    } else {
      return rowPointer == 0 && dataSize > 0;
    }
  }

  @Override
  public boolean isLast() throws SQLException {
    checkClose();
    if (rowPointer < dataSize - 1) {
      return false;
    } else if (loaded) {
      return rowPointer == dataSize - 1 && dataSize > 0;
    } else {
      // when streaming and not having read all results,
      // must read next packet to know if next packet is an EOF packet or some additional data
      addStreamingValue();

      if (loaded) {
        // now driver is sure when data ends.
        return rowPointer == dataSize - 1;
      }

      // There is data remaining
      return false;
    }
  }

  @Override
  public void beforeFirst() throws SQLException {
    checkClose();
    checkNotForwardOnly();
    setNullRowBuf();
    rowPointer = -1;
  }

  @Override
  public void afterLast() throws SQLException {
    checkClose();
    checkNotForwardOnly();
    fetchRemaining();
    setNullRowBuf();
    rowPointer = dataSize;
  }

  @Override
  public boolean first() throws SQLException {
    checkClose();
    checkNotForwardOnly();

    rowPointer = 0;
    if (dataSize > 0) {
      setRow(data[rowPointer]);
      return true;
    }
    setNullRowBuf();
    return false;
  }

  @Override
  public boolean last() throws SQLException {
    checkClose();
    fetchRemaining();
    rowPointer = dataSize - 1;
    if (dataSize > 0) {
      setRow(data[rowPointer]);
      return true;
    }
    setNullRowBuf();
    return false;
  }

  @Override
  public int getRow() throws SQLException {
    checkClose();
    if (resultSetType == TYPE_FORWARD_ONLY) {
      return 0;
    }
    return rowPointer + 1;
  }

  @Override
  public boolean absolute(int idx) throws SQLException {
    checkClose();
    checkNotForwardOnly();

    if (idx == 0) {
      rowPointer = -1;
      setNullRowBuf();
      return false;
    }

    if (idx > 0 && idx <= dataSize) {
      rowPointer = idx - 1;
      setRow(data[rowPointer]);
      return true;
    }

    // if streaming, must read additional results.
    fetchRemaining();

    if (idx > 0) {
      if (idx <= dataSize) {
        rowPointer = idx - 1;
        setRow(data[rowPointer]);
        return true;
      }

      rowPointer = dataSize; // go to afterLast() position
      setNullRowBuf();

    } else {

      if (dataSize + idx >= 0) {
        // absolute position reverse from ending resultSet
        rowPointer = dataSize + idx;
        setRow(data[rowPointer]);
        return true;
      }
      setNullRowBuf();
      rowPointer = -1; // go to before first position
    }
    return false;
  }

  @Override
  public boolean relative(int rows) throws SQLException {
    checkClose();
    int newPos = rowPointer + rows;
    if (newPos <= -1) {
      checkNotForwardOnly();
      rowPointer = -1;
      setNullRowBuf();
      return false;
    }

    while (newPos >= dataSize) {
      if (loaded) {
        rowPointer = dataSize;
        setNullRowBuf();
        return false;
      }
      addStreamingValue();
    }

    rowPointer = newPos;
    setRow(data[rowPointer]);
    return true;
  }

  @Override
  public boolean previous() throws SQLException {
    checkClose();
    checkNotForwardOnly();
    if (rowPointer > -1) {
      rowPointer--;
      if (rowPointer != -1) {
        setRow(data[rowPointer]);
        return true;
      }
    }
    setNullRowBuf();
    return false;
  }

  @Override
  public int getFetchSize() {
    return this.fetchSize;
  }

  @Override
  public void setFetchSize(int fetchSize) throws SQLException {
    if (fetchSize < 0) {
      throw exceptionFactory.create(String.format("invalid fetch size %s", fetchSize));
    }
    if (fetchSize == 0) {
      // fetch all results
      while (!loaded) {
        addStreamingValue();
      }
    }
    this.fetchSize = fetchSize;
  }
}
