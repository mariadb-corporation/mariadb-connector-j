/*
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 */

package org.mariadb.jdbc.client.result;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantLock;
import org.mariadb.jdbc.Statement;
import org.mariadb.jdbc.client.context.Context;
import org.mariadb.jdbc.client.socket.PacketReader;
import org.mariadb.jdbc.message.server.ColumnDefinitionPacket;

public class StreamingResult extends Result {

  private final ReentrantLock lock;
  private int dataFetchTime;
  private int fetchSize;

  public StreamingResult(
      Statement stmt,
      boolean binaryProtocol,
      long maxRows,
      ColumnDefinitionPacket[] metadataList,
      PacketReader reader,
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
        traceEnable);
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
      while (fetchSizeTmp > 0 && readNext()) {
        fetchSizeTmp--;
      }
      dataFetchTime++;
      if (maxRows > 0 && dataFetchTime * fetchSize >= maxRows && !loaded) skipRemaining();
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
      row.setRow(data[rowPointer]);
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
            row.setRow(data[rowPointer]);
            return true;
          }
        } else {
          // cursor can move backward, so driver must keep the results.
          // results have been added to current resultSet
          rowPointer++;
          if (dataSize > rowPointer) {
            row.setRow(data[rowPointer]);
            return true;
          }
        }
        row.setRow(null);
        return false;
      }

      // all data are reads and pointer is after last
      rowPointer = dataSize;
      row.setRow(null);
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
      // so result would have to always to be true,
      // but when result contain no row at all jdbc say that must return false
      return dataSize > 0 || dataFetchTime > 1;
    }
  }

  @Override
  public boolean isFirst() throws SQLException {
    checkClose();
    checkNotForwardOnly();
    return rowPointer == 0 && dataSize > 0;
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
        return rowPointer == dataSize - 1 && dataSize > 0;
      }

      // There is data remaining
      return false;
    }
  }

  @Override
  public void beforeFirst() throws SQLException {
    checkClose();
    checkNotForwardOnly();
    row.setRow(null);
    rowPointer = -1;
  }

  @Override
  public void afterLast() throws SQLException {
    checkClose();
    checkNotForwardOnly();
    fetchRemaining();
    row.setRow(null);
    rowPointer = dataSize;
  }

  @Override
  public boolean first() throws SQLException {
    checkClose();
    checkNotForwardOnly();

    rowPointer = 0;
    if (dataSize > 0) {
      row.setRow(data[rowPointer]);
      return true;
    }
    row.setRow(null);
    return false;
  }

  @Override
  public boolean last() throws SQLException {
    checkClose();
    fetchRemaining();
    rowPointer = dataSize - 1;
    if (dataSize > 0) {
      row.setRow(data[rowPointer]);
      return true;
    }
    row.setRow(null);
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
      row.setRow(null);
      return false;
    }

    if (idx > 0 && idx <= dataSize) {
      rowPointer = idx - 1;
      row.setRow(data[rowPointer]);
      return true;
    }

    // if streaming, must read additional results.
    fetchRemaining();

    if (idx > 0) {
      if (idx <= dataSize) {
        rowPointer = idx - 1;
        row.setRow(data[rowPointer]);
        return true;
      }

      rowPointer = dataSize; // go to afterLast() position
      row.setRow(null);

    } else {

      if (dataSize + idx >= 0) {
        // absolute position reverse from ending resultSet
        rowPointer = dataSize + idx;
        row.setRow(data[rowPointer]);
        return true;
      }
      row.setRow(null);
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
      row.setRow(null);
      return false;
    }

    while (newPos >= dataSize) {
      if (loaded) {
        rowPointer = dataSize;
        row.setRow(null);
        return false;
      }
      addStreamingValue();
    }

    rowPointer = newPos;
    row.setRow(data[rowPointer]);
    return true;
  }

  @Override
  public boolean previous() throws SQLException {
    checkClose();
    checkNotForwardOnly();
    if (rowPointer > -1) {
      rowPointer--;
      if (rowPointer != -1) {
        row.setRow(data[rowPointer]);
        return true;
      }
    }
    row.setRow(null);
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
