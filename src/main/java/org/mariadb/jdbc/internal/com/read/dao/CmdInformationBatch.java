/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2019 MariaDB Ab.
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
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc.internal.com.read.dao;

import org.mariadb.jdbc.internal.com.read.resultset.*;
import org.mariadb.jdbc.internal.protocol.*;

import java.sql.*;
import java.util.*;
import java.util.concurrent.*;

public class CmdInformationBatch implements CmdInformation {

  private final Queue<Long> insertIds = new ConcurrentLinkedQueue<>();
  private final Queue<Long> updateCounts = new ConcurrentLinkedQueue<>();
  private final int expectedSize;
  private final int autoIncrement;
  private int insertIdNumber = 0;
  private boolean hasException;
  private boolean rewritten;

  /**
   * CmdInformationBatch is similar to CmdInformationMultiple, but knowing it's for batch, doesn't
   * take take of moreResult. That permit to use ConcurrentLinkedQueue, and then when option
   * "useBatchMultiSend" is set and batch is interrupted, will permit to reading thread to keep
   * connection in a correct state without any ConcurrentModificationException.
   *
   * @param expectedSize expected batch size.
   * @param autoIncrement connection auto increment value.
   */
  public CmdInformationBatch(int expectedSize, int autoIncrement) {
    this.expectedSize = expectedSize;
    this.autoIncrement = autoIncrement;
  }

  @Override
  public void addErrorStat() {
    hasException = true;
    updateCounts.add((long) Statement.EXECUTE_FAILED);
  }

  /**
   * Clear error state, used for clear exception after first batch query, when fall back to
   * per-query execution.
   */
  @Override
  public void reset() {
    insertIds.clear();
    updateCounts.clear();
    insertIdNumber = 0;
    hasException = false;
    rewritten = false;
  }

  public void addResultSetStat() {
    this.updateCounts.add((long) RESULT_SET_VALUE);
  }

  @Override
  public void addSuccessStat(long updateCount, long insertId) {
    insertIds.add(insertId);
    insertIdNumber += updateCount;
    updateCounts.add(updateCount);
  }

  @Override
  public int[] getUpdateCounts() {
    if (rewritten) {
      int[] ret = new int[expectedSize];
      int resultValue;
      if (hasException) {
        resultValue = Statement.EXECUTE_FAILED;
      } else if (expectedSize == 1) {
        resultValue = updateCounts.element().intValue();
      } else {
        resultValue = 0;
        Iterator<Long> iterator = updateCounts.iterator();
        while (iterator.hasNext()) {
          if (iterator.next().intValue() != 0) {
            resultValue = Statement.SUCCESS_NO_INFO;
          }
        }
      }
      Arrays.fill(ret, resultValue);
      return ret;
    }

    int[] ret = new int[Math.max(updateCounts.size(), expectedSize)];

    Iterator<Long> iterator = updateCounts.iterator();
    int pos = 0;
    while (iterator.hasNext()) {
      ret[pos++] = iterator.next().intValue();
    }

    // in case of Exception
    while (pos < ret.length) {
      ret[pos++] = Statement.EXECUTE_FAILED;
    }

    return ret;
  }

  @Override
  public int[] getServerUpdateCounts() {
    int[] ret = new int[updateCounts.size()];
    Iterator<Long> iterator = updateCounts.iterator();
    int pos = 0;
    while (iterator.hasNext()) {
      ret[pos++] = iterator.next().intValue();
    }
    return ret;
  }

  @Override
  public long[] getLargeUpdateCounts() {
    if (rewritten) {
      long[] ret = new long[expectedSize];
      long resultValue;
      if (hasException) {
        resultValue = Statement.EXECUTE_FAILED;
      } else if (expectedSize == 1) {
        resultValue = updateCounts.element().longValue();
      } else {
        resultValue = 0;
        Iterator<Long> iterator = updateCounts.iterator();
        while (iterator.hasNext()) {
          if (iterator.next().intValue() != 0) {
            resultValue = Statement.SUCCESS_NO_INFO;
          }
        }
      }
      Arrays.fill(ret, resultValue);
      return ret;
    }

    long[] ret = new long[Math.max(updateCounts.size(), expectedSize)];

    Iterator<Long> iterator = updateCounts.iterator();
    int pos = 0;
    while (iterator.hasNext()) {
      ret[pos++] = iterator.next();
    }

    // in case of Exception
    while (pos < ret.length) {
      ret[pos++] = Statement.EXECUTE_FAILED;
    }

    return ret;
  }

  @Override
  public int getUpdateCount() {
    Long updateCount = updateCounts.peek();
    return (updateCount == null) ? -1 : updateCount.intValue();
  }

  @Override
  public long getLargeUpdateCount() {
    Long updateCount = updateCounts.peek();
    return (updateCount == null) ? -1 : updateCount;
  }

  @Override
  public ResultSet getBatchGeneratedKeys(Protocol protocol) {
    long[] ret = new long[insertIdNumber];
    int position = 0;
    long insertId;
    Iterator<Long> idIterator = insertIds.iterator();
    for (Long updateCountLong : updateCounts) {
      int updateCount = updateCountLong.intValue();
      if (updateCount != Statement.EXECUTE_FAILED
          && updateCount != RESULT_SET_VALUE
          && (insertId = idIterator.next()) > 0) {
        for (int i = 0; i < updateCount; i++) {
          ret[position++] = insertId + i * autoIncrement;
        }
      }
    }
    return SelectResultSet.createGeneratedData(ret, protocol, true);
  }

  /**
   * Return GeneratedKeys containing insert ids. Insert ids are calculated using autoincrement
   * value.
   *
   * @param protocol current protocol
   * @return a resultSet with insert ids.
   */
  public ResultSet getGeneratedKeys(Protocol protocol) {
    long[] ret = new long[insertIdNumber];
    int position = 0;
    long insertId;
    Iterator<Long> idIterator = insertIds.iterator();

    for (Long updateCountLong : updateCounts) {
      int updateCount = updateCountLong.intValue();
      if (updateCount != Statement.EXECUTE_FAILED
          && updateCount != RESULT_SET_VALUE
          && (insertId = idIterator.next()) > 0) {
        for (int i = 0; i < updateCount; i++) {
          ret[position++] = insertId + i * autoIncrement;
        }
      }
    }
    return SelectResultSet.createGeneratedData(ret, protocol, true);
  }

  public int getCurrentStatNumber() {
    return updateCounts.size();
  }

  @Override
  public boolean moreResults() {
    return false;
  }

  @Override
  public boolean isCurrentUpdateCount() {
    return false;
  }

  public void setRewrite(boolean rewritten) {
    this.rewritten = rewritten;
  }
}
