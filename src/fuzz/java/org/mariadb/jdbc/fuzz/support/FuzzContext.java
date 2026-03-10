// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab

package org.mariadb.jdbc.fuzz.support;

import java.sql.SQLException;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.function.Function;
import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import org.mariadb.jdbc.BasePreparedStatement;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.client.ColumnDecoder;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.ServerVersion;
import org.mariadb.jdbc.export.ExceptionFactory;
import org.mariadb.jdbc.export.Prepare;
import org.mariadb.jdbc.message.server.util.ServerVersionUtility;

/** Unified mock for MariaDB connection context. */
public class FuzzContext implements Context {
  private final Configuration conf;
  private final ServerVersion version;
  private final HostAddress hostAddress;

  public FuzzContext() {
    try {
      this.conf = Configuration.parse("jdbc:mariadb://localhost/test");
    } catch (SQLException e) {
      throw new RuntimeException("Failed to initialize FuzzContext", e);
    }
    this.version = new ServerVersionUtility("10.11.0-MariaDB", true);
    this.hostAddress = HostAddress.from("localhost", 3306);
  }

  public FuzzContext(FuzzedDataProvider data) {
    this();
  }

  @Override
  public long getThreadId() {
    return 1;
  }

  @Override
  public Boolean isLoopbackAddress() {
    return true;
  }

  @Override
  public void setThreadId(long connectionId) {}

  @Override
  public Long getAutoIncrement() {
    return 0L;
  }

  @Override
  public void setAutoIncrement(long autoIncrement) {}

  @Override
  public void setMaxscaleVersion(String maxscaleVersion) {}

  @Override
  public String getMaxscaleVersion() {
    return null;
  }

  @Override
  public void setRedirectUrl(String redirectUrl) {}

  @Override
  public String getRedirectUrl() {
    return null;
  }

  @Override
  public byte[] getSeed() {
    return new byte[20];
  }

  @Override
  public boolean hasServerCapability(long flag) {
    return true;
  }

  @Override
  public boolean hasClientCapability(long flag) {
    return true;
  }

  @Override
  public boolean permitPipeline() {
    return false;
  }

  @Override
  public int getServerStatus() {
    return 0;
  }

  @Override
  public void setServerStatus(int serverStatus) {}

  @Override
  public String getDatabase() {
    return "test";
  }

  @Override
  public void setDatabase(String database) {}

  @Override
  public ServerVersion getVersion() {
    return version;
  }

  @Override
  public boolean isEofDeprecated() {
    return false;
  }

  @Override
  public boolean canSkipMeta() {
    return false;
  }

  @Override
  public Function<ReadableByteBuf, ColumnDecoder> getColumnDecoderFunction() {
    return (buf) -> new FuzzColumn();
  }

  @Override
  public int getWarning() {
    return 0;
  }

  @Override
  public void setWarning(int warning) {}

  @Override
  public ExceptionFactory getExceptionFactory() {
    return new ExceptionFactory(conf, hostAddress);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public boolean canUseTransactionIsolation() {
    return true;
  }

  @Override
  public Integer getTransactionIsolationLevel() {
    return 0;
  }

  @Override
  public void setTransactionIsolationLevel(Integer transactionIsolationLevel) {}

  @Override
  public Prepare getPrepareCacheCmd(String sql, BasePreparedStatement preparedStatement) {
    return null;
  }

  @Override
  public Prepare putPrepareCacheCmd(
      String sql, Prepare result, BasePreparedStatement preparedStatement) {
    return null;
  }

  @Override
  public void resetPrepareCache() {}

  @Override
  public int getStateFlag() {
    return 0;
  }

  @Override
  public void resetStateFlag() {}

  @Override
  public void addStateFlag(int state) {}

  @Override
  public void setTreadsConnected(long threadsConnected) {}

  @Override
  public String getCharset() {
    return "utf8mb4";
  }

  @Override
  public void setCharset(String charset) {}

  @Override
  public TimeZone getConnectionTimeZone() {
    return TimeZone.getDefault();
  }

  @Override
  public void setConnectionTimeZone(TimeZone connectionTimeZone) {}

  @Override
  public Calendar getDefaultCalendar() {
    return Calendar.getInstance();
  }
}
