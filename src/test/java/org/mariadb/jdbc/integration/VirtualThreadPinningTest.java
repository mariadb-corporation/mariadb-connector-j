// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.integration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordedFrame;
import jdk.jfr.consumer.RecordedStackTrace;
import jdk.jfr.consumer.RecordingFile;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledForJreRange;
import org.junit.jupiter.api.condition.JRE;
import org.mariadb.jdbc.MariaDbPoolDataSource;

/**
 * Runs the driver's main paths on virtual threads and fails if a virtual thread pinned its carrier
 * inside driver code (a blocking operation under a {@code synchronized} monitor). Checked through
 * the {@code jdk.VirtualThreadPinned} JFR event, so it works on any Java 21+ runtime.
 */
@EnabledForJreRange(min = JRE.JAVA_21)
public class VirtualThreadPinningTest extends Common {

  @Test
  public void noCarrierPinningInDriverCode() throws Exception {
    Assumptions.assumeTrue(!isMaxscale());
    Statement init = sharedConn.createStatement();
    init.execute("DROP TABLE IF EXISTS pinning_t");
    init.execute("CREATE TABLE pinning_t(id int, v varchar(100), d datetime)");
    init.execute("INSERT INTO pinning_t SELECT seq, repeat('x', 80), now() FROM seq_1_to_500");

    // warm-up: class loading, lambda bootstrap and class initialization contend on JDK-internal
    // monitors, which is not what this test is about
    workload();

    Path file = Files.createTempFile("pinned", ".jfr");
    try (Recording recording = new Recording()) {
      recording.enable("jdk.VirtualThreadPinned").withThreshold(Duration.ZERO).withStackTrace();
      recording.start();
      workload();
      recording.stop();
      recording.dump(file);
    } finally {
      init.execute("DROP TABLE IF EXISTS pinning_t");
    }

    List<String> pinnedInDriver = new ArrayList<>();
    for (RecordedEvent event : RecordingFile.readAllEvents(file)) {
      RecordedStackTrace stack = event.getStackTrace();
      if (stack == null) continue;
      String driverFrame = null;
      boolean jdkInternal = false;
      for (RecordedFrame frame : stack.getFrames()) {
        String type = frame.getMethod().getType().getName();
        if (type.startsWith("jdk.internal.loader.")
            || type.equals("java.lang.ClassLoader")
            || type.startsWith("java.lang.invoke.")) {
          // pinned on a JDK-internal monitor (class loading, lambda bootstrap): not a driver lock
          jdkInternal = true;
          break;
        }
        if (type.startsWith("org.mariadb.jdbc")) {
          driverFrame = type + "." + frame.getMethod().getName() + ":" + frame.getLineNumber();
          break;
        }
      }
      if (driverFrame != null && !jdkInternal) {
        pinnedInDriver.add(driverFrame);
      }
    }
    Files.deleteIfExists(file);
    assertEquals(
        Collections.emptyList(),
        pinnedInDriver,
        "virtual threads pinned their carrier inside driver code");
  }

  private void workload() throws Exception {
    String base = mDefUrl + "&sslMode=" + (sslMode() == null ? "disable" : sslMode());
    // text protocol with calendar decoding
    run(
        16,
        () -> {
          Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
          try (Connection c = DriverManager.getConnection(base + "&useServerPrepStmts=false");
              Statement st = c.createStatement()) {
            for (int i = 0; i < 50; i++) {
              try (ResultSet rs = st.executeQuery("SELECT * FROM pinning_t LIMIT 50")) {
                while (rs.next()) {
                  rs.getTimestamp(3, cal);
                  rs.getString(2);
                }
              }
            }
          }
          return null;
        });
    // server prepared statements with a tiny cache: constant eviction (COM_STMT_CLOSE)
    run(
        16,
        () -> {
          try (Connection c =
              DriverManager.getConnection(
                  base + "&useServerPrepStmts=true&cachePrepStmts=true&prepStmtCacheSize=2")) {
            for (int i = 0; i < 100; i++) {
              try (PreparedStatement ps =
                  c.prepareStatement("SELECT id + " + (i % 10) + " FROM pinning_t WHERE id = ?")) {
                ps.setInt(1, i % 100);
                try (ResultSet rs = ps.executeQuery()) {
                  rs.next();
                }
              }
            }
          }
          return null;
        });
    // streaming result sets and compression
    run(
        16,
        () -> {
          try (Connection c = DriverManager.getConnection(base + "&useCompression=true");
              Statement st = c.createStatement()) {
            st.setFetchSize(10);
            for (int i = 0; i < 10; i++) {
              try (ResultSet rs = st.executeQuery("SELECT * FROM pinning_t")) {
                while (rs.next()) rs.getInt(1);
              }
            }
          }
          return null;
        });
    // pool churn, then pool close
    MariaDbPoolDataSource ds =
        new MariaDbPoolDataSource(base + "&maxPoolSize=8&minPoolSize=2&poolName=pinningPool");
    run(
        32,
        () -> {
          for (int i = 0; i < 30; i++) {
            try (Connection c = ds.getConnection();
                Statement st = c.createStatement();
                ResultSet rs = st.executeQuery("SELECT 1")) {
              rs.next();
            }
          }
          return null;
        });
    run(
        1,
        () -> {
          ds.close();
          return null;
        });
  }

  private static void run(int threads, Callable<Void> body) throws Exception {
    // compiled for Java 17: reach the virtual-thread executor reflectively
    ExecutorService executor =
        (ExecutorService) Executors.class.getMethod("newVirtualThreadPerTaskExecutor").invoke(null);
    try {
      List<Future<Void>> futures = new ArrayList<>();
      for (int i = 0; i < threads; i++) futures.add(executor.submit(body));
      for (Future<Void> future : futures) future.get(120, TimeUnit.SECONDS);
    } finally {
      executor.shutdown();
      executor.awaitTermination(120, TimeUnit.SECONDS);
    }
  }

  private static String sslMode() throws SQLException {
    return org.mariadb.jdbc.Configuration.parse(mDefUrl).sslMode() == null
        ? null
        : org.mariadb.jdbc.Configuration.parse(mDefUrl).sslMode().name().toLowerCase();
  }
}
