/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2017 MariaDB Ab.
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

package org.mariadb.jdbc;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.Thread.State;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.Assume;
import org.junit.BeforeClass;
import org.junit.Test;

public class ExecuteBatchTest extends BaseTest {

  private static final String oneHundredLengthString;
  private static final boolean profileSql = false;

  static {
    char[] chars = new char[100];
    for (int i = 27; i < 127; i++) {
      chars[i - 27] = (char) i;
    }
    oneHundredLengthString = new String(chars);
  }

  /**
   * Create test tables.
   *
   * @throws SQLException if connection error occur
   */
  @BeforeClass()
  public static void initClass() throws SQLException {
    createTable("ExecuteBatchTest",
        "id int not null primary key auto_increment, test varchar(100) , test2 int");
    createTable("ExecuteBatchUseBatchMultiSend", "test varchar(100)");
  }

  /**
   * CONJ-426: Test that executeBatch can be properly interrupted.
   *
   * @throws Exception If the test fails
   */
  @Test
  public void interruptExecuteBatch() throws Exception {
    Assume.assumeTrue(
        sharedOptions().useBatchMultiSend && !(sharedOptions().useBulkStmts && isMariadbServer()
            && minVersion(10, 2)));
    ExecutorService service = Executors.newFixedThreadPool(1);

    final CyclicBarrier barrier = new CyclicBarrier(2);
    final AtomicBoolean wasInterrupted = new AtomicBoolean(false);
    final AtomicReference<Exception> exceptionRef = new AtomicReference<>();

    service.submit(new Runnable() {
      @Override
      public void run() {
        try {
          PreparedStatement preparedStatement = sharedConnection.prepareStatement(
              "INSERT INTO ExecuteBatchTest(test, test2) values (?, ?)");

          // Send a large enough batch that will take long enough to allow us to interrupt it
          for (int i = 0; i < 1000000; i++) {
            preparedStatement.setString(1, String.valueOf(System.nanoTime()));
            preparedStatement.setInt(2, i);
            preparedStatement.addBatch();
          }

          barrier.await();

          preparedStatement.executeBatch();
        } catch (InterruptedException ex) {
          exceptionRef.set(ex);
          Thread.currentThread().interrupt();
        } catch (BrokenBarrierException ex) {
          exceptionRef.set(ex);
        } catch (SQLException ex) {
          exceptionRef.set(ex);
          wasInterrupted.set(Thread.currentThread().isInterrupted());
        } catch (Exception ex) {
          exceptionRef.set(ex);
        }
      }
    });

    barrier.await();

    // Allow the query time to send
    Thread.sleep(TimeUnit.SECONDS.toMillis(1));

    // Interrupt the thread
    service.shutdownNow();

    assertTrue(
        service.awaitTermination(1, TimeUnit.MINUTES)
    );

    assertNotNull(exceptionRef.get());

    //ensure that even interrupted, connection status is when sending in bulk (all corresponding bulk send are read)
    ResultSet rs = sharedConnection.createStatement().executeQuery("SELECT 123456");
    assertTrue(rs.next());
    assertEquals(123456, rs.getInt(1));

    StringWriter writer = new StringWriter();
    exceptionRef.get().printStackTrace(new PrintWriter(writer));

    assertTrue(
        "Exception should be a SQLException: \n" + writer.toString(),
        exceptionRef.get() instanceof SQLException
    );

    assertTrue(wasInterrupted.get());

  }

  @Test
  public void serverBulk8mTest() throws SQLException {
    Assume.assumeTrue(checkMaxAllowedPacketMore8m("serverBulk8mTest"));
    Assume.assumeTrue(runLongTest);
    Assume.assumeFalse(sharedIsAurora());

    sharedConnection.createStatement().execute("TRUNCATE TABLE ExecuteBatchTest");

    try (Connection connection = setConnection(
        "&useComMulti=false&useBatchMultiSend=true&profileSql=" + profileSql)) {
      PreparedStatement preparedStatement = connection
          .prepareStatement("INSERT INTO ExecuteBatchTest(test, test2) values (?, ?)");
      //packet size : 7 200 068 kb
      addBatchData(preparedStatement, 60000, connection);
    }
  }

  @Test
  public void serverBulk20mTest() throws SQLException {
    Assume.assumeTrue(checkMaxAllowedPacketMore20m("serverBulk20mTest"));
    Assume.assumeTrue(runLongTest);
    Assume.assumeFalse(sharedIsAurora());

    sharedConnection.createStatement().execute("TRUNCATE TABLE ExecuteBatchTest");

    try (Connection connection = setConnection(
        "&useComMulti=false&useBatchMultiSend=true&profileSql=" + profileSql)) {
      PreparedStatement preparedStatement = connection
          .prepareStatement("INSERT INTO ExecuteBatchTest(test, test2) values (?, ?)");
      //packet size : 7 200 068 kb
      addBatchData(preparedStatement, 160000, connection);
    }
  }


  @Test
  public void serverStd8mTest() throws SQLException {
    Assume.assumeTrue(checkMaxAllowedPacketMore8m("serverStd8mTest"));
    Assume.assumeTrue(runLongTest);
    sharedConnection.createStatement().execute("TRUNCATE TABLE ExecuteBatchTest");

    try (Connection connection = setConnection(
        "&useComMulti=false&useBatchMultiSend=false&profileSql=" + profileSql)) {
      PreparedStatement preparedStatement = connection
          .prepareStatement("INSERT INTO ExecuteBatchTest(test, test2) values (?, ?)");
      addBatchData(preparedStatement, 60000, connection);
    }
  }

  @Test
  public void clientBulkTest() throws SQLException {
    Assume.assumeTrue(checkMaxAllowedPacketMore8m("serverStd8mTest"));
    Assume.assumeTrue(runLongTest);
    Assume.assumeFalse(sharedIsAurora());

    sharedConnection.createStatement().execute("TRUNCATE TABLE ExecuteBatchTest");

    try (Connection connection = setConnection(
        "&useComMulti=false&useBatchMultiSend=true&useServerPrepStmts=false&profileSql="
            + profileSql)) {
      PreparedStatement preparedStatement = connection
          .prepareStatement("INSERT INTO ExecuteBatchTest(test, test2) values (?, ?)");
      addBatchData(preparedStatement, 60000, connection);
    }
  }

  @Test
  public void clientRewriteValuesNotPossible8mTest() throws SQLException {
    Assume.assumeTrue(checkMaxAllowedPacketMore8m("clientRewriteValuesNotPossibleTest"));
    Assume.assumeTrue(runLongTest);
    sharedConnection.createStatement().execute("TRUNCATE TABLE ExecuteBatchTest");
    try (Connection connection = setConnection(
        "&rewriteBatchedStatements=true&profileSql=" + profileSql)) {
      PreparedStatement preparedStatement = connection.prepareStatement(
          "INSERT INTO ExecuteBatchTest(test, test2) values (?, ?) ON DUPLICATE KEY UPDATE id=?");
      addBatchData(preparedStatement, 60000, connection, true);
    }
  }


  @Test
  public void clientRewriteValuesNotPossible20mTest() throws SQLException {
    Assume.assumeTrue(checkMaxAllowedPacketMore8m("clientRewriteValuesNotPossibleTest"));
    Assume.assumeTrue(runLongTest);
    sharedConnection.createStatement().execute("TRUNCATE TABLE ExecuteBatchTest");
    try (Connection connection = setConnection(
        "&rewriteBatchedStatements=true&profileSql=" + profileSql)) {
      PreparedStatement preparedStatement = connection.prepareStatement(
          "INSERT INTO ExecuteBatchTest(test, test2) values (?, ?) ON DUPLICATE KEY UPDATE id=?");
      addBatchData(preparedStatement, 160000, connection, true);
    }
  }

  private void addBatchData(PreparedStatement preparedStatement, int batchNumber,
      Connection connection) throws SQLException {
    addBatchData(preparedStatement, batchNumber, connection, false);
  }

  private void addBatchData(PreparedStatement preparedStatement, int batchNumber,
      Connection connection, boolean additionnalParameter)
      throws SQLException {
    for (int i = 0; i < batchNumber; i++) {
      preparedStatement.setString(1, oneHundredLengthString);
      preparedStatement.setInt(2, i);
      if (additionnalParameter) {
        preparedStatement.setInt(3, i);
      }
      preparedStatement.addBatch();
    }
    int[] resultInsert = preparedStatement.executeBatch();

    //test result Size
    assertEquals(batchNumber, resultInsert.length);
    for (int i = 0; i < batchNumber; i++) {
      assertEquals(1, resultInsert[i]);
    }

    //check that connection is OK and results are well inserted
    ResultSet resultSet = connection.createStatement()
        .executeQuery("SELECT * FROM ExecuteBatchTest");
    for (int i = 0; i < batchNumber; i++) {
      assertTrue(resultSet.next());
      assertEquals(i + 1, resultSet.getInt(1));
      assertEquals(oneHundredLengthString, resultSet.getString(2));
      assertEquals(i, resultSet.getInt(3));
    }
    assertFalse(resultSet.next());
  }

  @Test
  public void useBatchMultiSend() throws Exception {
    Assume.assumeFalse(sharedIsAurora());
    try (Connection connection = setConnection("&useBatchMultiSend=true")) {
      String sql = "insert into ExecuteBatchUseBatchMultiSend (test) values (?)";
      try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
        for (int i = 0; i < 10; i++) {
          pstmt.setInt(1, i);
          pstmt.addBatch();
        }
        int[] updateCounts = pstmt.executeBatch();
        assertEquals(10, updateCounts.length);
        for (int updateCount : updateCounts) {
          if ((sharedIsRewrite()
              || (sharedOptions().useBulkStmts
              && isMariadbServer()
              && minVersion(10, 2)))) {
            assertEquals(Statement.SUCCESS_NO_INFO, updateCount);
          } else {
            assertEquals(1, updateCount);
          }
        }
      }
    }
  }

  /**
   * CONJ-553: handling RejectedExecutionException.
   *
   * @throws Exception if any error occur
   */
  @Test
  public void ensureBulkSchedulerMaxPoolSizeRejection() throws Throwable {
    Assume.assumeFalse(sharedIsAurora() || sharedOptions().profileSql);
    System.out.println(getProtocolFromConnection(sharedConnection).getHostAddress());

    Statement statement = sharedConnection.createStatement();
    ResultSet resultSet = statement.executeQuery("SELECT @@max_connections");
    assertTrue(resultSet.next());
    int maxConnection = resultSet.getInt(1);
    int limit = Math.min(1, Math.min(200, maxConnection - 10));
    System.out.println("limit:" + limit);
    for (int i = 0; i < limit; i++) {
      createTable("multipleSimultaneousBatch_" + i, "a INT NOT NULL");
    }

    final AtomicInteger counter = new AtomicInteger();
    ExecutorService exec = Executors.newFixedThreadPool(limit + 50);
    for (int i = 0; i < limit; i++) {
      exec.execute(new Runnable() {
        @Override
        public void run() {
          Connection connection = null;
          try {
            connection = setConnection();
            connection.setAutoCommit(false);
            Statement stmt = connection.createStatement();
            int connectionCounter = counter.getAndIncrement();
            for (int j = 0; j < 1024; j++) {
              stmt.addBatch("INSERT INTO multipleSimultaneousBatch_" + connectionCounter + "(a) VALUES (" + j + ")");
            }
            stmt.executeBatch();
            connection.commit();
          } catch (Throwable e) {
            e.printStackTrace();
          } finally {
            try {
              if (connection != null) connection.close();
            } catch (SQLException sqle) {
              sqle.printStackTrace();
            }
          }
        }
      });
    }

    exec.shutdown();
    exec.awaitTermination(150, TimeUnit.SECONDS);

    //check results
    Statement stmt = sharedConnection.createStatement();
    for (int i = 0; i < limit; i++) {
      ResultSet rs = stmt.executeQuery("SELECT count(*) from multipleSimultaneousBatch_" + i);
      assertTrue(rs.next());
      assertEquals(1024, rs.getInt(1));
    }
  }


  @Test
  public void useBatchMultiSendWithError() throws Exception {
    Assume.assumeFalse(sharedIsAurora());

    Iterator<Thread> it = Thread.getAllStackTraces().keySet().iterator();
    Thread thread;

    while (it.hasNext()) {
      thread = it.next();
      if (thread.getName().contains("MariaDb-bulk-")) {
        assertEquals(State.WAITING, thread.getState());
      }
    }

    Properties properties = new Properties();
    properties.setProperty("useBatchMultiSend", "true");
    try (Connection connection = createProxyConnection(properties)) {
      Statement stmt = connection.createStatement();
      stmt.execute("CREATE TEMPORARY TABLE useBatchMultiSendWithError (id INT NOT NULL,"
          + "UNIQUE INDEX `index1` (id))");
      String sql = "insert into useBatchMultiSendWithError (id) values (?)";
      try (PreparedStatement pstmt = connection.prepareStatement(sql)) {
        for (int i = 0; i < 200000; i++) {
          pstmt.setInt(1, i);
          pstmt.addBatch();
        }

        final ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1);
        executor.schedule(new Runnable() {
                           @Override
                           public void run() {
                             stopProxy();
                           }
                         }, 10, TimeUnit.MILLISECONDS);

        try {
          pstmt.executeBatch();
          fail();
        } catch (SQLException e) {
          Iterator<Thread> it2 = Thread.getAllStackTraces().keySet().iterator();
          Thread.sleep(500);
          Thread thread2;

          while (it2.hasNext()) {
             thread2 = it2.next();
            if (thread2.getName().contains("MariaDb-bulk-")) {
              assertEquals(State.WAITING, thread2.getState());
            }
          }
          restartProxy();
        }

      }
    }
  }

}
