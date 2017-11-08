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

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import javax.sql.DataSource;
import java.sql.*;

public class ConnectionPoolTest extends BaseTest {

    /**
     * Tables initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void initClass() throws SQLException {
        for (int i = 0; i < 50; i++) {
            createTable("test_pool_batch" + i, "id int not null primary key auto_increment, test varchar(10)");
        }
    }

    @Test
    public void testBasicPool() throws SQLException {

        final HikariDataSource ds = new HikariDataSource();
        ds.setMaximumPoolSize(20);
        ds.setDriverClassName("org.mariadb.jdbc.Driver");
        ds.setJdbcUrl(connU);
        ds.addDataSourceProperty("user", username);
        if (password != null) ds.addDataSourceProperty("password", password);
        ds.setAutoCommit(false);
        Assert.assertTrue(validateDataSource(ds));
        ds.close();
    }

    @Test
    public void testPoolHikariCpWithConfig() throws SQLException {

        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(connU);
        config.setUsername(username);
        if (password != null) config.setPassword(password);
        config.addDataSourceProperty("cachePrepStmts", "true");
        config.addDataSourceProperty("prepStmtCacheSize", "250");
        config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048");
        try (HikariDataSource ds = new HikariDataSource(config)) {
            Assert.assertTrue(validateDataSource(ds));
        }

    }

    private boolean validateDataSource(DataSource ds) throws SQLException {
        try (Connection connection = ds.getConnection()) {
            try (Statement statement = connection.createStatement()) {
                try (ResultSet rs = statement.executeQuery("SELECT 1")) {
                    Assert.assertTrue(rs.next());
                    Assert.assertEquals(1, rs.getInt(1));
                    return true;
                }
            }
        }
    }

    private class InsertThread implements Runnable {
        private final DataSource dataSource;
        private final int insertNumber;
        private final int tableNumber;

        public InsertThread(int tableNumber, int insertNumber, DataSource dataSource) {
            this.insertNumber = insertNumber;
            this.tableNumber = tableNumber;
            this.dataSource = dataSource;
        }

        public void run() {

            try (Connection connection = dataSource.getConnection()) {
                PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO test_pool_batch"
                        + tableNumber + "(test) VALUES (?)");
                for (int i = 1; i < insertNumber; i++) {
                    preparedStatement.setString(1, i + "");
                    preparedStatement.addBatch();
                }
                preparedStatement.executeBatch();
            } catch (SQLException e) {
                Assert.fail("ERROR insert : " + e.getMessage());
            }
        }
    }
}
