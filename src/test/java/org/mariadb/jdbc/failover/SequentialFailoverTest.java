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

package org.mariadb.jdbc.failover;

import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.protocol.Protocol;
import org.mariadb.jdbc.internal.util.constant.HaMode;
import org.threadly.test.concurrent.TestableScheduler;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Test for sequential connection
 * exemple mvn test  -DdefaultGaleraUrl=jdbc:mariadb:sequential//localhost:3306,localhost:3307/test?user=root.
 */
public class SequentialFailoverTest extends BaseMultiHostTest {

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @BeforeClass()
    public static void beforeClass2() {
        proxyUrl = proxySequentialUrl;
        Assume.assumeTrue(initialGaleraUrl != null);
    }

    /**
     * Initialisation.
     *
     * @throws SQLException exception
     */
    @Before
    public void init() {
        defaultUrl = initialGaleraUrl;
        currentType = HaMode.SEQUENTIAL;
    }

    @Test
    public void connectionOrder() throws Throwable {

        Assume.assumeTrue(!initialGaleraUrl.contains("failover"));
        UrlParser urlParser = UrlParser.parse(initialGaleraUrl);
        for (int i = 0; i < urlParser.getHostAddresses().size(); i++) {
            int serverNb;
            try (Connection connection = getNewConnection(true)) {
                serverNb = getServerId(connection);
                assertTrue(serverNb == i + 1);
            }
            stopProxy(serverNb);
        }
    }

    @Test
    public void checkStaticBlacklist() throws Throwable {
        assureProxy();
        try (Connection connection = getNewConnection("&loadBalanceBlacklistTimeout=500", true)) {
            Statement st = connection.createStatement();

            int firstServerId = getServerId(connection);
            stopProxy(firstServerId);

            try {
                st.execute("show variables like 'slow_query_log'");
                fail();
            } catch (SQLException e) {
                //normal exception that permit to blacklist the failing connection.
            }

            //check blacklist size
            try {
                Protocol protocol = getProtocolFromConnection(connection);
                assertTrue(protocol.getProxy().getListener().getBlacklistKeys().size() == 1);

                //replace proxified HostAddress by normal one
                UrlParser urlParser = UrlParser.parse(defaultUrl);
                protocol.getProxy().getListener().addToBlacklist(urlParser.getHostAddresses().get(firstServerId - 1));
            } catch (Throwable e) {
                e.printStackTrace();
                fail();
            }

            //add first Host to blacklist
            Protocol protocol = getProtocolFromConnection(connection);
            TestableScheduler scheduler = new TestableScheduler();

            //check blacklist shared
            scheduler.execute(new CheckBlacklist(firstServerId, protocol.getProxy().getListener().getBlacklistKeys()));
            scheduler.execute(new CheckBlacklist(firstServerId, protocol.getProxy().getListener().getBlacklistKeys()));

            // deterministically execute CheckBlacklists
            scheduler.tick();
        }
    }

    @Test
    public void testMultiHostWriteOnMaster() throws Throwable {
        Assume.assumeTrue(initialGaleraUrl != null);
        try (Connection connection = getNewConnection()) {
            Statement stmt = connection.createStatement();
            stmt.execute("drop table  if exists multinode");
            stmt.execute("create table multinode (id int not null primary key auto_increment, test VARCHAR(10))");
        } catch (SQLException sqle) {
            fail("must have worked");
        }
    }

    @Test
    public void pingReconnectAfterRestart() throws Throwable {
        try (Connection connection = getNewConnection("&retriesAllDown=6", true)) {
            Statement st = connection.createStatement();
            int masterServerId = getServerId(connection);
            stopProxy(masterServerId);

            try {
                st.execute("SELECT 1");
            } catch (SQLException e) {
                //eat exception
            }
            restartProxy(masterServerId);
            long restartTime = System.nanoTime();
            boolean loop = true;
            while (loop) {
                if (!connection.isClosed()) {
                    loop = false;
                }
                long duration = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - restartTime);
                if (duration > 15 * 1000) {
                    fail();
                }
                Thread.sleep(250);
            }
        }
    }

    protected class CheckBlacklist implements Runnable {
        private final int firstServerId;
        private final Set<HostAddress> blacklistKeys;

        public CheckBlacklist(int firstServerId, Set<HostAddress> blacklistKeys) {
            this.firstServerId = firstServerId;
            this.blacklistKeys = blacklistKeys;
        }

        public void run() {
            try (Connection connection2 = getNewConnection()) {
                int otherServerId = getServerId(connection2);
                assertTrue(otherServerId != firstServerId);
                Protocol protocol = getProtocolFromConnection(connection2);
                assertTrue(blacklistKeys.toArray()[0].equals(protocol.getProxy().getListener()
                        .getBlacklistKeys().toArray()[0]));

            } catch (Throwable e) {
                e.printStackTrace();
                fail();
            }
        }
    }

    class MutableInt {
        int value = 1; // note that we start at 1 since we're counting

        public void increment() {
            ++value;
        }

        public int get() {
            return value;
        }
    }

}
