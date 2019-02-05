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

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assume;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.MariaDbConnection;
import org.mariadb.jdbc.MariaDbPoolDataSource;
import org.mariadb.jdbc.UrlParser;
import org.mariadb.jdbc.internal.util.constant.HaMode;

/**
 * test for galera
 * The node must be configure with specific names :
 * node 1 : wsrep_node_name = "galera1"
 * ...
 * node x : wsrep_node_name = "galerax"
 * exemple mvn test  -DdbUrl=jdbc:mariadb://localhost:3306,localhost:3307/test?user=root
 */
public class GaleraFailoverTest extends SequentialFailoverTest {
  /**
   * Initialisation.
   */
  @BeforeClass()
  public static void beforeClass2() {
    proxyUrl = proxyGaleraUrl;
    Assume.assumeTrue(initialGaleraUrl != null);
  }

  /**
   * Initialisation.
   */
  @Before
  public void init() {
    defaultUrl = initialGaleraUrl;
    currentType = HaMode.FAILOVER;
  }


  @Test
  public void showRep() throws Exception {
    UrlParser urlParser = UrlParser.parse(initialGaleraUrl);
    List<HostAddress> initAddresses = urlParser.getHostAddresses();

    for (int i = 0; i < initAddresses.size(); i++) {
      urlParser.setHostAddresses(Arrays.asList(initAddresses.get(i)));
      Connection master = null;
      try {
        master = MariaDbConnection.newConnection(urlParser, null);
        Statement stmt = master.createStatement();
        ResultSet rs = stmt.executeQuery("show status like 'wsrep_local_state'");
        assertTrue(rs.next());
        System.out.println("host:" + initAddresses.get(i) + " status:" + rs.getString(2));
      } finally {
        if (master != null) {
          master.close();
        }
      }
    }

  }

  @Test
  public void validGaleraPing() throws Exception {
    long start = System.currentTimeMillis();
    MariaDbPoolDataSource pool = null;
    try {
      pool = new MariaDbPoolDataSource(initialGaleraUrl + "&maxPoolSize=1");
      Connection connection = null;
      try {
        connection = pool.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("SELECT 1 ");
      } finally {
        if (connection != null) {
          connection.close();
        }
      }
      Thread.sleep(2000);
      //Galera ping must occur
      connection = null;
      try {
        connection = pool.getConnection();
        Statement statement = connection.createStatement();
        statement.execute("SELECT 1 ");
      } finally {
        if (connection != null) {
          connection.close();
        }
      }
    } finally {
      pool.close();
    }
    //if fail, will loop until connectTimeout = 30s
    assertTrue(System.currentTimeMillis() - start < 5000);
  }

  @Test
  @Override
  public void connectionOrder() throws Throwable {
    Assume.assumeTrue(initialGaleraUrl.contains("failover"));
    Map<String, MutableInt> connectionMap = new HashMap<String, MutableInt>();
    for (int i = 0; i < 20; i++) {
      Connection connection = null;
      try {
        connection = getNewConnection(false);
        int serverId = getServerId(connection);
        MutableInt count = connectionMap.get(String.valueOf(serverId));
        if (count == null) {
          connectionMap.put(String.valueOf(serverId), new MutableInt());
        } else {
          count.increment();
        }
      } finally {
        if (connection != null) {
          connection.close();
        }
      }
    }

    assertTrue(connectionMap.size() >= 2);
    for (String key : connectionMap.keySet()) {
      Integer connectionCount = connectionMap.get(key).get();
      assertTrue(connectionCount > 1);
    }
  }

  @Test
  public void isValidGaleraConnection() throws SQLException {
    Connection connection = null;
    try {
      connection = getNewConnection(false);
      assertTrue(connection.isValid(0));
    } finally {
      if (connection != null) {
        connection.close();
      }
    }
  }
}
