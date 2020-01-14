/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2019 Christoph Läubrich
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

package org.mariadb.jdbc.internal.osgi;

import org.mariadb.jdbc.Driver;
import org.mariadb.jdbc.MariaDbDataSource;
import org.mariadb.jdbc.MariaDbPoolDataSource;
import org.osgi.service.jdbc.DataSourceFactory;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.DataSource;
import javax.sql.XADataSource;
import java.sql.SQLException;
import java.util.Properties;

public class MariaDbDataSourceFactory implements DataSourceFactory {

  @Override
  public DataSource createDataSource(Properties props) throws SQLException {
    if (props != null
        || props.containsKey(JDBC_MIN_POOL_SIZE)
        || props.containsKey(JDBC_MAX_POOL_SIZE)
        || props.containsKey(JDBC_MAX_IDLE_TIME)) {
      return createPoolDataSource(props);
    } else {
      return createBasicDataSource(props);
    }
  }

  @Override
  public ConnectionPoolDataSource createConnectionPoolDataSource(Properties props)
      throws SQLException {
    if (props != null
        || props.containsKey(JDBC_MIN_POOL_SIZE)
        || props.containsKey(JDBC_MAX_POOL_SIZE)
        || props.containsKey(JDBC_MAX_IDLE_TIME)) {
      return createPoolDataSource(props);
    } else {
      return createBasicDataSource(props);
    }
  }

  @Override
  public XADataSource createXADataSource(Properties props) throws SQLException {
    if (props != null
        || props.containsKey(JDBC_MIN_POOL_SIZE)
        || props.containsKey(JDBC_MAX_POOL_SIZE)
        || props.containsKey(JDBC_MAX_IDLE_TIME)) {
      return createPoolDataSource(props);
    } else {
      return createBasicDataSource(props);
    }
  }

  @Override
  public Driver createDriver(Properties props) throws SQLException {
    return new Driver();
  }

  private MariaDbDataSource createBasicDataSource(Properties props) throws SQLException {
    MariaDbDataSource dataSource = new MariaDbDataSource();

    if (props.containsKey(JDBC_URL)) {
      dataSource.setUrl(props.getProperty(JDBC_URL));
    }
    if (props.containsKey(JDBC_SERVER_NAME)) {
      dataSource.setServerName(props.getProperty(JDBC_SERVER_NAME));
    }
    if (props.containsKey(JDBC_PORT_NUMBER)) {
      try {
        dataSource.setPortNumber(Integer.parseInt(props.getProperty(JDBC_PORT_NUMBER)));
      } catch (NumberFormatException nfe) {
        throw new SQLException(
            "Port format must be integer, but value is '"
                + props.getProperty(JDBC_PORT_NUMBER)
                + "'");
      }
    }
    if (props.containsKey(JDBC_USER)) {
      dataSource.setUser(props.getProperty(JDBC_USER));
    }
    if (props.containsKey(JDBC_PASSWORD)) {
      dataSource.setPassword(props.getProperty(JDBC_PASSWORD));
    }
    if (props.containsKey(JDBC_DATABASE_NAME)) {
      dataSource.setDatabaseName(props.getProperty(JDBC_DATABASE_NAME));
    }
    return dataSource;
  }

  private MariaDbPoolDataSource createPoolDataSource(Properties props) throws SQLException {
    MariaDbPoolDataSource dataSource = new MariaDbPoolDataSource();
    if (props.containsKey(JDBC_URL)) {
      dataSource.setUrl(props.getProperty(JDBC_URL));
    }
    if (props.containsKey(JDBC_SERVER_NAME)) {
      dataSource.setServerName(props.getProperty(JDBC_SERVER_NAME));
    }
    if (props.containsKey(JDBC_PORT_NUMBER)) {
      try {
        dataSource.setPortNumber(Integer.parseInt(props.getProperty(JDBC_PORT_NUMBER)));
      } catch (NumberFormatException nfe) {
        throw new SQLException(
            "Port number format must be integer, but value is '"
                + props.getProperty(JDBC_PORT_NUMBER)
                + "'");
      }
    }
    if (props.containsKey(JDBC_USER)) {
      dataSource.setUser(props.getProperty(JDBC_USER));
    }
    if (props.containsKey(JDBC_PASSWORD)) {
      dataSource.setPassword(props.getProperty(JDBC_PASSWORD));
    }
    if (props.containsKey(JDBC_DATABASE_NAME)) {
      dataSource.setDatabaseName(props.getProperty(JDBC_DATABASE_NAME));
    }
    if (props.containsKey(JDBC_MAX_IDLE_TIME)) {
      try {
        dataSource.setMaxIdleTime(Integer.parseInt(props.getProperty(JDBC_MAX_IDLE_TIME)));
      } catch (NumberFormatException nfe) {
        throw new SQLException(
            "Max idle time format must be integer, but value is '"
                + props.getProperty(JDBC_MAX_IDLE_TIME)
                + "'");
      }
    }
    if (props.containsKey(JDBC_MAX_POOL_SIZE)) {
      try {
        dataSource.setMaxPoolSize(Integer.parseInt(props.getProperty(JDBC_MAX_POOL_SIZE)));
      } catch (NumberFormatException nfe) {
        throw new SQLException(
            "Max pool size format must be integer, but value is '"
                + props.getProperty(JDBC_MAX_POOL_SIZE)
                + "'");
      }
    }
    if (props.containsKey(JDBC_MIN_POOL_SIZE)) {
      try {
        dataSource.setMinPoolSize(Integer.parseInt(props.getProperty(JDBC_MIN_POOL_SIZE)));
      } catch (NumberFormatException nfe) {
        throw new SQLException(
            "Min pool size format must be integer, but value is '"
                + props.getProperty(JDBC_MIN_POOL_SIZE)
                + "'");
      }
    }
    return dataSource;
  }
}
