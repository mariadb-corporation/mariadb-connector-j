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
 */

package org.mariadb.jdbc.authentication;

import org.mariadb.jdbc.internal.com.read.Buffer;
import org.mariadb.jdbc.internal.io.input.PacketInputStream;
import org.mariadb.jdbc.internal.io.output.PacketOutputStream;
import org.mariadb.jdbc.util.Options;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicInteger;

public interface AuthenticationPlugin {
  /**
   * Authentication plugin name.
   *
   * @return authentication plugin name. ex: Mysql native password
   */
  String name();

  /**
   * Authentication plugin type.
   *
   * @return authentication plugin type. ex: mysql_native_password
   */
  String type();

  /**
   * Indicate if use of this plugins need SSL enabled.
   *
   * @return true if SSL is mandatory
   */
  default boolean mustUseSsl() {
    return false;
  }

  /**
   * Plugin initialization.
   *
   * @param authenticationData authentication data (password/token)
   * @param seed server provided seed
   * @param options Connection string options
   */
  void initialize(String authenticationData, byte[] seed, Options options);

  /**
   * Process plugin authentication.
   *
   * @param out out stream
   * @param in in stream
   * @param sequence packet sequence
   * @return response packet
   * @throws IOException if socket error
   * @throws SQLException if plugin exception
   */
  Buffer process(PacketOutputStream out, PacketInputStream in, AtomicInteger sequence)
      throws IOException, SQLException;
}
