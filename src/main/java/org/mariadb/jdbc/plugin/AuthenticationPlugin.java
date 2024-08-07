// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2024 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin;

import java.io.IOException;
import java.sql.SQLException;
import org.mariadb.jdbc.Configuration;
import org.mariadb.jdbc.HostAddress;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.socket.Writer;

/** Authentication plugin descriptor */
public interface AuthenticationPlugin {

  /**
   * Authentication plugin type.
   *
   * @return authentication plugin type. ex: mysql_native_password
   */
  String type();

  /**
   * Plugin initialization.
   *
   * @param authenticationData authentication data (password/token)
   * @param seed server provided seed
   * @param conf Connection options
   * @param hostAddress host address
   */
  void initialize(
      String authenticationData, byte[] seed, Configuration conf, HostAddress hostAddress);

  /**
   * Process plugin authentication.
   *
   * @param encoder out stream
   * @param decoder in stream
   * @param context connection context
   * @return response packet
   * @throws IOException if socket error
   * @throws SQLException if plugin exception
   */
  ReadableByteBuf process(Writer encoder, Reader decoder, Context context)
      throws IOException, SQLException;

  /**
   * Can plugins is MitM-proof, permitting returning HASH
   *
   * @return true if permitted
   */
  default boolean isMitMProof() {
    return false;
  }

  /**
   * Return Hash
   *
   * @param credential credential
   * @return hash
   */
  default byte[] hash(Credential credential) {
    return null;
  }

  /**
   * Authentication plugin required SSL to be used
   *
   * @return true if SSL is required
   */
  default boolean requireSsl() {
    return false;
  }
}
