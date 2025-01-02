// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin.authentication.addon.gssapi;

import java.io.IOException;
import java.sql.SQLException;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.socket.Writer;

/** GSSAPI interface */
public interface GssapiAuth {

  /**
   * Authenticate
   *
   * @param writer socket writer
   * @param in socket reader
   * @param servicePrincipalName SPN
   * @param mechanisms mechanisms
   * @throws IOException if any socket error occurs
   * @throws SQLException for any other type of errors
   */
  void authenticate(Writer writer, Reader in, String servicePrincipalName, String mechanisms)
      throws IOException, SQLException;
}
