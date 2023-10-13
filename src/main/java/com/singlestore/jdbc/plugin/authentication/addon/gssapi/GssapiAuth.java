// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.plugin.authentication.addon.gssapi;

import com.singlestore.jdbc.client.socket.Reader;
import com.singlestore.jdbc.client.socket.Writer;
import java.io.IOException;
import java.sql.SQLException;

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
  void authenticate(
      Writer writer,
      Reader in,
      String servicePrincipalName,
      String jaasApplicationName,
      String mechanisms)
      throws SQLException, IOException;
}
