// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package org.mariadb.jdbc.plugin.authentication.standard.gssapi;

import java.io.IOException;
import java.sql.SQLException;
import org.mariadb.jdbc.client.socket.PacketReader;
import org.mariadb.jdbc.client.socket.PacketWriter;

public interface GssapiAuth {

  void authenticate(
      PacketWriter writer, PacketReader in, String servicePrincipalName, String mechanisms)
      throws SQLException, IOException;
}
