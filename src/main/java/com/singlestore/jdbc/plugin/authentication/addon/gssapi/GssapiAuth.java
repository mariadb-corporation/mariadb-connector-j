// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab

package com.singlestore.jdbc.plugin.authentication.addon.gssapi;

import com.singlestore.jdbc.client.socket.PacketReader;
import com.singlestore.jdbc.client.socket.PacketWriter;
import java.io.IOException;
import java.sql.SQLException;

public interface GssapiAuth {

  void authenticate(
      PacketWriter writer, PacketReader in, String servicePrincipalName, String mechanisms)
      throws SQLException, IOException;
}
