// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc.plugin;

import java.io.IOException;
import java.sql.SQLException;
import org.mariadb.jdbc.client.Context;
import org.mariadb.jdbc.client.ReadableByteBuf;
import org.mariadb.jdbc.client.socket.Reader;
import org.mariadb.jdbc.client.socket.Writer;

/** Authentication plugin descriptor */
public interface AuthenticationPlugin {

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
}
