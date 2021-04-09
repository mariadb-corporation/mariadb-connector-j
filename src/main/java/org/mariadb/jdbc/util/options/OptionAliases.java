/*
 * Copyright (C) 2012-2020 MariaDB Corporation Ab
 *
 * This library is free software; you can redistribute it and/or modify it under the terms of the
 * GNU Lesser General Public License as published by the Free Software Foundation; either version
 * 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along with this
 * library; if not, write to the Free Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
 * Boston, MA 02110-1301 USA
 */

package org.mariadb.jdbc.util.options;

import java.util.HashMap;
import java.util.Map;

public final class OptionAliases {

  public static final Map<String, String> OPTIONS_ALIASES;

  static {
    OPTIONS_ALIASES = new HashMap<>();
    OPTIONS_ALIASES.put("enabledSSLCipherSuites", "enabledSslCipherSuites");
    OPTIONS_ALIASES.put("serverRSAPublicKeyFile", "serverRsaPublicKeyFile");
    OPTIONS_ALIASES.put("useSsl", "sslMode");
    OPTIONS_ALIASES.put("useSSL", "sslMode");
  }
}
