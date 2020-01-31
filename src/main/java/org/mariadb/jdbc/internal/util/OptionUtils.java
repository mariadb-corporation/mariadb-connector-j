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

package org.mariadb.jdbc.internal.util;

import org.mariadb.jdbc.util.DefaultOptions;

import java.util.HashMap;
import java.util.Map;

public class OptionUtils {

  public static final Map<String, DefaultOptions> OPTIONS_MAP;

  static {
    OPTIONS_MAP = new HashMap<>();
    for (DefaultOptions defaultOption : DefaultOptions.values()) {
      OPTIONS_MAP.put(defaultOption.getOptionName(), defaultOption);
    }
    // add alias
    OPTIONS_MAP.put("createDB", DefaultOptions.CREATE_DATABASE_IF_NOT_EXISTS);
    OPTIONS_MAP.put("useSSL", DefaultOptions.USE_SSL);
    OPTIONS_MAP.put("profileSQL", DefaultOptions.PROFILE_SQL);
    OPTIONS_MAP.put("enabledSSLCipherSuites", DefaultOptions.ENABLED_SSL_CIPHER_SUITES);
    OPTIONS_MAP.put(
        "trustCertificateKeyStorePassword", DefaultOptions.TRUST_CERTIFICATE_KEYSTORE_PASSWORD);
    OPTIONS_MAP.put("trustCertificateKeyStoreUrl", DefaultOptions.TRUSTSTORE);
    OPTIONS_MAP.put("clientCertificateKeyStorePassword", DefaultOptions.KEYSTORE_PASSWORD);
    OPTIONS_MAP.put("clientCertificateKeyStoreUrl", DefaultOptions.KEYSTORE);
    OPTIONS_MAP.put("trustCertificateKeyStoreType", DefaultOptions.TRUST_STORE_TYPE);
    OPTIONS_MAP.put("clientCertificateKeyStoreType", DefaultOptions.KEY_STORE_TYPE);
  }
}
