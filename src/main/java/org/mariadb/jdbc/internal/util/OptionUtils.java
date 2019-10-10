package org.mariadb.jdbc.internal.util;

import org.mariadb.jdbc.util.*;

import java.util.*;

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
