package org.mariadb.jdbc.internal.util;

import java.util.HashMap;
import java.util.Map;

public class OptionUtils {

    public static final Map<String, DefaultOptions> OPTIONS_MAP;

    static {
        OPTIONS_MAP = new HashMap<String, DefaultOptions>();
        for (DefaultOptions defaultOption : DefaultOptions.values()) {
            OPTIONS_MAP.put(defaultOption.getOptionName(), defaultOption);
        }
        //add alias
        OPTIONS_MAP.put("createDB", DefaultOptions.CREATE_DATABASE_IF_NOT_EXISTS);
        OPTIONS_MAP.put("useSSL", DefaultOptions.USE_SSL);
        OPTIONS_MAP.put("profileSQL", DefaultOptions.PROFILE_SQL);
        OPTIONS_MAP.put("enabledSSLCipherSuites", DefaultOptions.ENABLED_SSL_CIPHER_SUITES);
        OPTIONS_MAP.put("trustCertificateKeyStorePassword", DefaultOptions.TRUST_CERTIFICATE_KEYSTORE_PASSWORD);
        OPTIONS_MAP.put("trustCertificateKeyStoreUrl", DefaultOptions.TRUSTSTORE);
        OPTIONS_MAP.put("clientCertificateKeyStorePassword", DefaultOptions.KEYSTORE_PASSWORD);
        OPTIONS_MAP.put("clientCertificateKeyStoreUrl", DefaultOptions.KEYSTORE);
    }
}
