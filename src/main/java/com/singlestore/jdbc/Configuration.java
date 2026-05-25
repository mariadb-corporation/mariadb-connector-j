// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
// Copyright (c) 2021-2025 SingleStore, Inc.
package com.singlestore.jdbc;

import com.singlestore.jdbc.export.HaMode;
import com.singlestore.jdbc.export.SslMode;
import com.singlestore.jdbc.plugin.Codec;
import com.singlestore.jdbc.plugin.CredentialPlugin;
import com.singlestore.jdbc.plugin.credential.CredentialPluginLoader;
import com.singlestore.jdbc.util.log.Loggers;
import com.singlestore.jdbc.util.options.OptionAliases;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import org.ietf.jgss.GSSCredential;

/**
 * parse and verification of URL.
 *
 * <p>basic syntax :<br>
 * {@code
 * jdbc:singlestore:[sequential:|failover|loadbalance:]//<hostDescription>[,<hostDescription>]/[database>]
 * [?<key1>=<value1>[&<key2>=<value2>]] }
 *
 * <p>hostDescription:<br>
 * - simple :<br>
 * {@code <host>:<portnumber>}<br>
 * (for example localhost:3306)<br>
 * <br>
 * - complex :<br>
 * {@code address=[(port=<portnumber>)](host=<host>)}<br>
 * <br>
 * <br>
 * type is by default master<br>
 * port is by default 3306<br>
 *
 * <p>host can be dns name, ipv4 or ipv6.<br>
 * in case of ipv6 and simple host description, the ip must be written inside bracket.<br>
 * exemple : {@code jdbc:singlestore://[2001:0660:7401:0200:0000:0000:0edf:bdd7]:3306}<br>
 *
 * <p>Some examples :<br>
 * {@code jdbc:singlestore://localhost:3306/database?user=greg&password=pass}<br>
 * {@code
 * jdbc:singlestore://address=(port=3306)(host=master1),address=(port=3307)(host=child1)/database?user=greg&password=pass}
 * <br>
 */
public class Configuration {

  private static final Set<String> EXCLUDED_FIELDS;
  private static final Set<String> SECURE_FIELDS;
  private static final Set<String> PROPERTIES_TO_SKIP;
  private static final Set<String> SENSITIVE_FIELDS;

  static {
    EXCLUDED_FIELDS = new HashSet<>();
    EXCLUDED_FIELDS.add("database");
    EXCLUDED_FIELDS.add("haMode");
    EXCLUDED_FIELDS.add("$jacocoData");
    EXCLUDED_FIELDS.add("addresses");
    EXCLUDED_FIELDS.add("transactionIsolation");
    EXCLUDED_FIELDS.add("gssCredential");

    SECURE_FIELDS = new HashSet<>();
    SECURE_FIELDS.add("password");
    SECURE_FIELDS.add("keyStorePassword");
    SECURE_FIELDS.add("trustStorePassword");

    PROPERTIES_TO_SKIP = new HashSet<>();
    PROPERTIES_TO_SKIP.add("initialUrl");
    PROPERTIES_TO_SKIP.add("logger");
    PROPERTIES_TO_SKIP.add("codecs");
    PROPERTIES_TO_SKIP.add("$jacocoData");
    PROPERTIES_TO_SKIP.add("gssCredential");

    SENSITIVE_FIELDS = new HashSet<>();
    SENSITIVE_FIELDS.add("password");
    SENSITIVE_FIELDS.add("keyStorePassword");
    SENSITIVE_FIELDS.add("trustStorePassword");
  }

  // standard options
  private String user;
  private String password;
  private String database;
  private List<HostAddress> addresses;
  private HaMode haMode;
  private String initialUrl;
  private Properties nonMappedOptions;

  // various
  private Boolean autocommit;
  private boolean useMysqlMetadata;
  private boolean useMysqlVersion;
  private boolean nullDatabaseMeansCurrent;
  private boolean createDatabaseIfNotExist;
  private String initSql;
  private TransactionIsolation transactionIsolation;
  private int defaultFetchSize;
  private Integer maxAllowedPacket;
  private String geometryDefaultType;
  private String restrictedAuth;

  // socket
  private String socketFactory;
  private int connectTimeout;
  private String pipe;
  private String localSocket;
  private boolean tcpKeepAlive;
  private int tcpKeepIdle;
  private int tcpKeepCount;
  private int tcpKeepInterval;
  private boolean tcpAbortiveClose;
  private String localSocketAddress;
  private int socketTimeout;
  private String socksProxyHost;
  private Integer socksProxyPort;
  private boolean useReadAheadInput;
  private String tlsSocketType;

  // SSL
  private SslMode sslMode;
  private String serverSslCert;
  private String trustStore;
  private String trustStorePassword;
  private String trustStoreType;
  private String keyStore;
  private String keyStorePassword;
  private String keyPassword;
  private String keyStoreType;
  private String enabledSslCipherSuites;
  private String enabledSslProtocolSuites;
  private String hostNameInCertificate;

  // protocol
  private boolean allowMultiQueries;
  private boolean allowLocalInfile;
  private boolean useCompression;
  private boolean useAffectedRows;
  private boolean disablePipeline;

  // prepare
  private boolean cachePrepStmts;
  private int prepStmtCacheSize;
  private boolean useServerPrepStmts;
  private boolean rewriteBatchedStatements;
  private boolean preserveInstants;

  // authentication
  private CredentialPlugin credentialType;
  private String sessionVariables;
  private String connectionAttributes;
  private String servicePrincipalName;
  private String jaasApplicationName;
  private Boolean cacheJaasLoginContext;
  private GSSCredential gssCredential;
  private boolean requestCredentialDelegation;

  // meta
  private boolean blankTableNameMeta;
  private boolean tinyInt1isBit;
  private boolean transformedBitIsBoolean;
  private boolean yearIsDateType;
  private boolean dumpQueriesOnException;
  private boolean includeThreadDumpInDeadlockExceptions;

  // HA options
  private int retriesAllDown;
  private boolean transactionReplay;
  private int transactionReplaySize;

  // Pool options
  private boolean pool;
  private String poolName;
  private int maxPoolSize;
  private int minPoolSize;
  private int maxIdleTime;
  private boolean registerJmxPool;
  private int poolValidMinDelay;
  private boolean useResetConnection;

  // Logging
  private int maxQuerySizeToLog;
  private String consoleLogLevel;
  private String consoleLogFilepath;
  private boolean printStackTrace;
  private Integer maxPrintStackSizeToLog;

  // Extended data types e.g. VECTOR, BSON
  private boolean enableExtendedDataTypes;
  private String vectorTypeOutputFormat;
  private boolean vectorExtendedMetadata;

  private Codec<?>[] codecs;

  private Configuration(Builder builder) {
    // Set basic configuration
    try {
      initializeBasicConfig(builder);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }

    // Set SSL/TLS configuration
    initializeSslConfig(builder);

    // Set socket configuration
    initializeSocketConfig(builder);

    // Set transaction and protocol settings
    initializeTransactionConfig(builder);

    // Set data type handling
    initializeDataTypeConfig(builder);

    // Set query and statement handling
    initializeQueryConfig(builder);

    // Set pipeline and transaction settings
    initializePipelineConfig(builder);

    // Set database and schema settings
    initializeDatabaseConfig(builder);

    // Set exception handling
    initializeExceptionConfig(builder);

    // Set pool configuration
    initializePoolConfig(builder);

    // Set logging configuration
    initializeLoggingConfig(builder);

    // Set extended type configuration
    initializeExtendedTypesConfig(builder);

    // Set additional properties
    initializeAdditionalConfig(builder);

    // Validate configuration
    validateConfiguration();
  }

  private void initializeBasicConfig(Builder builder) throws SQLException {
    this.database = builder.database;
    this.addresses = builder._addresses;
    this.nonMappedOptions = builder._nonMappedOptions;
    this.haMode = builder._haMode != null ? builder._haMode : HaMode.NONE;
    this.credentialType = CredentialPluginLoader.get(builder.credentialType);
    this.user = builder.user;
    this.password = builder.password;
  }

  private void initializeSslConfig(Builder builder) {
    this.enabledSslProtocolSuites = builder.enabledSslProtocolSuites;
    this.hostNameInCertificate = builder.hostNameInCertificate;
    this.serverSslCert = builder.serverSslCert;
    this.keyStore = builder.keyStore;
    this.trustStore = builder.trustStore;
    this.keyStorePassword = builder.keyStorePassword;
    this.trustStorePassword = builder.trustStorePassword;
    this.keyPassword = builder.keyPassword;
    this.keyStoreType = builder.keyStoreType;
    this.trustStoreType = builder.trustStoreType;
    // SSL Mode configuration
    if (this.credentialType != null
        && this.credentialType.mustUseSsl()
        && (builder.sslMode == null || SslMode.from(builder.sslMode) == SslMode.DISABLE)) {
      Loggers.getLogger(Configuration.class)
          .warn(
              "Credential type '"
                  + this.credentialType.type()
                  + "' is required to be used with SSL. "
                  + "Enabling SSL.");
      this.sslMode = SslMode.VERIFY_FULL;
    } else {
      this.sslMode = builder.sslMode != null ? SslMode.from(builder.sslMode) : SslMode.DISABLE;
    }
  }

  private void initializeSocketConfig(Builder builder) {
    this.socketFactory = builder.socketFactory;
    this.connectTimeout =
        builder.connectTimeout != null
            ? builder.connectTimeout
            : (DriverManager.getLoginTimeout() > 0
                ? DriverManager.getLoginTimeout() * 1000
                : 30_000);
    this.pipe = builder.pipe;
    this.localSocket = builder.localSocket;
    this.tcpKeepAlive = builder.tcpKeepAlive == null || builder.tcpKeepAlive;
    this.tcpKeepIdle = builder.tcpKeepIdle != null ? builder.tcpKeepIdle : 0;
    this.tcpKeepCount = builder.tcpKeepCount != null ? builder.tcpKeepCount : 0;
    this.tcpKeepInterval = builder.tcpKeepInterval != null ? builder.tcpKeepInterval : 0;
    this.tcpAbortiveClose = builder.tcpAbortiveClose != null && builder.tcpAbortiveClose;
    this.localSocketAddress = builder.localSocketAddress;
    this.socketTimeout = builder.socketTimeout != null ? builder.socketTimeout : 0;
    this.socksProxyHost = builder.socksProxyHost;
    this.socksProxyPort = builder.socksProxyPort;
    this.useReadAheadInput = builder.useReadAheadInput != null && builder.useReadAheadInput;
    this.tlsSocketType = builder.tlsSocketType;
    this.useCompression = builder.useCompression != null && builder.useCompression;
  }

  private void initializeTransactionConfig(Builder builder) {
    if (builder.transactionIsolation != null) {
      if (TransactionIsolation.from(builder.transactionIsolation)
          != TransactionIsolation.READ_COMMITTED) {
        throw new IllegalArgumentException(
            "Currently, the 'Read Committed' is the only isolation level that is supported in SingleStore.");
      }
    }
    this.transactionIsolation = TransactionIsolation.READ_COMMITTED;
    this.enabledSslCipherSuites = builder.enabledSslCipherSuites;
    this.sessionVariables = builder.sessionVariables;
  }

  private void initializeDataTypeConfig(Builder builder) {
    this.tinyInt1isBit = builder.tinyInt1isBit == null || builder.tinyInt1isBit;
    this.transformedBitIsBoolean =
        builder.transformedBitIsBoolean != null && builder.transformedBitIsBoolean;
    this.yearIsDateType = builder.yearIsDateType == null || builder.yearIsDateType;
  }

  private void initializeQueryConfig(Builder builder) {
    this.dumpQueriesOnException =
        builder.dumpQueriesOnException != null && builder.dumpQueriesOnException;
    this.prepStmtCacheSize = builder.prepStmtCacheSize != null ? builder.prepStmtCacheSize : 250;
    this.useAffectedRows = builder.useAffectedRows != null && builder.useAffectedRows;
    this.useServerPrepStmts = builder.useServerPrepStmts != null && builder.useServerPrepStmts;
    this.rewriteBatchedStatements =
        builder.rewriteBatchedStatements != null && builder.rewriteBatchedStatements;
    this.preserveInstants = builder.preserveInstants != null && builder.preserveInstants;
    this.connectionAttributes = builder.connectionAttributes;
    this.allowLocalInfile = builder.allowLocalInfile == null || builder.allowLocalInfile;
    this.allowMultiQueries = builder.allowMultiQueries != null && builder.allowMultiQueries;
  }

  private void initializePipelineConfig(Builder builder) {
    this.disablePipeline = builder.disablePipeline != null && builder.disablePipeline;
    this.autocommit = builder.autocommit;
    this.useMysqlMetadata = builder.useMysqlMetadata != null && builder.useMysqlMetadata;
    this.useMysqlVersion = builder.useMysqlVersion != null && builder.useMysqlVersion;
    this.nullDatabaseMeansCurrent =
        builder.nullDatabaseMeansCurrent != null && builder.nullDatabaseMeansCurrent;
  }

  private void initializeDatabaseConfig(Builder builder) {
    this.createDatabaseIfNotExist =
        builder.createDatabaseIfNotExist != null && builder.createDatabaseIfNotExist;
    this.blankTableNameMeta = builder.blankTableNameMeta != null && builder.blankTableNameMeta;
  }

  private void initializeExceptionConfig(Builder builder) {
    this.includeThreadDumpInDeadlockExceptions =
        builder.includeThreadDumpInDeadlockExceptions != null
            && builder.includeThreadDumpInDeadlockExceptions;
  }

  private void initializePoolConfig(Builder builder) {
    this.pool = builder.pool != null && builder.pool;
    this.poolName = builder.poolName;
    this.maxPoolSize = builder.maxPoolSize != null ? builder.maxPoolSize : 8;
    this.minPoolSize = builder.minPoolSize != null ? builder.minPoolSize : this.maxPoolSize;
    if (builder.maxIdleTime != null) {
      if (builder.maxIdleTime < 2) {
        throw new IllegalArgumentException(
            String.format(
                "Wrong argument value '%d' for maxIdleTime, must be >= 2", builder.maxIdleTime));
      }
      this.maxIdleTime = builder.maxIdleTime;
    } else {
      this.maxIdleTime = 600_000;
    }
    this.registerJmxPool = builder.registerJmxPool == null || builder.registerJmxPool;
    this.poolValidMinDelay = builder.poolValidMinDelay != null ? builder.poolValidMinDelay : 1000;
    this.useResetConnection = builder.useResetConnection != null && builder.useResetConnection;
  }

  private void initializeLoggingConfig(Builder builder) {
    this.maxQuerySizeToLog = builder.maxQuerySizeToLog != null ? builder.maxQuerySizeToLog : 1024;
    this.consoleLogLevel = builder.consoleLogLevel;
    this.consoleLogFilepath = builder.consoleLogFilepath;
    this.printStackTrace = builder.printStackTrace != null && builder.printStackTrace;
    this.maxPrintStackSizeToLog =
        builder.maxPrintStackSizeToLog != null ? builder.maxPrintStackSizeToLog : 10;
  }

  private void initializeExtendedTypesConfig(Builder builder) {
    this.enableExtendedDataTypes =
        builder.enableExtendedDataTypes != null && builder.enableExtendedDataTypes;
    if (builder.vectorTypeOutputFormat != null) {
      String format = builder.vectorTypeOutputFormat.toUpperCase().trim();
      if (!"JSON".equals(format) && !"BINARY".equals(format)) {
        throw new IllegalArgumentException(
            "Invalid 'vectorTypeOutputFormat' parameter: '"
                + format
                + "'. Expected values are 'JSON' or 'BINARY'.");
      }
      this.vectorTypeOutputFormat = format;
    }
    this.vectorExtendedMetadata =
        builder.vectorExtendedMetadata != null && builder.vectorExtendedMetadata;
  }

  private void initializeAdditionalConfig(Builder builder) {
    this.servicePrincipalName = builder.servicePrincipalName;
    this.jaasApplicationName = builder.jaasApplicationName;
    this.cacheJaasLoginContext = builder.cacheJaasLoginContext;
    this.gssCredential = builder.gssCredential;
    this.requestCredentialDelegation =
        builder.requestCredentialDelegation != null && builder.requestCredentialDelegation;
    this.defaultFetchSize = builder.defaultFetchSize != null ? builder.defaultFetchSize : 0;
    this.tlsSocketType = builder.tlsSocketType;
    this.maxAllowedPacket = builder.maxAllowedPacket;
    this.retriesAllDown = builder.retriesAllDown != null ? builder.retriesAllDown : 120;
    this.cachePrepStmts = builder.cachePrepStmts == null || builder.cachePrepStmts;
    this.transactionReplay = builder.transactionReplay != null && builder.transactionReplay;
    this.transactionReplaySize =
        builder.transactionReplaySize != null ? builder.transactionReplaySize : 64;
    this.geometryDefaultType = builder.geometryDefaultType;
    this.restrictedAuth = builder.restrictedAuth;
    this.initSql = builder.initSql;
    this.codecs = null;
  }

  private void validateConfiguration() {
    // Validate integer fields
    validateIntegerFields();
    validateProxyConfiguration();
  }

  private void validateProxyConfiguration() {
    if (socksProxyPort != null && socksProxyHost == null) {
      throw new IllegalArgumentException("socksProxyPort requires socksProxyHost to be set.");
    }
  }

  private void validateIntegerFields() {
    Field[] fields = Configuration.class.getDeclaredFields();
    try {
      for (Field field : fields) {
        if (field.getType().equals(int.class)) {
          int val = field.getInt(this);
          if (val < 0) {
            throw new IllegalArgumentException(
                String.format("Value for %s must be >= 1 (value is %s)", field.getName(), val));
          }
        }
      }
    } catch (IllegalAccessException ie) {
      // Ignore reflection errors
    }
  }

  /**
   * Create a Builder from current configuration. Since configuration data are final, this permit to
   * change configuration, creating another object.
   *
   * @return builder
   */
  public Builder toBuilder() {
    Builder builder =
        new Builder()
            .user(this.user)
            .password(this.password)
            .database(this.database)
            .addresses(this.addresses == null ? null : this.addresses.toArray(new HostAddress[0]))
            .haMode(this.haMode)
            .autocommit(this.autocommit)
            .useMysqlMetadata(this.useMysqlMetadata)
            .useMysqlVersion(this.useMysqlVersion)
            .nullDatabaseMeansCurrent(this.nullDatabaseMeansCurrent)
            .createDatabaseIfNotExist(this.createDatabaseIfNotExist)
            .transactionIsolation(
                transactionIsolation == null ? null : this.transactionIsolation.getValue())
            .defaultFetchSize(this.defaultFetchSize)
            .maxQuerySizeToLog(this.maxQuerySizeToLog)
            .maxAllowedPacket(this.maxAllowedPacket)
            .geometryDefaultType(this.geometryDefaultType)
            .geometryDefaultType(this.geometryDefaultType)
            .restrictedAuth(this.restrictedAuth)
            .initSql(this.initSql)
            .socketFactory(this.socketFactory)
            .connectTimeout(this.connectTimeout)
            .pipe(this.pipe)
            .localSocket(this.localSocket)
            .tcpKeepAlive(this.tcpKeepAlive)
            .tcpKeepIdle(this.tcpKeepIdle)
            .tcpKeepCount(this.tcpKeepCount)
            .tcpKeepInterval(this.tcpKeepInterval)
            .tcpAbortiveClose(this.tcpAbortiveClose)
            .localSocketAddress(this.localSocketAddress)
            .socketTimeout(this.socketTimeout)
            .socksProxyHost(this.socksProxyHost)
            .socksProxyPort(this.socksProxyPort)
            .useReadAheadInput(this.useReadAheadInput)
            .tlsSocketType(this.tlsSocketType)
            .sslMode(this.sslMode.name())
            .serverSslCert(this.serverSslCert)
            .keyStore(this.keyStore)
            .trustStore(this.trustStore)
            .keyStoreType(this.keyStoreType)
            .keyStorePassword(this.keyStorePassword)
            .trustStorePassword(this.trustStorePassword)
            .keyPassword(this.keyPassword)
            .trustStoreType(this.trustStoreType)
            .enabledSslCipherSuites(this.enabledSslCipherSuites)
            .enabledSslProtocolSuites(this.enabledSslProtocolSuites)
            .hostNameInCertificate(this.hostNameInCertificate)
            .allowMultiQueries(this.allowMultiQueries)
            .allowLocalInfile(this.allowLocalInfile)
            .useCompression(this.useCompression)
            .useAffectedRows(this.useAffectedRows)
            .rewriteBatchedStatements(this.rewriteBatchedStatements)
            .preserveInstants(this.preserveInstants)
            .disablePipeline(this.disablePipeline)
            .cachePrepStmts(this.cachePrepStmts)
            .prepStmtCacheSize(this.prepStmtCacheSize)
            .useServerPrepStmts(this.useServerPrepStmts)
            .credentialType(this.credentialType == null ? null : this.credentialType.type())
            .sessionVariables(this.sessionVariables)
            .connectionAttributes(this.connectionAttributes)
            .servicePrincipalName(this.servicePrincipalName)
            .jaasApplicationName(this.jaasApplicationName)
            .cacheJaasLoginContext(this.cacheJaasLoginContext)
            .gssCredential(this.gssCredential)
            .requestCredentialDelegation(this.requestCredentialDelegation)
            .blankTableNameMeta(this.blankTableNameMeta)
            .tinyInt1isBit(this.tinyInt1isBit)
            .transformedBitIsBoolean(this.transformedBitIsBoolean)
            .yearIsDateType(this.yearIsDateType)
            .dumpQueriesOnException(this.dumpQueriesOnException)
            .includeThreadDumpInDeadlockExceptions(this.includeThreadDumpInDeadlockExceptions)
            .retriesAllDown(this.retriesAllDown)
            .transactionReplay(this.transactionReplay)
            .transactionReplaySize(this.transactionReplaySize)
            .pool(this.pool)
            .poolName(this.poolName)
            .maxPoolSize(this.maxPoolSize)
            .minPoolSize(this.minPoolSize)
            .maxIdleTime(this.maxIdleTime)
            .registerJmxPool(this.registerJmxPool)
            .poolValidMinDelay(this.poolValidMinDelay)
            .useResetConnection(this.useResetConnection)
            .consoleLogLevel(this.consoleLogLevel)
            .consoleLogFilepath(this.consoleLogFilepath)
            .printStackTrace(this.printStackTrace)
            .maxPrintStackSizeToLog(this.maxPrintStackSizeToLog)
            .enableExtendedDataTypes(this.enableExtendedDataTypes)
            .vectorTypeOutputFormat(this.vectorTypeOutputFormat)
            .vectorExtendedMetadata(this.vectorExtendedMetadata);
    builder._nonMappedOptions = this.nonMappedOptions;
    return builder;
  }

  /**
   * Tell if the driver accepts url string. (Correspond to interface java.jdbc.Driver.acceptsURL()
   * method)
   *
   * @param url url String
   * @return true if url string correspond.
   */
  public static boolean acceptsUrl(String url) {
    return url != null && url.startsWith("jdbc:singlestore:");
  }

  public static Configuration parse(final String url) throws SQLException {
    return parse(url, new Properties());
  }

  /**
   * Parse url connection string with additional properties.
   *
   * @param url connection string
   * @param prop properties
   * @return UrlParser instance
   * @throws SQLException if parsing exception occur
   */
  public static Configuration parse(final String url, Properties prop) throws SQLException {
    if (acceptsUrl(url)) {
      return parseInternal(url, (prop == null) ? new Properties() : prop);
    }
    return null;
  }

  /**
   * Parses the connection URL in order to set the UrlParser instance with all the information
   * provided through the URL.
   *
   * @param url connection URL
   * @param properties properties
   * @throws SQLException if format is incorrect
   */
  private static Configuration parseInternal(String url, Properties properties)
      throws SQLException {
    try {
      Builder builder = new Builder();

      // Validate and parse basic URL structure
      validateUrlFormat(url);
      int separator = url.indexOf("//");
      builder.haMode(parseHaMode(url, separator));

      // Extract host and parameters sections
      String urlSecondPart = url.substring(separator + 2);

      // Skip complex address definitions
      int posToSkip = skipComplexAddresses(urlSecondPart);
      int dbIndex = urlSecondPart.indexOf("/", posToSkip);
      int paramIndex = urlSecondPart.indexOf("?");

      // parse address and additional parameter parts
      String hostAddressesString;
      String additionalParameters;
      if ((dbIndex < paramIndex && dbIndex < 0) || (dbIndex > paramIndex && paramIndex > -1)) {
        hostAddressesString = urlSecondPart.substring(0, paramIndex);
        additionalParameters = urlSecondPart.substring(paramIndex);
      } else if (dbIndex < paramIndex || dbIndex > paramIndex) {
        hostAddressesString = urlSecondPart.substring(0, dbIndex);
        additionalParameters = urlSecondPart.substring(dbIndex);
      } else {
        hostAddressesString = urlSecondPart;
        additionalParameters = null;
      }

      // Process database and parameters if present
      if (additionalParameters != null) {
        processDatabaseAndParameters(additionalParameters, builder, properties);
      } else {
        builder.database(null);
      }

      // Map properties to configuration options
      mapPropertiesToOption(builder, properties);

      // Parse host addresses
      builder._addresses = HostAddress.parse(hostAddressesString, builder._haMode);

      return builder.build();

    } catch (IllegalArgumentException i) {
      throw new SQLException("error parsing url: " + i.getMessage(), i);
    }
  }

  private static void validateUrlFormat(String url) {
    int separator = url.indexOf("//");
    if (separator == -1) {
      throw new IllegalArgumentException(
          "url parsing error : '//' is not present in the url " + url);
    }
  }

  private static int skipComplexAddresses(String urlSecondPart) {
    int posToSkip = 0;
    int skipPos;
    while ((skipPos = urlSecondPart.indexOf("address=(", posToSkip)) > -1) {
      posToSkip = urlSecondPart.indexOf(")", skipPos) + 1;
      while (urlSecondPart.startsWith("(", posToSkip)) {
        int endingBraceIndex = urlSecondPart.indexOf(")", posToSkip);
        if (endingBraceIndex == -1) break;
        posToSkip = endingBraceIndex + 1;
      }
    }
    return posToSkip;
  }

  private static void processDatabaseAndParameters(
      String additionalParameters, Builder builder, Properties properties) {

    int optIndex = additionalParameters.indexOf("?");

    // Extract database name
    String database;
    if (optIndex < 0) {
      database = (additionalParameters.length() > 1) ? additionalParameters.substring(1) : null;
    } else {
      database = extractDatabase(additionalParameters, optIndex);
      processUrlParameters(additionalParameters.substring(optIndex + 1), properties);
    }

    builder.database(database);
  }

  private static String extractDatabase(String additionalParameters, int optIndex) {
    if (optIndex == 0) {
      return null;
    }
    String database = additionalParameters.substring(1, optIndex);
    return database.isEmpty() ? null : database;
  }

  private static void processUrlParameters(String urlParameters, Properties properties) {
    if (!urlParameters.isEmpty()) {
      String[] parameters = urlParameters.split("&");
      for (String parameter : parameters) {
        int pos = parameter.indexOf('=');
        if (pos == -1) {
          properties.setProperty(parameter, "");
        } else {
          properties.setProperty(parameter.substring(0, pos), parameter.substring(pos + 1));
        }
      }
    }
  }

  private static void mapPropertiesToOption(Builder builder, Properties properties) {
    Properties nonMappedOptions = new Properties();

    try {
      processProperties(builder, properties, nonMappedOptions);
      handleLegacySslSettings(builder, nonMappedOptions);
      builder._nonMappedOptions = nonMappedOptions;
    } catch (ReflectiveOperationException e) {
      throw new IllegalArgumentException("Unexpected error while mapping properties", e);
    }
  }

  private static void processProperties(
      Builder builder, Properties properties, Properties nonMappedOptions)
      throws ReflectiveOperationException {

    for (final Map.Entry<Object, Object> entry : properties.entrySet()) {
      String realKey = getRealKey(entry.getKey().toString());
      final Object propertyValue = entry.getValue();

      if (propertyValue != null) {
        processProperty(builder, realKey, propertyValue, entry.getKey(), nonMappedOptions);
      }
    }
  }

  private static String getRealKey(String key) {
    String lowercaseKey = key.toLowerCase(Locale.ROOT);
    String realKey = OptionAliases.OPTIONS_ALIASES.get(lowercaseKey);
    return realKey != null ? realKey : key;
  }

  private static void processProperty(
      Builder builder,
      String realKey,
      Object propertyValue,
      Object originalKey,
      Properties nonMappedOptions)
      throws ReflectiveOperationException {

    if ("gssCredential".equalsIgnoreCase(realKey)) {
      if (propertyValue instanceof GSSCredential) {
        builder.gssCredential((GSSCredential) propertyValue);
      } else {
        throw new IllegalArgumentException(
            "Optional parameter gssCredential must be a GSSCredential instance");
      }
      return;
    }

    boolean used = false;
    for (Field field : Builder.class.getDeclaredFields()) {
      if (realKey.toLowerCase(Locale.ROOT).equals(field.getName().toLowerCase(Locale.ROOT))) {
        used = true;
        setFieldValue(builder, field, propertyValue, originalKey);
      }
    }
    if (!used) {
      nonMappedOptions.put(realKey, propertyValue);
    }
  }

  private static void setFieldValue(
      Builder builder, Field field, Object propertyValue, Object originalKey)
      throws ReflectiveOperationException {

    if (field.getGenericType().equals(String.class)) {
      handleStringField(builder, field, propertyValue);
    } else if (field.getGenericType().equals(Boolean.class)) {
      handleBooleanField(builder, field, propertyValue, originalKey);
    } else if (field.getGenericType().equals(Integer.class)) {
      handleIntegerField(builder, field, propertyValue, originalKey);
    }
  }

  private static void handleStringField(Builder builder, Field field, Object value)
      throws ReflectiveOperationException {
    String stringValue = value.toString();
    if (!stringValue.isEmpty()) {
      Method method = Builder.class.getDeclaredMethod(field.getName(), String.class);
      method.invoke(builder, stringValue);
    }
  }

  private static void handleBooleanField(
      Builder builder, Field field, Object value, Object originalKey)
      throws ReflectiveOperationException {

    Method method = Builder.class.getDeclaredMethod(field.getName(), Boolean.class);
    switch (value.toString().toLowerCase()) {
      case "":
      case "1":
      case "true":
        method.invoke(builder, Boolean.TRUE);
        break;
      case "0":
      case "false":
        method.invoke(builder, Boolean.FALSE);
        break;
      default:
        throw new IllegalArgumentException(
            String.format(
                "Optional parameter %s must be boolean (true/false or 0/1) was '%s'",
                originalKey, value));
    }
  }

  private static void handleIntegerField(
      Builder builder, Field field, Object value, Object originalKey)
      throws ReflectiveOperationException {

    try {
      Method method = Builder.class.getDeclaredMethod(field.getName(), Integer.class);
      final Integer intValue = Integer.parseInt(value.toString());
      method.invoke(builder, intValue);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException(
          String.format("Optional parameter %s must be Integer, was '%s'", originalKey, value));
    }
  }

  private static void handleLegacySslSettings(Builder builder, Properties nonMappedOptions) {
    if (isSet("useSsl", nonMappedOptions) || isSet("useSSL", nonMappedOptions)) {
      Properties deprecatedDesc = new Properties();
      try (InputStream inputStream =
          Driver.class.getClassLoader().getResourceAsStream("deprecated.properties")) {
        deprecatedDesc.load(inputStream);
        Loggers.getLogger(Configuration.class).warn(deprecatedDesc.getProperty("useSsl"));

        if (isSet("trustServerCertificate", nonMappedOptions)) {
          builder.sslMode("trust");
          Loggers.getLogger(Configuration.class)
              .warn(deprecatedDesc.getProperty("trustServerCertificate"));
        } else if (isSet("disableSslHostnameVerification", nonMappedOptions)) {
          Loggers.getLogger(Configuration.class)
              .warn(deprecatedDesc.getProperty("disableSslHostnameVerification"));
          builder.sslMode("verify-ca");
        } else {
          builder.sslMode("verify-full");
        }

      } catch (IOException e) {
        // Ignore IO exceptions when loading deprecation messages
      }
    }
  }

  private static boolean isSet(String key, Properties nonMappedOptions) {
    String value = nonMappedOptions.getProperty(key);
    return value != null && (value.equals("1") || value.equals("true") || value.isEmpty());
  }

  private static HaMode parseHaMode(String url, int separator) {
    // parser is sure to have at least 2 colon, since jdbc:[mysql|singlestore]: is tested.
    int firstColonPos = url.indexOf(':');
    int secondColonPos = url.indexOf(':', firstColonPos + 1);
    int thirdColonPos = url.indexOf(':', secondColonPos + 1);

    if (thirdColonPos > separator || thirdColonPos == -1) {
      if (secondColonPos == separator - 1) {
        return HaMode.NONE;
      }
      thirdColonPos = separator;
    }

    try {
      String haModeString = url.substring(secondColonPos + 1, thirdColonPos);
      if ("FAILOVER".equalsIgnoreCase(haModeString)) {
        haModeString = "LOADBALANCE";
      }
      return HaMode.from(haModeString);
    } catch (IllegalArgumentException i) {
      throw new IllegalArgumentException(
          "wrong failover parameter format in connection String " + url);
    }
  }

  /**
   * Permit to have string information on how string is parsed. example :
   * Configuration.toConf("jdbc:singlestore://localhost/test") will return a String containing:
   * <code>
   * Configuration:
   *  * resulting Url : jdbc:singlestore://localhost/test
   * Unknown options : None
   *
   * Non default options :
   *  * database : test
   *
   * default options :
   *  * user : null
   *  ...
   * </code>
   *
   * @param url url string
   * @return string describing the configuration parsed from url
   * @throws SQLException if parsing fails
   */
  public static String toConf(String url) throws SQLException {
    Configuration conf = Configuration.parseInternal(url, new Properties());
    Configuration defaultConf = Configuration.parse("jdbc:singlestore://localhost/");

    StringBuilder result = new StringBuilder();
    appendBasicConfiguration(result, conf);
    appendUnknownOptions(result, conf);
    appendNonDefaultOptions(result, conf, defaultConf);
    appendDefaultOptions(result, conf, defaultConf);

    return result.toString();
  }

  private static void appendBasicConfiguration(StringBuilder sb, Configuration conf) {
    sb.append("Configuration:\n * resulting Url : ").append(conf.initialUrl);
  }

  private static void appendUnknownOptions(StringBuilder sb, Configuration conf) {
    sb.append("\nUnknown options : ");
    if (conf.nonMappedOptions.isEmpty()) {
      sb.append("None\n");
      return;
    }

    conf.nonMappedOptions.entrySet().stream()
        .map(
            entry ->
                new AbstractMap.SimpleEntry<>(
                    entry.getKey().toString(),
                    entry.getValue() != null ? entry.getValue().toString() : ""))
        .sorted(Map.Entry.comparingByKey())
        .forEach(
            entry ->
                sb.append("\n * ").append(entry.getKey()).append(" : ").append(entry.getValue()));
    sb.append("\n");
  }

  private static void appendNonDefaultOptions(
      StringBuilder sb, Configuration conf, Configuration defaultConf) {
    try {
      StringBuilder diffOpts = new StringBuilder();
      processFields(conf, defaultConf, new StringBuilder(), diffOpts);

      sb.append("\nNon default options : ");
      if (diffOpts.length() == 0) {
        sb.append("None\n");
      } else {
        sb.append(diffOpts);
      }
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Error processing non-default options", e);
    }
  }

  private static void appendDefaultOptions(
      StringBuilder sb, Configuration conf, Configuration defaultConf) {
    try {
      StringBuilder defaultOpts = new StringBuilder();
      processFields(conf, defaultConf, defaultOpts, new StringBuilder());

      sb.append("\n\ndefault options :");
      if (defaultOpts.length() == 0) {
        sb.append("None\n");
      } else {
        sb.append(defaultOpts);
      }
    } catch (IllegalAccessException e) {
      throw new IllegalArgumentException("Error processing default options", e);
    }
  }

  private static void processFields(
      Configuration conf,
      Configuration defaultConf,
      StringBuilder defaultOpts,
      StringBuilder diffOpts)
      throws IllegalAccessException {
    Field[] fields = Configuration.class.getDeclaredFields();
    Arrays.sort(fields, Comparator.comparing(Field::getName));

    for (Field field : fields) {
      if (PROPERTIES_TO_SKIP.contains(field.getName())) {
        continue;
      }

      Object fieldValue = field.get(conf);
      Object defaultValue = field.get(defaultConf);
      processField(field, fieldValue, defaultValue, defaultOpts, diffOpts);
    }
  }

  private static void processField(
      Field field,
      Object fieldValue,
      Object defaultValue,
      StringBuilder defaultOpts,
      StringBuilder diffOpts) {
    if (fieldValue == null) {
      appendNullField(field, defaultValue, defaultOpts, diffOpts);
      return;
    }

    if (field.getName().equals("haMode")) {
      appendHaModeField(field, fieldValue, defaultValue, defaultOpts, diffOpts);
      return;
    }

    String typeName = fieldValue.getClass().getSimpleName();
    switch (typeName) {
      case "String":
      case "Boolean":
      case "HaMode":
      case "TransactionIsolation":
      case "Integer":
      case "SslMode":
        appendSimpleField(field, fieldValue, defaultValue, defaultOpts, diffOpts);
        break;
      case "ArrayList":
        appendListField(field, fieldValue, defaultValue, defaultOpts, diffOpts);
        break;
      case "Properties":
      case "HashSet":
        break;
      default:
        throw new IllegalArgumentException("Unexpected field type for: " + field.getName());
    }
  }

  private static void appendNullField(
      Field field, Object defaultValue, StringBuilder defaultOpts, StringBuilder diffOpts) {
    StringBuilder target = defaultValue == null ? defaultOpts : diffOpts;
    target.append("\n * ").append(field.getName()).append(" : null");
  }

  private static void appendHaModeField(
      Field field,
      Object fieldValue,
      Object defaultValue,
      StringBuilder defaultOpts,
      StringBuilder diffOpts) {
    StringBuilder target = Objects.equals(fieldValue, defaultValue) ? defaultOpts : diffOpts;
    target.append("\n * ").append(field.getName()).append(" : ").append(fieldValue);
  }

  private static void appendSimpleField(
      Field field,
      Object fieldValue,
      Object defaultValue,
      StringBuilder defaultOpts,
      StringBuilder diffOpts) {
    StringBuilder target = Objects.equals(fieldValue, defaultValue) ? defaultOpts : diffOpts;
    target.append("\n * ").append(field.getName()).append(" : ");

    if (SENSITIVE_FIELDS.contains(field.getName())) {
      target.append("***");
    } else {
      target.append(fieldValue);
    }
  }

  private static void appendListField(
      Field field,
      Object fieldValue,
      Object defaultValue,
      StringBuilder defaultOpts,
      StringBuilder diffOpts) {
    StringBuilder target =
        Objects.equals(fieldValue.toString(), defaultValue.toString()) ? defaultOpts : diffOpts;
    target.append("\n * ").append(field.getName()).append(" : ").append(fieldValue);
  }

  public Configuration clone(String username, String password) {
    return this.toBuilder()
        .user(username != null && username.isEmpty() ? null : username)
        .password(password != null && password.isEmpty() ? null : password)
        .build();
  }

  /**
   * Connection default database
   *
   * @return database
   */
  public String database() {
    return database;
  }

  /**
   * addresses
   *
   * @return addresses
   */
  public List<HostAddress> addresses() {
    return addresses;
  }

  /**
   * High availability mode
   *
   * @return configuration HA mode
   */
  public HaMode haMode() {
    return haMode;
  }

  /**
   * credential plugin to use
   *
   * @return credential plugin to use, null of none
   */
  public CredentialPlugin credentialPlugin() {
    return credentialType;
  }

  /**
   * configuration user
   *
   * @return user
   */
  public String user() {
    return user;
  }

  /**
   * configuration password
   *
   * @return password
   */
  public String password() {
    return password;
  }

  /**
   * Configuration generated URL depending on current configuration option. Password will be hidden
   * by "***"
   *
   * @return generated url
   */
  public String initialUrl() {
    return initialUrl;
  }

  /**
   * server ssl certificate (file path / certificat content)
   *
   * @return server ssl certificate
   */
  public String serverSslCert() {
    return serverSslCert;
  }

  /**
   * trust store
   *
   * @return trust store
   */
  public String trustStore() {
    return trustStore;
  }

  /**
   * trust store password
   *
   * @return trust store password
   */
  public String trustStorePassword() {
    return trustStorePassword;
  }

  /**
   * trust store type
   *
   * @return trust store type
   */
  public String trustStoreType() {
    return trustStoreType;
  }

  /**
   * key store
   *
   * @return key store
   */
  public String keyStore() {
    return keyStore;
  }

  /**
   * key store password
   *
   * @return key store password
   */
  public String keyStorePassword() {
    return keyStorePassword;
  }

  /**
   * key store alias password
   *
   * @return key store alias password
   */
  public String keyPassword() {
    return keyPassword;
  }

  /**
   * key store type (to replace default javax.net.ssl.keyStoreType system property)
   *
   * @return key store type
   */
  public String keyStoreType() {
    return keyStoreType;
  }

  /**
   * permitted ssl protocol list (comma separated)
   *
   * @return enabled ssl protocol list
   */
  public String enabledSslProtocolSuites() {
    return enabledSslProtocolSuites;
  }

  /**
   * the host name to be used to validate the SingleStore TLS/SSL certificate
   *
   * @return the host name to be used to validate the SingleStore TLS/SSL certificate
   */
  public String hostNameInCertificate() {
    return hostNameInCertificate;
  }

  /**
   * Socket factory class name
   *
   * @return socket factory
   */
  public String socketFactory() {
    return socketFactory;
  }

  /**
   * socket connect timeout
   *
   * @return connect timeout
   */
  public int connectTimeout() {
    return connectTimeout;
  }

  /**
   * Set connect timeout
   *
   * @param connectTimeout timeout value
   * @return current configuration
   */
  public Configuration connectTimeout(int connectTimeout) {
    this.connectTimeout = connectTimeout;
    return this;
  }

  /**
   * Pipe path
   *
   * @return pipe value
   */
  public String pipe() {
    return pipe;
  }

  /**
   * local socket configuration
   *
   * @return local socket path
   */
  public String localSocket() {
    return localSocket;
  }

  /**
   * socket tcp keep alive
   *
   * @return socket tcp keep alive value
   */
  public boolean tcpKeepAlive() {
    return tcpKeepAlive;
  }

  /**
   * socket tcp keep idle (java 11+ only)
   *
   * @return socket tcp keep idle
   */
  public int tcpKeepIdle() {
    return tcpKeepIdle;
  }

  /**
   * socket tcp keep count (java 11+ only)
   *
   * @return socket tcp keep count
   */
  public int tcpKeepCount() {
    return tcpKeepCount;
  }

  /**
   * socket tcp keep interval (java 11+ only)
   *
   * @return socket tcp keep interval
   */
  public int tcpKeepInterval() {
    return tcpKeepInterval;
  }

  /**
   * close using TCP abortive close (RST TCP packet, in place or FIN packet)
   *
   * @return close using TCP abortive close
   */
  public boolean tcpAbortiveClose() {
    return tcpAbortiveClose;
  }

  /**
   * local socket address path
   *
   * @return local socket address
   */
  public String localSocketAddress() {
    return localSocketAddress;
  }

  /**
   * socket timeout
   *
   * @return socket timeout
   */
  public int socketTimeout() {
    return socketTimeout;
  }

  /**
   * SOCKS proxy host.
   *
   * @return SOCKS proxy host or null when disabled
   */
  public String socksProxyHost() {
    return socksProxyHost;
  }

  /**
   * SOCKS proxy port.
   *
   * @return SOCKS proxy port, or null when unset
   */
  public Integer socksProxyPort() {
    return socksProxyPort;
  }

  /**
   * permit using multi queries command
   *
   * @return permit using multi queries command
   */
  public boolean allowMultiQueries() {
    return allowMultiQueries;
  }

  /**
   * permits LOAD LOCAL INFILE commands
   *
   * @return allow LOAD LOCAL INFILE
   */
  public boolean allowLocalInfile() {
    return allowLocalInfile;
  }

  /**
   * Enable compression if server has compression capability
   *
   * @return use compression
   */
  public boolean useCompression() {
    return useCompression;
  }

  /**
   * force returning blank table metadata (for old oracle compatibility)
   *
   * @return metadata table return blank
   */
  public boolean blankTableNameMeta() {
    return blankTableNameMeta;
  }

  /**
   * SSl mode
   *
   * @return ssl mode
   */
  public SslMode sslMode() {
    return sslMode;
  }

  /**
   * Default transaction isolation
   *
   * @return default transaction isolation.
   */
  public TransactionIsolation transactionIsolation() {
    return transactionIsolation;
  }

  /**
   * autorized cipher list.
   *
   * @return list of permitted ciphers
   */
  public String enabledSslCipherSuites() {
    return enabledSslCipherSuites;
  }

  /**
   * coma separated Session variable list
   *
   * @return session variable
   */
  public String sessionVariables() {
    return sessionVariables;
  }

  /**
   * Must tinyint be considered as Bit (TINYINT is always has reserved length = 4)
   *
   * @return true if tinyint must be considered as Bit
   */
  public boolean tinyInt1isBit() {
    return tinyInt1isBit;
  }

  /**
   * Must tinyint be considered as Boolean or Bit
   *
   * @return true if tinyint must be considered as Boolean
   */
  public boolean transformedBitIsBoolean() {
    return transformedBitIsBoolean;
  }

  /**
   * Must year be return by default as Date in result-set
   *
   * @return year is Date type
   */
  public boolean yearIsDateType() {
    return yearIsDateType;
  }

  /**
   * Must query by logged on exception.
   *
   * @return dump queries on exception
   */
  public boolean dumpQueriesOnException() {
    return dumpQueriesOnException;
  }

  /**
   * Prepare statement cache size.
   *
   * @return Prepare statement cache size
   */
  public int prepStmtCacheSize() {
    return prepStmtCacheSize;
  }

  /**
   * Use affected row
   *
   * @return use affected rows
   */
  public boolean useAffectedRows() {
    return useAffectedRows;
  }

  public boolean disablePipeline() {
    return disablePipeline;
  }

  /**
   * Use server prepared statement. IF false, using client prepared statement.
   *
   * @return use server prepared statement
   */
  public boolean useServerPrepStmts() {
    return useServerPrepStmts;
  }

  public String connectionAttributes() {
    return connectionAttributes;
  }

  /**
   * Force session autocommit on connection creation
   *
   * @return autocommit forced value
   */
  public Boolean autocommit() {
    return autocommit;
  }

  /**
   * When enabled, in DatabaseMetadata, will handle null database as current
   *
   * @return must null value be considered as current catalog
   */
  public boolean nullDatabaseMeansCurrent() {
    return nullDatabaseMeansCurrent;
  }

  public boolean includeThreadDumpInDeadlockExceptions() {
    return includeThreadDumpInDeadlockExceptions;
  }

  /**
   * create database if not exist
   *
   * @return create database if not exist
   */
  public boolean createDatabaseIfNotExist() {
    return createDatabaseIfNotExist;
  }

  /**
   * Execute initial command when connection is established
   *
   * @return initial SQL command
   */
  public String initSql() {
    return initSql;
  }

  public String servicePrincipalName() {
    return servicePrincipalName;
  }

  public String jaasApplicationName() {
    return jaasApplicationName;
  }

  /**
   * When using GSSAPI authentication, explicitly enable or disable caching for the JAAS Login
   * Context.
   *
   * @return cacheJaasLoginContext forced value
   */
  public Boolean cacheJaasLoginContext() {
    return cacheJaasLoginContext;
  }

  /**
   * @return pre-supplied GSS credential for GSSAPI auth, or null to use JAAS / default credentials
   */
  public GSSCredential gssCredential() {
    return gssCredential;
  }

  /**
   * @return {@code true} when GSSAPI auth should request credential delegation ({@code
   *     GSSContext.requestCredDeleg}); {@code false} when unset or explicitly false.
   */
  public boolean requestCredentialDelegation() {
    return requestCredentialDelegation;
  }

  public int defaultFetchSize() {
    return defaultFetchSize;
  }

  public Properties nonMappedOptions() {
    return nonMappedOptions;
  }

  public String tlsSocketType() {
    return tlsSocketType;
  }

  public int maxQuerySizeToLog() {
    return maxQuerySizeToLog;
  }

  /**
   * max_allowed_packet value to avoid sending packet with non supported size, droping the
   * connection without reason.
   *
   * @return max_allowed_packet value
   */
  public Integer maxAllowedPacket() {
    return maxAllowedPacket;
  }

  public int retriesAllDown() {
    return retriesAllDown;
  }

  public boolean pool() {
    return pool;
  }

  public String poolName() {
    return poolName;
  }

  public int maxPoolSize() {
    return maxPoolSize;
  }

  public int minPoolSize() {
    return minPoolSize;
  }

  public int maxIdleTime() {
    return maxIdleTime;
  }

  public boolean registerJmxPool() {
    return registerJmxPool;
  }

  public int poolValidMinDelay() {
    return poolValidMinDelay;
  }

  public boolean useResetConnection() {
    return useResetConnection;
  }

  public boolean useReadAheadInput() {
    return useReadAheadInput;
  }

  public boolean cachePrepStmts() {
    return cachePrepStmts;
  }

  public boolean transactionReplay() {
    return transactionReplay;
  }

  /**
   * transaction replay maximum number of saved command.
   *
   * @return transaction replay buffer size.
   */
  public int transactionReplaySize() {
    return transactionReplaySize;
  }

  public String geometryDefaultType() {
    return geometryDefaultType;
  }

  public String restrictedAuth() {
    return restrictedAuth;
  }

  public Codec<?>[] codecs() {
    return codecs;
  }

  public boolean useMysqlVersion() {
    return useMysqlVersion;
  }

  /**
   * Force returning MySQL metadata information
   *
   * @return force returning MySQL in metadata
   */
  public boolean useMysqlMetadata() {
    return useMysqlMetadata;
  }

  public boolean rewriteBatchedStatements() {
    return rewriteBatchedStatements;
  }

  public boolean preserveInstants() {
    return preserveInstants;
  }

  public String getConsoleLogLevel() {
    return consoleLogLevel;
  }

  public String getConsoleLogFilepath() {
    return consoleLogFilepath;
  }

  public boolean printStackTrace() {
    return printStackTrace;
  }

  public int maxPrintStackSizeToLog() {
    return maxPrintStackSizeToLog;
  }

  public boolean enableExtendedDataTypes() {
    return enableExtendedDataTypes;
  }

  public String vectorTypeOutputFormat() {
    return vectorTypeOutputFormat;
  }

  public boolean vectorExtendedMetadata() {
    return vectorExtendedMetadata;
  }

  /**
   * ToString implementation.
   *
   * @return String value
   */
  public String toString() {
    return initialUrl;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    Configuration that = (Configuration) o;

    return initialUrl.equals(that.initialUrl);
  }

  /**
   * Builds a JDBC URL from the provided configuration.
   *
   * @param conf Current configuration
   * @return Complete JDBC URL string
   */
  protected static String buildUrl(Configuration conf) {
    try {
      StringBuilder urlBuilder = new StringBuilder("jdbc:singlestore:");
      appendHaModeIfPresent(urlBuilder, conf);
      appendHostAddresses(urlBuilder, conf);
      appendDatabase(urlBuilder, conf);
      appendConfigurationParameters(urlBuilder, conf);

      conf.loadCodecs();
      conf.resetLoggerFactory();
      return urlBuilder.toString();
    } catch (SecurityException s) {
      throw new IllegalArgumentException("Security too restrictive: " + s.getMessage());
    }
  }

  private static void appendHostAddresses(StringBuilder sb, Configuration conf) {
    sb.append("//");
    for (int i = 0; i < conf.addresses.size(); i++) {
      if (i > 0) sb.append(",");
      appendHostAddress(sb, conf.addresses.get(i), i);
    }
    sb.append("/");
  }

  private static void appendHostAddress(StringBuilder sb, HostAddress hostAddress, int index) {

    if (index < 1) {
      sb.append(hostAddress.host);
      if (hostAddress.port != 3306) {
        sb.append(":").append(hostAddress.port);
      }
    } else {
      sb.append(hostAddress);
    }
  }

  private static void appendDatabase(StringBuilder sb, Configuration conf) {
    if (conf.database != null) {
      sb.append(conf.database);
    }
  }

  private static void appendHaModeIfPresent(StringBuilder sb, Configuration conf) {
    if (conf.haMode != HaMode.NONE) {
      sb.append(conf.haMode.toString().toLowerCase(Locale.ROOT).replace("_", "-")).append(":");
    }
  }

  private static void appendConfigurationParameters(StringBuilder sb, Configuration conf) {
    try {
      Configuration defaultConf = new Configuration(new Builder());
      ParameterAppender paramAppender = new ParameterAppender(sb);

      for (Field field : Configuration.class.getDeclaredFields()) {
        if (EXCLUDED_FIELDS.contains(field.getName())) {
          continue;
        }

        Object value = field.get(conf);
        if (value == null || (value instanceof Properties && ((Properties) value).isEmpty())) {
          continue;
        }

        appendFieldParameter(paramAppender, field, value, defaultConf);
      }
    } catch (IllegalAccessException e) {
      throw new IllegalStateException(e);
    }
  }

  private static void appendFieldParameter(
      ParameterAppender appender, Field field, Object value, Configuration defaultConf)
      throws IllegalAccessException {

    if (SECURE_FIELDS.contains(field.getName())) {
      appender.appendParameter(field.getName(), "***");
      return;
    }

    Class<?> fieldType = field.getType();
    if (fieldType.equals(String.class)) {
      appendStringParameter(appender, field, value, defaultConf);
    } else if (fieldType.equals(boolean.class)) {
      appendBooleanParameter(appender, field, value, defaultConf);
    } else if (fieldType.equals(int.class)) {
      appendIntParameter(appender, field, value, defaultConf);
    } else if (fieldType.equals(Properties.class)) {
      appendPropertiesParameter(appender, (Properties) value);
    } else if (fieldType.equals(CredentialPlugin.class)) {
      appendCredentialPluginParameter(appender, field, value, defaultConf);
    } else {
      appendDefaultParameter(appender, field, value, defaultConf);
    }
  }

  private static void appendStringParameter(
      ParameterAppender appender, Field field, Object value, Configuration defaultConf)
      throws IllegalAccessException {
    String defaultValue = (String) field.get(defaultConf);
    if (!value.equals(defaultValue)) {
      appender.appendParameter(field.getName(), (String) value);
    }
  }

  private static void appendBooleanParameter(
      ParameterAppender appender, Field field, Object value, Configuration defaultConf)
      throws IllegalAccessException {
    boolean defaultValue = field.getBoolean(defaultConf);
    if (!value.equals(defaultValue)) {
      appender.appendParameter(field.getName(), value.toString());
    }
  }

  private static void appendIntParameter(
      ParameterAppender appender, Field field, Object value, Configuration defaultConf) {
    try {
      int defaultValue = field.getInt(defaultConf);
      if (!value.equals(defaultValue)) {
        appender.appendParameter(field.getName(), value.toString());
      }
    } catch (IllegalAccessException e) {
      // Ignore access errors for int fields
    }
  }

  private static void appendPropertiesParameter(ParameterAppender appender, Properties props) {
    for (Object key : props.keySet()) {
      appender.appendParameter(key.toString(), props.get(key).toString());
    }
  }

  private static void appendCredentialPluginParameter(
      ParameterAppender appender, Field field, Object value, Configuration defaultConf)
      throws IllegalAccessException {
    Object defaultValue = field.get(defaultConf);
    if (!value.equals(defaultValue)) {
      appender.appendParameter(field.getName(), ((CredentialPlugin) value).type());
    }
  }

  private static void appendDefaultParameter(
      ParameterAppender appender, Field field, Object value, Configuration defaultConf)
      throws IllegalAccessException {
    Object defaultValue = field.get(defaultConf);
    if (!value.equals(defaultValue)) {
      appender.appendParameter(field.getName(), value.toString());
    }
  }

  private static class ParameterAppender {
    private final StringBuilder sb;
    private boolean first = true;

    ParameterAppender(StringBuilder sb) {
      this.sb = sb;
    }

    void appendParameter(String name, String value) {
      sb.append(first ? '?' : '&').append(name).append('=').append(value);
      first = false;
    }
  }

  @SuppressWarnings("rawtypes")
  private void loadCodecs() {
    ServiceLoader<Codec> loader =
        ServiceLoader.load(Codec.class, Configuration.class.getClassLoader());
    List<Codec<?>> result = new ArrayList<>();
    loader.iterator().forEachRemaining(result::add);
    codecs = result.toArray(new Codec<?>[0]);
  }

  private void resetLoggerFactory() {
    Loggers.resetLoggerFactoryProperties(
        this.consoleLogLevel,
        this.consoleLogFilepath,
        this.printStackTrace,
        this.maxPrintStackSizeToLog);
  }

  @Override
  public int hashCode() {
    return initialUrl.hashCode();
  }

  /** A builder for {@link Configuration} instances. */
  public static final class Builder implements Cloneable {

    private Properties _nonMappedOptions;
    private HaMode _haMode;
    private List<HostAddress> _addresses = new ArrayList<>();

    // standard options
    private String user;
    private String password;
    private String database;

    // various
    private Boolean autocommit;
    private Boolean useMysqlMetadata;
    private Boolean useMysqlVersion;
    private Boolean nullDatabaseMeansCurrent;
    private Boolean createDatabaseIfNotExist;
    private String initSql;
    private Integer defaultFetchSize;
    private Integer maxQuerySizeToLog;
    private Integer maxAllowedPacket;
    private String geometryDefaultType;
    private String restrictedAuth;
    private String transactionIsolation;

    // socket
    private String socketFactory;
    private Integer connectTimeout;
    private String pipe;
    private String localSocket;
    private Boolean tcpKeepAlive;
    private Integer tcpKeepIdle;
    private Integer tcpKeepCount;
    private Integer tcpKeepInterval;
    private Boolean tcpAbortiveClose;
    private String localSocketAddress;
    private Integer socketTimeout;
    private String socksProxyHost;
    private Integer socksProxyPort;
    private Boolean useReadAheadInput;
    private String tlsSocketType;

    // SSL
    private String sslMode;
    private String serverSslCert;
    private String trustStore;
    private String trustStorePassword;
    private String trustStoreType;
    private String keyStore;
    private String keyStorePassword;
    private String keyPassword;
    private String keyStoreType;
    private String enabledSslCipherSuites;
    private String enabledSslProtocolSuites;
    private String hostNameInCertificate;

    // protocol
    private Boolean allowMultiQueries;
    private Boolean allowLocalInfile;
    private Boolean useCompression;
    private Boolean useAffectedRows;
    private Boolean disablePipeline;

    // prepare
    private Boolean cachePrepStmts;
    private Integer prepStmtCacheSize;
    private Boolean useServerPrepStmts;

    // authentication
    private String credentialType;
    private String sessionVariables;
    private String connectionAttributes;
    private String servicePrincipalName;
    private String jaasApplicationName;
    private Boolean cacheJaasLoginContext;
    private GSSCredential gssCredential;
    private Boolean requestCredentialDelegation;

    // meta
    private Boolean blankTableNameMeta;
    private Boolean tinyInt1isBit;

    private Boolean transformedBitIsBoolean;
    private Boolean yearIsDateType;
    private Boolean dumpQueriesOnException;
    private Boolean includeThreadDumpInDeadlockExceptions;

    // HA options
    private Integer retriesAllDown;
    private Boolean transactionReplay;
    private Integer transactionReplaySize;

    // Pool options
    private Boolean pool;
    private String poolName;
    private Integer maxPoolSize;
    private Integer minPoolSize;
    private Integer maxIdleTime;
    private Boolean registerJmxPool;
    private Integer poolValidMinDelay;
    private Boolean useResetConnection;

    private Boolean rewriteBatchedStatements;
    private Boolean preserveInstants;
    private String consoleLogLevel;
    private String consoleLogFilepath;
    private Boolean printStackTrace;
    private Integer maxPrintStackSizeToLog;

    private Boolean enableExtendedDataTypes;
    private String vectorTypeOutputFormat;
    private Boolean vectorExtendedMetadata;

    public Builder user(String user) {
      this.user = nullOrEmpty(user);
      return this;
    }

    public Builder serverSslCert(String serverSslCert) {
      this.serverSslCert = nullOrEmpty(serverSslCert);
      return this;
    }

    public Builder trustStore(String trustStore) {
      this.trustStore = nullOrEmpty(trustStore);
      return this;
    }

    public Builder trustStorePassword(String trustStorePassword) {
      this.trustStorePassword = nullOrEmpty(trustStorePassword);
      return this;
    }

    public Builder trustStoreType(String trustStoreType) {
      this.trustStoreType = nullOrEmpty(trustStoreType);
      return this;
    }

    /**
     * File path of the keyStore file that contain client private key store and associate
     * certificates (similar to java System property \"javax.net.ssl.keyStore\", but ensure that
     * only the private key's entries are used)
     *
     * @param keyStore client store certificates
     * @return this {@link Builder}
     */
    public Builder keyStore(String keyStore) {
      this.keyStore = nullOrEmpty(keyStore);
      return this;
    }

    /**
     * Client keystore password
     *
     * @param keyStorePassword client store password
     * @return this {@link Builder}
     */
    public Builder keyStorePassword(String keyStorePassword) {
      this.keyStorePassword = nullOrEmpty(keyStorePassword);
      return this;
    }

    /**
     * Client keystore alias password
     *
     * @param keyPassword client store alias password
     * @return this {@link Builder}
     */
    public Builder keyPassword(String keyPassword) {
      this.keyPassword = nullOrEmpty(keyPassword);
      return this;
    }

    public Builder keyStoreType(String keyStoreType) {
      this.keyStoreType = nullOrEmpty(keyStoreType);
      return this;
    }

    public Builder password(String password) {
      this.password = nullOrEmpty(password);
      return this;
    }

    public Builder enabledSslProtocolSuites(String enabledSslProtocolSuites) {
      this.enabledSslProtocolSuites = nullOrEmpty(enabledSslProtocolSuites);
      return this;
    }

    public Builder hostNameInCertificate(String hostNameInCertificate) {
      this.hostNameInCertificate = nullOrEmpty(hostNameInCertificate);
      return this;
    }

    public Builder database(String database) {
      this.database = database;
      return this;
    }

    public Builder haMode(HaMode haMode) {
      this._haMode = haMode;
      return this;
    }

    public Builder addHost(String host, int port) {
      this._addresses.add(HostAddress.from(nullOrEmpty(host), port));
      return this;
    }

    public Builder addresses(HostAddress... hostAddress) {
      this._addresses = new ArrayList<>();
      this._addresses.addAll(Arrays.asList(hostAddress));
      return this;
    }

    public Builder socketFactory(String socketFactory) {
      this.socketFactory = socketFactory;
      return this;
    }

    /**
     * Indicate connect timeout value, in milliseconds, or zero for no timeout. Default: 30000
     *
     * @param connectTimeout connect Timeout
     * @return this {@link Builder}
     */
    public Builder connectTimeout(Integer connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    /**
     * Indicate to use windows named pipe, specify named pipe name to connect
     *
     * @param pipe windows named pipe
     * @return this {@link Builder}
     */
    public Builder pipe(String pipe) {
      this.pipe = nullOrEmpty(pipe);
      return this;
    }

    /**
     * Indicate to use Unix domain socket, if the server allows it, specifying named pipe name to
     * connect The value is the path of Unix domain socket (available with "select @@socket"
     * command).
     *
     * @param localSocket local socket path
     * @return this {@link Builder}
     */
    public Builder localSocket(String localSocket) {
      this.localSocket = nullOrEmpty(localSocket);
      return this;
    }

    /**
     * Indicate if TCP keep-alive must be enable.
     *
     * @param tcpKeepAlive value
     * @return this {@link Builder}
     */
    public Builder tcpKeepAlive(Boolean tcpKeepAlive) {
      this.tcpKeepAlive = tcpKeepAlive;
      return this;
    }

    /**
     * Indicate TCP keep-idle value (for java 11+ only).
     *
     * @param tcpKeepIdle value
     * @return this {@link Builder}
     */
    public Builder tcpKeepIdle(Integer tcpKeepIdle) {
      this.tcpKeepIdle = tcpKeepIdle;
      return this;
    }

    /**
     * Indicate TCP keep-count value (for java 11+ only).
     *
     * @param tcpKeepCount value
     * @return this {@link Builder}
     */
    public Builder tcpKeepCount(Integer tcpKeepCount) {
      this.tcpKeepCount = tcpKeepCount;
      return this;
    }

    /**
     * Indicate TCP keep-interval value (for java 11+ only).
     *
     * @param tcpKeepInterval value
     * @return this {@link Builder}
     */
    public Builder tcpKeepInterval(Integer tcpKeepInterval) {
      this.tcpKeepInterval = tcpKeepInterval;
      return this;
    }

    /**
     * Indicate that when connection fails, to send an RST TCP packet.
     *
     * @param tcpAbortiveClose value
     * @return this {@link Builder}
     */
    public Builder tcpAbortiveClose(Boolean tcpAbortiveClose) {
      this.tcpAbortiveClose = tcpAbortiveClose;
      return this;
    }

    /**
     * Indicate what default Object type Geometry a resultset.getObject must return. possibility :
     *
     * <ul>
     *   <li>null or empty is WKB byte array
     *   <li>'default' will return com.singlestore.jdbc.type Object
     * </ul>
     *
     * In the future JTS might be implemented
     *
     * @param geometryDefault value
     * @return this {@link Builder}
     */
    public Builder geometryDefaultType(String geometryDefault) {
      this.geometryDefaultType = nullOrEmpty(geometryDefault);
      return this;
    }

    /**
     * restrict authentication method to secure list. Default "default".
     *
     * @param restrictedAuth use authentication plugin list
     * @return this {@link Builder}
     */
    public Builder restrictedAuth(String restrictedAuth) {
      this.restrictedAuth = restrictedAuth;
      return this;
    }

    /**
     * Indicate Hostname or IP address to bind the connection socket to a local (UNIX domain)
     * socket.
     *
     * @param localSocketAddress Hostname or IP address
     * @return this {@link Builder}
     */
    public Builder localSocketAddress(String localSocketAddress) {
      this.localSocketAddress = nullOrEmpty(localSocketAddress);
      return this;
    }

    /**
     * Indicate the network socket timeout (SO_TIMEOUT) in milliseconds. Value of 0 disables this
     * timeout.
     *
     * <p>If the goal is to set a timeout for all queries, the server has permitted a solution to
     * limit the query time by setting a system variable, max_statement_time. Default: 0
     *
     * @param socketTimeout socket timeout value
     * @return this {@link Builder}
     */
    public Builder socketTimeout(Integer socketTimeout) {
      this.socketTimeout = socketTimeout;
      return this;
    }

    /**
     * SOCKS proxy host to connect through.
     *
     * @param socksProxyHost SOCKS proxy host
     * @return this {@link Builder}
     */
    public Builder socksProxyHost(String socksProxyHost) {
      this.socksProxyHost = nullOrEmpty(socksProxyHost);
      return this;
    }

    /**
     * SOCKS proxy port to connect through. Default: null (uses 1080 when proxy is enabled)
     *
     * @param socksProxyPort SOCKS proxy port
     * @return this {@link Builder}
     */
    public Builder socksProxyPort(Integer socksProxyPort) {
      this.socksProxyPort = socksProxyPort;
      return this;
    }

    /**
     * Indicate that multi-queries are allowed. example: "insert into ab (i) values (1); insert into
     * ab (i) values (2)".
     *
     * <p>If application build sql command string, this is probably a bad idea to enable this
     * option, opening the door to sql injection. default: false.
     *
     * @param allowMultiQueries indicate if active
     * @return this {@link Builder}
     */
    public Builder allowMultiQueries(Boolean allowMultiQueries) {
      this.allowMultiQueries = allowMultiQueries;
      return this;
    }

    /**
     * Indicate if LOAD DATA LOCAL INFILE are permitted. This will disable all pipelining
     * implementation.
     *
     * @param allowLocalInfile indicate if permit LOAD DATA LOCAL INFILE commands
     * @return this {@link Builder}
     */
    public Builder allowLocalInfile(Boolean allowLocalInfile) {
      this.allowLocalInfile = allowLocalInfile;
      return this;
    }

    /**
     * Indicate to compresses exchanges with the database through gzip. This permits better
     * performance when the database is not in the same location.
     *
     * @param useCompression to enable/disable compression
     * @return this {@link Builder}
     */
    public Builder useCompression(Boolean useCompression) {
      this.useCompression = useCompression;
      return this;
    }

    public Builder blankTableNameMeta(Boolean blankTableNameMeta) {
      this.blankTableNameMeta = blankTableNameMeta;
      return this;
    }

    public Builder credentialType(String credentialType) {
      this.credentialType = nullOrEmpty(credentialType);
      return this;
    }

    public Builder sslMode(String sslMode) {
      this.sslMode = sslMode;
      return this;
    }

    public Builder transactionIsolation(String transactionIsolation) {
      this.transactionIsolation = nullOrEmpty(transactionIsolation);
      return this;
    }

    public Builder enabledSslCipherSuites(String enabledSslCipherSuites) {
      this.enabledSslCipherSuites = nullOrEmpty(enabledSslCipherSuites);
      return this;
    }

    public Builder sessionVariables(String sessionVariables) {
      this.sessionVariables = nullOrEmpty(sessionVariables);
      return this;
    }

    /**
     * TinyInt(1) to be considered as bit
     *
     * @param tinyInt1isBit Indicate if Tinyint(1) to be considered as bit
     * @return this {@link Builder}
     */
    public Builder tinyInt1isBit(Boolean tinyInt1isBit) {
      this.tinyInt1isBit = tinyInt1isBit;
      return this;
    }

    /**
     * TinyInt(1) to be considered as boolean
     *
     * @param transformedBitIsBoolean Indicate if Tinyint(1) to be considered as boolean
     * @return this {@link Builder}
     */
    public Builder transformedBitIsBoolean(Boolean transformedBitIsBoolean) {
      this.transformedBitIsBoolean = transformedBitIsBoolean;
      return this;
    }

    public Builder yearIsDateType(Boolean yearIsDateType) {
      this.yearIsDateType = yearIsDateType;
      return this;
    }

    public Builder dumpQueriesOnException(Boolean dumpQueriesOnException) {
      this.dumpQueriesOnException = dumpQueriesOnException;
      return this;
    }

    public Builder prepStmtCacheSize(Integer prepStmtCacheSize) {
      this.prepStmtCacheSize = prepStmtCacheSize;
      return this;
    }

    public Builder useAffectedRows(Boolean useAffectedRows) {
      this.useAffectedRows = useAffectedRows;
      return this;
    }

    public Builder disablePipeline(Boolean disablePipeline) {
      this.disablePipeline = disablePipeline;
      return this;
    }

    public Builder useServerPrepStmts(Boolean useServerPrepStmts) {
      this.useServerPrepStmts = useServerPrepStmts;
      return this;
    }

    /**
     * Permit to force autocommit connection value
     *
     * @param autocommit autocommit value
     * @return this {@link Builder}
     */
    public Builder autocommit(Boolean autocommit) {
      this.autocommit = autocommit;
      return this;
    }

    /**
     * Permit indicating to force DatabaseMetadata.getDatabaseProductName() to return `MySQL` as
     * database type, not real database type
     *
     * @param useMysqlMetadata force DatabaseMetadata.getDatabaseProductName() to return `MySQL`
     * @return this {@link Builder}
     */
    public Builder useMysqlMetadata(Boolean useMysqlMetadata) {
      this.useMysqlMetadata = useMysqlMetadata;
      return this;
    }

    /**
     * Permit indicating in DatabaseMetadata if null value must be considered current catalog
     *
     * @param nullDatabaseMeansCurrent indicating in DatabaseMetadata if null value must be
     *     considered current catalog
     * @return this {@link Builder}
     */
    public Builder nullDatabaseMeansCurrent(Boolean nullDatabaseMeansCurrent) {
      this.nullDatabaseMeansCurrent = nullDatabaseMeansCurrent;
      return this;
    }

    /**
     * Create database if not exist. This is mainly for test, since does require an additional query
     * after connection
     *
     * @param createDatabaseIfNotExist must driver create database if doesn't exist
     * @return this {@link Builder}
     */
    public Builder createDatabaseIfNotExist(Boolean createDatabaseIfNotExist) {
      this.createDatabaseIfNotExist = createDatabaseIfNotExist;
      return this;
    }

    /**
     * permit to execute an SQL command on connection creation
     *
     * @param initSql initial SQL command
     * @return this {@link Builder}
     */
    public Builder initSql(String initSql) {
      this.initSql = initSql;
      return this;
    }

    public Builder connectionAttributes(String connectionAttributes) {
      this.connectionAttributes = nullOrEmpty(connectionAttributes);
      return this;
    }

    public Builder includeThreadDumpInDeadlockExceptions(
        Boolean includeThreadDumpInDeadlockExceptions) {
      this.includeThreadDumpInDeadlockExceptions = includeThreadDumpInDeadlockExceptions;
      return this;
    }

    public Builder servicePrincipalName(String servicePrincipalName) {
      this.servicePrincipalName = nullOrEmpty(servicePrincipalName);
      return this;
    }

    public Builder jaasApplicationName(String jaasApplicationName) {
      this.jaasApplicationName = nullOrEmpty(jaasApplicationName);
      return this;
    }

    public Builder cacheJaasLoginContext(Boolean cacheJaasLoginContext) {
      this.cacheJaasLoginContext = cacheJaasLoginContext;
      return this;
    }

    public Builder gssCredential(GSSCredential gssCredential) {
      this.gssCredential = gssCredential;
      return this;
    }

    public Builder requestCredentialDelegation(Boolean requestCredentialDelegation) {
      this.requestCredentialDelegation = requestCredentialDelegation;
      return this;
    }

    public Builder defaultFetchSize(Integer defaultFetchSize) {
      this.defaultFetchSize = defaultFetchSize;
      return this;
    }

    public Builder tlsSocketType(String tlsSocketType) {
      this.tlsSocketType = nullOrEmpty(tlsSocketType);
      return this;
    }

    public Builder maxQuerySizeToLog(Integer maxQuerySizeToLog) {
      this.maxQuerySizeToLog = maxQuerySizeToLog;
      return this;
    }

    /**
     * Indicate to driver server max_allowed_packet. This permit to driver to avoid sending commands
     * too big, that would have make server to drop connection
     *
     * @param maxAllowedPacket indicate server max_allowed_packet value
     * @return this {@link Builder}
     */
    public Builder maxAllowedPacket(Integer maxAllowedPacket) {
      this.maxAllowedPacket = maxAllowedPacket;
      return this;
    }

    public Builder retriesAllDown(Integer retriesAllDown) {
      this.retriesAllDown = retriesAllDown;
      return this;
    }

    public Builder pool(Boolean pool) {
      this.pool = pool;
      return this;
    }

    public Builder poolName(String poolName) {
      this.poolName = nullOrEmpty(poolName);
      return this;
    }

    public Builder maxPoolSize(Integer maxPoolSize) {
      this.maxPoolSize = maxPoolSize;
      return this;
    }

    public Builder minPoolSize(Integer minPoolSize) {
      this.minPoolSize = minPoolSize;
      return this;
    }

    public Builder maxIdleTime(Integer maxIdleTime) {
      this.maxIdleTime = maxIdleTime;
      return this;
    }

    public Builder registerJmxPool(Boolean registerJmxPool) {
      this.registerJmxPool = registerJmxPool;
      return this;
    }

    public Builder poolValidMinDelay(Integer poolValidMinDelay) {
      this.poolValidMinDelay = poolValidMinDelay;
      return this;
    }

    public Builder useResetConnection(Boolean useResetConnection) {
      this.useResetConnection = useResetConnection;
      return this;
    }

    /**
     * Cache all socket available information.
     *
     * @param useReadAheadInput cache available socket data when reading socket.
     * @return this {@link Builder}
     */
    public Builder useReadAheadInput(Boolean useReadAheadInput) {
      this.useReadAheadInput = useReadAheadInput;
      return this;
    }

    /**
     * Cache server prepare result
     *
     * @param cachePrepStmts cache server prepared result
     * @return this {@link Builder}
     */
    public Builder cachePrepStmts(Boolean cachePrepStmts) {
      this.cachePrepStmts = cachePrepStmts;
      return this;
    }

    /**
     * Must cache commands in transaction and replay transaction on failover.
     *
     * @param transactionReplay cache transaction and replay on failover
     * @return this {@link Builder}
     */
    public Builder transactionReplay(Boolean transactionReplay) {
      this.transactionReplay = transactionReplay;
      return this;
    }

    /**
     * Transaction replay cache size
     *
     * @param transactionReplaySize transaction replay cache size
     * @return this {@link Builder}
     */
    public Builder transactionReplaySize(Integer transactionReplaySize) {
      this.transactionReplaySize = transactionReplaySize;
      return this;
    }

    public Builder useMysqlVersion(Boolean useMysqlVersion) {
      this.useMysqlVersion = useMysqlVersion;
      return this;
    }

    public Builder rewriteBatchedStatements(Boolean rewriteBatchedStatements) {
      this.rewriteBatchedStatements = rewriteBatchedStatements;
      return this;
    }

    /**
     * Set preserveInstants.
     *
     * @param preserveInstants when true, OffsetDateTime parameters are encoded as {@code
     *     FROM_UNIXTIME(epoch)} preserving the UTC instant
     * @return this {@link Builder}
     */
    public Builder preserveInstants(Boolean preserveInstants) {
      this.preserveInstants = preserveInstants;
      return this;
    }

    public Builder consoleLogLevel(String consoleLogLevel) {
      this.consoleLogLevel = consoleLogLevel;
      return this;
    }

    public Builder consoleLogFilepath(String consoleLogFilepath) {
      this.consoleLogFilepath = consoleLogFilepath;
      return this;
    }

    public Builder printStackTrace(Boolean printStackTrace) {
      this.printStackTrace = printStackTrace;
      return this;
    }

    public Builder maxPrintStackSizeToLog(Integer maxPrintStackSizeToLog) {
      this.maxPrintStackSizeToLog = maxPrintStackSizeToLog;
      return this;
    }

    /**
     * Enable protocol extended types response.
     *
     * @param enableExtendedDataTypes to enable extended type like Vector, Bson, etc...
     * @return this {@link Builder}
     */
    public Builder enableExtendedDataTypes(Boolean enableExtendedDataTypes) {
      this.enableExtendedDataTypes = enableExtendedDataTypes;
      return this;
    }

    /**
     * Sets Vector type output format as JSON or BINARY.
     *
     * @param vectorTypeOutputFormat Vector type output format
     * @return this {@link Builder}
     */
    public Builder vectorTypeOutputFormat(String vectorTypeOutputFormat) {
      this.vectorTypeOutputFormat = vectorTypeOutputFormat;
      return this;
    }

    /**
     * Enable extended metadata for {@code VECTOR(<N> [, <elementType>])} data type.
     *
     * @param vectorExtendedMetadata to enable extended metadata for VECTOR
     * @return this {@link Builder}
     */
    public Builder vectorExtendedMetadata(Boolean vectorExtendedMetadata) {
      this.vectorExtendedMetadata = vectorExtendedMetadata;
      return this;
    }

    /**
     * Build a configuration
     *
     * @return a Configuration object
     */
    public Configuration build() {
      Configuration conf = new Configuration(this);
      conf.initialUrl = buildUrl(conf);
      return conf;
    }
  }

  private static String nullOrEmpty(String val) {
    return (val == null || val.isEmpty()) ? null : val;
  }
}
