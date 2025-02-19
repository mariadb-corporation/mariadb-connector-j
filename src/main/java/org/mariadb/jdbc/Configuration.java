// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2025 MariaDB Corporation Ab
package org.mariadb.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.*;
import org.mariadb.jdbc.export.HaMode;
import org.mariadb.jdbc.export.SslMode;
import org.mariadb.jdbc.plugin.Codec;
import org.mariadb.jdbc.plugin.CredentialPlugin;
import org.mariadb.jdbc.plugin.credential.CredentialPluginLoader;
import org.mariadb.jdbc.util.constants.CatalogTerm;
import org.mariadb.jdbc.util.log.Logger;
import org.mariadb.jdbc.util.log.Loggers;
import org.mariadb.jdbc.util.options.OptionAliases;

/**
 * parse and verification of URL.
 *
 * <p>basic syntax :<br>
 * {@code
 * jdbc:mariadb:[replication:|failover|loadbalance:|aurora:]//<hostDescription>[,<hostDescription>]/[database>]
 * [?<key1>=<value1>[&<key2>=<value2>]] }
 *
 * <p>hostDescription:<br>
 * - simple :<br>
 * {@code <host>:<portnumber>}<br>
 * (for example localhost:3306)<br>
 * <br>
 * - complex :<br>
 * {@code address=[(type=(master|slave))][(port=<portnumber>)](host=<host>)}<br>
 * <br>
 * <br>
 * type is by default master<br>
 * port is by default 3306<br>
 *
 * <p>host can be dns name, ipv4 or ipv6.<br>
 * in case of ipv6 and simple host description, the ip must be written inside bracket.<br>
 * exemple : {@code jdbc:mariadb://[2001:0660:7401:0200:0000:0000:0edf:bdd7]:3306}<br>
 *
 * <p>Some examples :<br>
 * {@code jdbc:mariadb://localhost:3306/database?user=greg&password=pass}<br>
 * {@code
 * jdbc:mariadb://address=(type=master)(host=master1),address=(port=3307)(type=slave)(host=slave1)/database?user=greg&password=pass}
 * <br>
 */
public class Configuration {
  private static final Logger logger = Loggers.getLogger(Configuration.class);

  private static final Set<String> EXCLUDED_FIELDS;
  private static final Set<String> SECURE_FIELDS;
  private static final Set<String> PROPERTIES_TO_SKIP;
  private static final Set<String> SENSITIVE_FIELDS;
  private static final String CATALOG_TERM = "CATALOG";
  private static final String SCHEMA_TERM = "SCHEMA";

  static {
    EXCLUDED_FIELDS = new HashSet<>();
    EXCLUDED_FIELDS.add("database");
    EXCLUDED_FIELDS.add("haMode");
    EXCLUDED_FIELDS.add("$jacocoData");
    EXCLUDED_FIELDS.add("addresses");

    SECURE_FIELDS = new HashSet<>();
    SECURE_FIELDS.add("password");
    SECURE_FIELDS.add("keyStorePassword");
    SECURE_FIELDS.add("trustStorePassword");

    PROPERTIES_TO_SKIP = new HashSet<>();
    PROPERTIES_TO_SKIP.add("initialUrl");
    PROPERTIES_TO_SKIP.add("logger");
    PROPERTIES_TO_SKIP.add("codecs");
    PROPERTIES_TO_SKIP.add("$jacocoData");
    PROPERTIES_TO_SKIP.add("CATALOG_TERM");
    PROPERTIES_TO_SKIP.add("SCHEMA_TERM");

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
  private String timezone;
  private String connectionCollation;
  private String connectionTimeZone;
  private Boolean forceConnectionTimeZoneToSession;
  private boolean preserveInstants;
  private Boolean autocommit;
  private boolean useMysqlMetadata;
  private boolean nullDatabaseMeansCurrent;
  private CatalogTerm useCatalogTerm;
  private boolean createDatabaseIfNotExist;
  private boolean useLocalSessionState;
  private boolean returnMultiValuesGeneratedIds;
  private boolean jdbcCompliantTruncation;
  private boolean oldModeNoPrecisionTimestamp;
  private boolean permitRedirect;
  private TransactionIsolation transactionIsolation;
  private int defaultFetchSize;
  private int maxQuerySizeToLog;
  private Integer maxAllowedPacket;
  private String geometryDefaultType;
  private String restrictedAuth;
  private String initSql;
  private boolean pinGlobalTxToPhysicalConnection;
  private boolean permitNoResults;

  // socket
  private String socketFactory;
  private int connectTimeout;
  private String pipe;
  private String localSocket;
  private boolean uuidAsString;
  private boolean tcpKeepAlive;
  private int tcpKeepIdle;
  private int tcpKeepCount;
  private int tcpKeepInterval;
  private boolean tcpAbortiveClose;
  private String localSocketAddress;
  private int socketTimeout;
  private boolean useReadAheadInput;
  private String tlsSocketType;

  // SSL
  private SslMode sslMode;
  private String serverSslCert;
  private String keyStore;
  private String trustStore;
  private String keyStorePassword;
  private String trustStorePassword;
  private String keyPassword;
  private String keyStoreType;
  private String trustStoreType;
  private String enabledSslCipherSuites;
  private String enabledSslProtocolSuites;
  private boolean fallbackToSystemKeyStore;
  private boolean fallbackToSystemTrustStore;
  // protocol
  private boolean allowMultiQueries;
  private boolean allowLocalInfile;
  private boolean useCompression;
  private boolean useAffectedRows;
  private boolean useBulkStmts;
  private boolean useBulkStmtsForInserts;
  private boolean disablePipeline;
  // prepare
  private boolean cachePrepStmts;
  private int prepStmtCacheSize;
  private boolean useServerPrepStmts;

  // authentication
  private CredentialPlugin credentialType;
  private String sessionVariables;
  private String connectionAttributes;
  private String servicePrincipalName;
  private boolean disconnectOnExpiredPasswords;

  // meta
  private boolean blankTableNameMeta;
  private boolean tinyInt1isBit;
  private boolean transformedBitIsBoolean;
  private boolean yearIsDateType;
  private boolean dumpQueriesOnException;
  private boolean includeInnodbStatusInDeadlockExceptions;
  private boolean includeThreadDumpInDeadlockExceptions;

  // HA options
  private int retriesAllDown;
  private String galeraAllowedState;
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

  // MySQL sha authentication
  private String serverRsaPublicKeyFile;
  private boolean allowPublicKeyRetrieval;

  private Codec<?>[] codecs;

  private Configuration(Builder builder) {
    // Set basic configuration
    initializeBasicConfig(builder);

    // Set SSL/TLS configuration
    initializeSslConfig(builder);

    // Set socket configuration
    initializeSocketConfig(builder);

    // Set transaction and protocol settings
    initializeTransactionConfig(builder);

    // Set data type handling
    initializeDataTypeConfig(builder);

    // Set timezone settings
    initializeTimezoneConfig(builder);

    // Set query and statement handling
    initializeQueryConfig(builder);

    // Set bulk operations
    initializeBulkConfig(builder);

    // Set pipeline and transaction settings
    initializePipelineConfig(builder);

    // Set database and schema settings
    initializeDatabaseConfig(builder);

    // Set exception handling
    initializeExceptionConfig(builder);

    // Set pool configuration
    initializePoolConfig(builder);

    // Set security settings
    initializeSecurityConfig(builder);

    // Set additional properties
    initializeAdditionalConfig(builder);

    // Configure hosts
    configureHosts();

    // Validate configuration
    validateConfiguration();
  }

  private void initializeBasicConfig(Builder builder) {
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
    this.fallbackToSystemKeyStore =
        builder.fallbackToSystemKeyStore == null || builder.fallbackToSystemKeyStore;
    this.fallbackToSystemTrustStore =
        builder.fallbackToSystemTrustStore == null || builder.fallbackToSystemTrustStore;
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
    this.uuidAsString = builder.uuidAsString != null && builder.uuidAsString;
    this.tcpKeepIdle = builder.tcpKeepIdle != null ? builder.tcpKeepIdle : 0;
    this.tcpKeepCount = builder.tcpKeepCount != null ? builder.tcpKeepCount : 0;
    this.tcpKeepInterval = builder.tcpKeepInterval != null ? builder.tcpKeepInterval : 0;
    this.tcpAbortiveClose = builder.tcpAbortiveClose != null && builder.tcpAbortiveClose;
    this.localSocketAddress = builder.localSocketAddress;
    this.socketTimeout = builder.socketTimeout != null ? builder.socketTimeout : 0;
    this.useReadAheadInput = builder.useReadAheadInput != null && builder.useReadAheadInput;
    this.tlsSocketType = builder.tlsSocketType;
    this.useCompression = builder.useCompression != null && builder.useCompression;
  }

  private void initializeTransactionConfig(Builder builder) {
    this.transactionIsolation =
        builder.transactionIsolation != null
            ? TransactionIsolation.from(builder.transactionIsolation)
            : null;
    this.enabledSslCipherSuites = builder.enabledSslCipherSuites;
    this.sessionVariables = builder.sessionVariables;
  }

  private void initializeDataTypeConfig(Builder builder) {
    this.tinyInt1isBit = builder.tinyInt1isBit == null || builder.tinyInt1isBit;
    this.transformedBitIsBoolean =
        builder.transformedBitIsBoolean == null || builder.transformedBitIsBoolean;
    this.yearIsDateType = builder.yearIsDateType == null || builder.yearIsDateType;
  }

  private void initializeTimezoneConfig(Builder builder) {
    this.timezone = builder.timezone;
    this.connectionTimeZone = builder.connectionTimeZone;
    this.connectionCollation = builder.connectionCollation;
    this.forceConnectionTimeZoneToSession = builder.forceConnectionTimeZoneToSession;
    this.preserveInstants = builder.preserveInstants != null && builder.preserveInstants;
  }

  private void initializeQueryConfig(Builder builder) {
    this.dumpQueriesOnException =
        builder.dumpQueriesOnException != null && builder.dumpQueriesOnException;
    this.prepStmtCacheSize = builder.prepStmtCacheSize != null ? builder.prepStmtCacheSize : 250;
    this.useAffectedRows = builder.useAffectedRows != null && builder.useAffectedRows;
    this.useServerPrepStmts = builder.useServerPrepStmts != null && builder.useServerPrepStmts;
    this.connectionAttributes = builder.connectionAttributes;
    this.allowLocalInfile = builder.allowLocalInfile == null || builder.allowLocalInfile;
    this.allowMultiQueries = builder.allowMultiQueries != null && builder.allowMultiQueries;
  }

  private void initializeBulkConfig(Builder builder) {
    this.useBulkStmts = builder.useBulkStmts != null && builder.useBulkStmts;
    this.useBulkStmtsForInserts =
        builder.useBulkStmtsForInserts != null
            ? builder.useBulkStmtsForInserts
            : (builder.useBulkStmts == null || builder.useBulkStmts);
  }

  private void initializePipelineConfig(Builder builder) {
    this.disablePipeline = builder.disablePipeline != null && builder.disablePipeline;
    this.autocommit = builder.autocommit;
    this.useMysqlMetadata = builder.useMysqlMetadata != null && builder.useMysqlMetadata;
    this.nullDatabaseMeansCurrent =
        builder.nullDatabaseMeansCurrent != null && builder.nullDatabaseMeansCurrent;
  }

  private void initializeDatabaseConfig(Builder builder) {
    if (builder.useCatalogTerm != null) {
      if (!CATALOG_TERM.equalsIgnoreCase(builder.useCatalogTerm)
          && !SCHEMA_TERM.equalsIgnoreCase(builder.useCatalogTerm)) {
        throw new IllegalArgumentException(
            "useCatalogTerm can only have CATALOG/SCHEMA value, current set value is "
                + builder.useCatalogTerm);
      }
      this.useCatalogTerm =
          CATALOG_TERM.equalsIgnoreCase(builder.useCatalogTerm)
              ? CatalogTerm.UseCatalog
              : CatalogTerm.UseSchema;
    } else {
      this.useCatalogTerm = CatalogTerm.UseCatalog;
    }

    this.createDatabaseIfNotExist =
        builder.createDatabaseIfNotExist != null && builder.createDatabaseIfNotExist;
    this.useLocalSessionState =
        builder.useLocalSessionState != null && builder.useLocalSessionState;
    this.returnMultiValuesGeneratedIds =
        builder.returnMultiValuesGeneratedIds != null && builder.returnMultiValuesGeneratedIds;
    this.jdbcCompliantTruncation =
        builder.jdbcCompliantTruncation == null || builder.jdbcCompliantTruncation;
    this.oldModeNoPrecisionTimestamp =
        builder.oldModeNoPrecisionTimestamp != null && builder.oldModeNoPrecisionTimestamp;
    this.permitRedirect = builder.permitRedirect == null || builder.permitRedirect;
    this.pinGlobalTxToPhysicalConnection =
        builder.pinGlobalTxToPhysicalConnection != null && builder.pinGlobalTxToPhysicalConnection;
    this.permitNoResults = builder.permitNoResults == null || builder.permitNoResults;
    this.blankTableNameMeta = builder.blankTableNameMeta != null && builder.blankTableNameMeta;
    this.disconnectOnExpiredPasswords =
        builder.disconnectOnExpiredPasswords == null || builder.disconnectOnExpiredPasswords;
  }

  private void initializeExceptionConfig(Builder builder) {
    this.includeInnodbStatusInDeadlockExceptions =
        builder.includeInnodbStatusInDeadlockExceptions != null
            && builder.includeInnodbStatusInDeadlockExceptions;
    this.includeThreadDumpInDeadlockExceptions =
        builder.includeThreadDumpInDeadlockExceptions != null
            && builder.includeThreadDumpInDeadlockExceptions;
  }

  private void initializePoolConfig(Builder builder) {
    this.pool = builder.pool != null && builder.pool;
    this.poolName = builder.poolName;
    this.maxPoolSize = builder.maxPoolSize != null ? builder.maxPoolSize : 8;
    this.minPoolSize = builder.minPoolSize != null ? builder.minPoolSize : this.maxPoolSize;
    this.maxIdleTime = builder.maxIdleTime != null ? builder.maxIdleTime : 600_000;
    this.registerJmxPool = builder.registerJmxPool == null || builder.registerJmxPool;
    this.poolValidMinDelay = builder.poolValidMinDelay != null ? builder.poolValidMinDelay : 1000;
    this.useResetConnection = builder.useResetConnection != null && builder.useResetConnection;
  }

  private void initializeSecurityConfig(Builder builder) {
    this.serverRsaPublicKeyFile =
        builder.serverRsaPublicKeyFile != null && !builder.serverRsaPublicKeyFile.isEmpty()
            ? builder.serverRsaPublicKeyFile
            : null;
    this.allowPublicKeyRetrieval =
        builder.allowPublicKeyRetrieval != null && builder.allowPublicKeyRetrieval;
  }

  private void initializeAdditionalConfig(Builder builder) {
    this.servicePrincipalName = builder.servicePrincipalName;
    this.defaultFetchSize = builder.defaultFetchSize != null ? builder.defaultFetchSize : 0;
    this.tlsSocketType = builder.tlsSocketType;
    this.maxQuerySizeToLog = builder.maxQuerySizeToLog != null ? builder.maxQuerySizeToLog : 1024;
    this.maxAllowedPacket = builder.maxAllowedPacket;
    this.retriesAllDown = builder.retriesAllDown != null ? builder.retriesAllDown : 120;
    this.galeraAllowedState = builder.galeraAllowedState;
    this.cachePrepStmts = builder.cachePrepStmts == null || builder.cachePrepStmts;
    this.transactionReplay = builder.transactionReplay != null && builder.transactionReplay;
    this.transactionReplaySize =
        builder.transactionReplaySize != null ? builder.transactionReplaySize : 64;
    this.geometryDefaultType = builder.geometryDefaultType;
    this.restrictedAuth = builder.restrictedAuth;
    this.initSql = builder.initSql;
    this.codecs = null;
  }

  private void configureHosts() {
    if (addresses.isEmpty()) {
      if (this.localSocket != null) {
        addresses.add(HostAddress.localSocket(this.localSocket));
      } else if (this.pipe != null) {
        addresses.add(HostAddress.pipe(this.pipe));
      }
    } else {
      if (this.localSocket != null) {
        List<HostAddress> newAddresses = new ArrayList<>();
        for (HostAddress host : addresses) {
          newAddresses.add(host.withLocalSocket(this.localSocket));
        }
        this.addresses = newAddresses;
      }
      if (this.pipe != null) {
        List<HostAddress> newAddresses = new ArrayList<>();
        for (HostAddress host : addresses) {
          newAddresses.add(host.withPipe(this.pipe));
        }
        this.addresses = newAddresses;
      }
    }

    // Configure host primary settings
    boolean first = true;
    for (HostAddress host : addresses) {
      boolean primary = haMode != HaMode.REPLICATION || first;
      if (host.primary == null) {
        host.primary = primary;
      }
      first = false;
    }
  }

  private void validateConfiguration() {
    // Validate timezone settings
    if (this.timezone != null && this.connectionTimeZone == null) {
      if ("disable".equalsIgnoreCase(this.timezone)) {
        this.forceConnectionTimeZoneToSession = false;
      } else {
        this.forceConnectionTimeZoneToSession = true;
        if (!"auto".equalsIgnoreCase(this.timezone)) {
          this.connectionTimeZone = this.timezone;
        }
      }
    }

    // Validate connection collation
    if (connectionCollation != null) {
      if (connectionCollation.trim().isEmpty()) {
        this.connectionCollation = null;
      } else {
        if (!connectionCollation.toLowerCase(Locale.ROOT).startsWith("utf8mb4_")) {
          throw new IllegalArgumentException(
              String.format(
                  "wrong connection collation '%s' only utf8mb4 collation are accepted",
                  connectionCollation));
        } else if (!connectionCollation.matches("\\w+$")) {
          throw new IllegalArgumentException(
              String.format("wrong connection collation '%s' name", connectionCollation));
        }
      }
    }

    // Validate integer fields
    validateIntegerFields();
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
            .timezone(this.timezone)
            .connectionTimeZone(this.connectionTimeZone)
            .connectionCollation(this.connectionCollation)
            .forceConnectionTimeZoneToSession(this.forceConnectionTimeZoneToSession)
            .preserveInstants(this.preserveInstants)
            .autocommit(this.autocommit)
            .useMysqlMetadata(this.useMysqlMetadata)
            .nullDatabaseMeansCurrent(this.nullDatabaseMeansCurrent)
            .useCatalogTerm(
                this.useCatalogTerm == CatalogTerm.UseCatalog ? CATALOG_TERM : SCHEMA_TERM)
            .createDatabaseIfNotExist(this.createDatabaseIfNotExist)
            .useLocalSessionState(this.useLocalSessionState)
            .returnMultiValuesGeneratedIds(this.returnMultiValuesGeneratedIds)
            .jdbcCompliantTruncation(this.jdbcCompliantTruncation)
            .oldModeNoPrecisionTimestamp(this.oldModeNoPrecisionTimestamp)
            .permitRedirect(this.permitRedirect)
            .pinGlobalTxToPhysicalConnection(this.pinGlobalTxToPhysicalConnection)
            .permitNoResults(this.permitNoResults)
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
            .uuidAsString(this.uuidAsString)
            .tcpKeepAlive(this.tcpKeepAlive)
            .tcpKeepIdle(this.tcpKeepIdle)
            .tcpKeepCount(this.tcpKeepCount)
            .tcpKeepInterval(this.tcpKeepInterval)
            .tcpAbortiveClose(this.tcpAbortiveClose)
            .localSocketAddress(this.localSocketAddress)
            .socketTimeout(this.socketTimeout)
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
            .fallbackToSystemKeyStore(this.fallbackToSystemKeyStore)
            .fallbackToSystemTrustStore(this.fallbackToSystemTrustStore)
            .allowMultiQueries(this.allowMultiQueries)
            .allowLocalInfile(this.allowLocalInfile)
            .useCompression(this.useCompression)
            .useAffectedRows(this.useAffectedRows)
            .useBulkStmts(this.useBulkStmts)
            .useBulkStmtsForInserts(this.useBulkStmtsForInserts)
            .disablePipeline(this.disablePipeline)
            .cachePrepStmts(this.cachePrepStmts)
            .prepStmtCacheSize(this.prepStmtCacheSize)
            .useServerPrepStmts(this.useServerPrepStmts)
            .credentialType(this.credentialType == null ? null : this.credentialType.type())
            .sessionVariables(this.sessionVariables)
            .connectionAttributes(this.connectionAttributes)
            .servicePrincipalName(this.servicePrincipalName)
            .blankTableNameMeta(this.blankTableNameMeta)
            .disconnectOnExpiredPasswords(this.disconnectOnExpiredPasswords)
            .tinyInt1isBit(this.tinyInt1isBit)
            .transformedBitIsBoolean(this.transformedBitIsBoolean)
            .yearIsDateType(this.yearIsDateType)
            .dumpQueriesOnException(this.dumpQueriesOnException)
            .includeInnodbStatusInDeadlockExceptions(this.includeInnodbStatusInDeadlockExceptions)
            .includeThreadDumpInDeadlockExceptions(this.includeThreadDumpInDeadlockExceptions)
            .retriesAllDown(this.retriesAllDown)
            .galeraAllowedState(this.galeraAllowedState)
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
            .serverRsaPublicKeyFile(this.serverRsaPublicKeyFile)
            .allowPublicKeyRetrieval(this.allowPublicKeyRetrieval);
    builder._nonMappedOptions = this.nonMappedOptions;
    return builder;
  }

  /**
   * Tell if mariadb driver accept url string. (Correspond to interface
   * java.jdbc.Driver.acceptsURL() method)
   *
   * @param url url String
   * @return true if url string correspond.
   */
  public static boolean acceptsUrl(String url) {
    return url != null
        && (url.startsWith("jdbc:mariadb:")
            || (url.startsWith("jdbc:mysql:") && url.contains("permitMysqlScheme")));
  }

  /**
   * parse connection string
   *
   * @param url connection string
   * @return configuration resulting object
   * @throws SQLException if not supported driver or wrong connection string format.
   */
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
        logger.warn(deprecatedDesc.getProperty("useSsl"));

        if (isSet("trustServerCertificate", nonMappedOptions)) {
          builder.sslMode("trust");
          logger.warn(deprecatedDesc.getProperty("trustServerCertificate"));
        } else if (isSet("disableSslHostnameVerification", nonMappedOptions)) {
          logger.warn(deprecatedDesc.getProperty("disableSslHostnameVerification"));
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
    // parser is sure to have at least 2 colon, since jdbc:[mysql|mariadb]: is tested.
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
   * Configuration.toConf("jdbc:mariadb://localhost/test") will return a String containing: <code>
   * Configuration:
   *  * resulting Url : jdbc:mariadb://localhost/test
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
    Configuration defaultConf = Configuration.parse("jdbc:mariadb://localhost/");

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
      case "CatalogTerm":
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

  /**
   * Builds a JDBC URL from the provided configuration.
   *
   * @param conf Current configuration
   * @return Complete JDBC URL string
   */
  protected static String buildUrl(Configuration conf) {
    try {
      StringBuilder urlBuilder = new StringBuilder("jdbc:mariadb:");
      appendHaModeIfPresent(urlBuilder, conf);
      appendHostAddresses(urlBuilder, conf);
      appendDatabase(urlBuilder, conf);
      appendConfigurationParameters(urlBuilder, conf);

      conf.loadCodecs();
      return urlBuilder.toString();
    } catch (SecurityException s) {
      throw new IllegalArgumentException("Security too restrictive: " + s.getMessage());
    }
  }

  private static void appendHostAddresses(StringBuilder sb, Configuration conf) {
    sb.append("//");
    for (int i = 0; i < conf.addresses.size(); i++) {
      if (i > 0) sb.append(",");
      appendHostAddress(sb, conf, conf.addresses.get(i), i);
    }
    sb.append("/");
  }

  private static void appendHostAddress(
      StringBuilder sb, Configuration conf, HostAddress hostAddress, int index) {
    boolean useSimpleFormat = shouldUseSimpleHostFormat(conf, hostAddress, index);

    if (useSimpleFormat) {
      sb.append(hostAddress.host);
      if (hostAddress.port != 3306) {
        sb.append(":").append(hostAddress.port);
      }
    } else {
      sb.append(hostAddress);
    }
  }

  private static boolean shouldUseSimpleHostFormat(
      Configuration conf, HostAddress hostAddress, int index) {
    return (conf.haMode == HaMode.NONE && hostAddress.primary)
        || (conf.haMode == HaMode.REPLICATION
            && ((index == 0 && hostAddress.primary) || (index != 0 && !hostAddress.primary)));
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
    } else if (fieldType.equals(CatalogTerm.class)) {
      appendCatalogTermParameter(appender, field, value, defaultConf);
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

  private static void appendCatalogTermParameter(
      ParameterAppender appender, Field field, Object value, Configuration defaultConf)
      throws IllegalAccessException {
    Object defaultValue = field.get(defaultConf);
    if (!value.equals(defaultValue)) {
      appender.appendParameter(field.getName(), SCHEMA_TERM);
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

  private static String nullOrEmpty(String val) {
    return (val == null || val.isEmpty()) ? null : val;
  }

  /**
   * Clone configuration with another user/password
   *
   * @param username new username
   * @param password new password
   * @return new cloned configuration object
   */
  public Configuration clone(String username, String password) {
    return this.toBuilder()
        .user(username != null && username.isEmpty() ? null : username)
        .password(password != null && password.isEmpty() ? null : password)
        .build();
  }

  public boolean havePrimaryHostOnly() {
    for (HostAddress hostAddress : this.addresses) {
      if (!hostAddress.primary) return false;
    }
    return true;
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
   * key store
   *
   * @return key store
   */
  public String keyStore() {
    return keyStore;
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
   * key store password
   *
   * @return key store password
   */
  public String keyStorePassword() {
    return keyStorePassword;
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
   * trust store type (to replace default javax.net.ssl.keyStoreType system property)
   *
   * @return trust store type
   */
  public String trustStoreType() {
    return trustStoreType;
  }

  /**
   * permitted ssl protocol list (comma separated)
   *
   * @return enabled ssl protocol list
   */
  public String enabledSslProtocolSuites() {
    return enabledSslProtocolSuites;
  }

  // do not remove, used with reflection
  public String credentialType() {
    return credentialType == null ? null : credentialType.type();
  }

  /**
   * Indicate if keyStore option is not set to use keystore system property like
   * "javax.net.ssl.keyStore"
   *
   * @return true if can use keystore system property
   */
  public boolean fallbackToSystemKeyStore() {
    return fallbackToSystemKeyStore;
  }

  /**
   * Indicate if system default truststore implementation can be used
   *
   * @return true if system default truststore implementation can be used
   */
  public boolean fallbackToSystemTrustStore() {
    return fallbackToSystemTrustStore;
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
   * must uuid fields return as String and not java.util.UUID
   *
   * @return must UUID return as String and not uuid
   */
  public boolean uuidAsString() {
    return uuidAsString;
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
   * On connection creation, indicate behavior when password is expired. When true (default) throw
   * an expired password error When false, connection succeed in "sandbox" mode, only queries
   * related to password change are allowed
   *
   * @return must connection fails on expired password
   */
  public boolean disconnectOnExpiredPasswords() {
    return disconnectOnExpiredPasswords;
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
   * Must tinyint(1) be considered as Bit
   *
   * @return true if tinyint(1) must be considered as Bit
   */
  public boolean tinyInt1isBit() {
    return tinyInt1isBit;
  }

  /**
   * Must tinyint(1) be considered as Boolean or Bit
   *
   * @return true if tinyint(1) must be considered as Boolean
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
   * Set timezone
   *
   * @return timezone
   */
  public String timezone() {
    return timezone;
  }

  /**
   * Set connectionTimeZone
   *
   * @return connectionTimeZone
   */
  public String connectionTimeZone() {
    return connectionTimeZone;
  }

  /**
   * get connectionCollation
   *
   * @return connectionCollation
   */
  public String connectionCollation() {
    return connectionCollation;
  }

  /**
   * forceConnectionTimeZoneToSession must connection timezone be forced
   *
   * @return forceConnectionTimeZoneToSession
   */
  public Boolean forceConnectionTimeZoneToSession() {
    return forceConnectionTimeZoneToSession;
  }

  /**
   * Must timezone change preserve instants
   *
   * @return true if instants must be preserved
   */
  public boolean preserveInstants() {
    return preserveInstants;
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

  /**
   * Use server prepared statement. IF false, using client prepared statement.
   *
   * @return use server prepared statement
   */
  public boolean useServerPrepStmts() {
    return useServerPrepStmts;
  }

  /**
   * Connections attributes
   *
   * @return connection meta informations
   */
  public String connectionAttributes() {
    return connectionAttributes;
  }

  /**
   * Use server COM_STMT_BULK for batching.
   *
   * @return use server bulk command.
   */
  public boolean useBulkStmts() {
    return useBulkStmts;
  }

  /**
   * Use server COM_STMT_BULK for batching inserts. if useBulkStmts is enabled,
   * useBulkStmtsForInserts will be as well
   *
   * @return use server bulk command for inserts
   */
  public boolean useBulkStmtsForInserts() {
    return useBulkStmtsForInserts;
  }

  /**
   * Disable pipeline.
   *
   * @return is pipeline disabled.
   */
  public boolean disablePipeline() {
    return disablePipeline;
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
   * Force returning MySQL metadata information
   *
   * @return force returning MySQL in metadata
   */
  public boolean useMysqlMetadata() {
    return useMysqlMetadata;
  }

  /**
   * When enable, in DatabaseMetadata, will handle null database/schema (depending on
   * UseCatalog=catalog/schema) as current
   *
   * @return must null value be considered as current catalog/schema
   */
  public boolean nullDatabaseMeansCurrent() {
    return nullDatabaseMeansCurrent;
  }

  /**
   * Indicating using Catalog or Schema
   *
   * @return Indicating using Catalog or Schema
   */
  public CatalogTerm useCatalogTerm() {
    return useCatalogTerm;
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
   * use local state to avoid unnecessary queries. This means application must use JDBC dedicated
   * methods, like connection.setTransactionIsolation and never queries like "SET SESSION
   * TRANSACTION ISOLATION LEVEL X" directly
   *
   * @return can use local state
   */
  public boolean useLocalSessionState() {
    return useLocalSessionState;
  }

  /**
   * Returns multi-values generated ids. For mariadb 2.x connector compatibility
   *
   * @return must returns multi-values generated ids.
   */
  public boolean returnMultiValuesGeneratedIds() {
    return returnMultiValuesGeneratedIds;
  }

  /**
   * Force sql_mode to strict mode for JDBC compliance
   *
   * @return must force jdbc compliance
   */
  public boolean jdbcCompliantTruncation() {
    return jdbcCompliantTruncation;
  }

  /**
   * Force Timestamp string representation compatible to 2.7 version Timestamp string representation
   * will then correspond to Timestamp.toString() in place of taking field precision
   *
   * @return force 2.7 timestamp to string behavior
   */
  public boolean oldModeNoPrecisionTimestamp() {
    return oldModeNoPrecisionTimestamp;
  }

  /**
   * must client redirect when required
   *
   * @return must client redirect when required
   */
  public boolean permitRedirect() {
    return permitRedirect;
  }

  /**
   * When enabled, ensure that for XA operation to use the same connection
   *
   * @return pinGlobalTxToPhysicalConnection
   */
  public boolean pinGlobalTxToPhysicalConnection() {
    return pinGlobalTxToPhysicalConnection;
  }

  /**
   * Indicate if Statement/PreparedStatement.executeQuery for command that produce no result will
   * return an exception or just an empty result-set
   *
   * <p>When enabled, command not returning no data will end returning an empty result-set When
   * disabled, command not returning no data will end throwing an exception
   *
   * @return permitNoResults
   */
  public boolean permitNoResults() {
    return permitNoResults;
  }

  /**
   * On deadlock exception, must driver execute additional commands to show innodb status in error
   * description.
   *
   * @return includeInnodbStatusInDeadlockExceptions
   */
  public boolean includeInnodbStatusInDeadlockExceptions() {
    return includeInnodbStatusInDeadlockExceptions;
  }

  /**
   * On deadlock exception, must driver display threads information on error description.
   *
   * @return include Thread Dump In Deadlock Exceptions
   */
  public boolean includeThreadDumpInDeadlockExceptions() {
    return includeThreadDumpInDeadlockExceptions;
  }

  /**
   * Service principal name (GSSAPI option)
   *
   * @return service principal name
   */
  public String servicePrincipalName() {
    return servicePrincipalName;
  }

  /**
   * result-set streaming default fetch size
   *
   * @return Default fetch size.
   */
  public int defaultFetchSize() {
    return defaultFetchSize;
  }

  /**
   * non standard options
   *
   * @return non standard options
   */
  public Properties nonMappedOptions() {
    return nonMappedOptions;
  }

  /**
   * TLS socket type
   *
   * @return TLS socket type
   */
  public String tlsSocketType() {
    return tlsSocketType;
  }

  /**
   * query maximum size to log (query will be truncated of more than this limit)
   *
   * @return max query log size
   */
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

  /**
   * retry the maximum retry number of attempts to reconnect after a failover.
   *
   * @return the maximum retry number of attempts to reconnect after a failover.
   */
  public int retriesAllDown() {
    return retriesAllDown;
  }

  /**
   * Galera comma separated allowed state
   *
   * @return galera allowed state
   */
  public String galeraAllowedState() {
    return galeraAllowedState;
  }

  /**
   * Create pool
   *
   * @return create pool if don't exists
   */
  public boolean pool() {
    return pool;
  }

  /**
   * pool name
   *
   * @return pool name.
   */
  public String poolName() {
    return poolName;
  }

  /**
   * max pool size
   *
   * @return maximum pool size
   */
  public int maxPoolSize() {
    return maxPoolSize;
  }

  /**
   * Minimum pool size
   *
   * @return minimum pool size
   */
  public int minPoolSize() {
    return minPoolSize;
  }

  /**
   * Max idle time
   *
   * @return pool max idle time.
   */
  public int maxIdleTime() {
    return maxIdleTime;
  }

  /**
   * register pool information to JMX
   *
   * @return register pool to JMX
   */
  public boolean registerJmxPool() {
    return registerJmxPool;
  }

  /**
   * Pool mininum validation delay.
   *
   * @return pool validation delay
   */
  public int poolValidMinDelay() {
    return poolValidMinDelay;
  }

  /**
   * Must connection returned to pool be RESET
   *
   * @return use RESET on connection
   */
  public boolean useResetConnection() {
    return useResetConnection;
  }

  /**
   * Server RSA public key file for caching_sha2_password authentication
   *
   * @return server key file
   */
  public String serverRsaPublicKeyFile() {
    return serverRsaPublicKeyFile;
  }

  /**
   * permit mysql authentication to retrieve server certificate
   *
   * @return is driver allowed to retrieve server certificate from server
   */
  public boolean allowPublicKeyRetrieval() {
    return allowPublicKeyRetrieval;
  }

  /**
   * Read all data from socket in advance
   *
   * @return use read ahead buffer implementation
   */
  public boolean useReadAheadInput() {
    return useReadAheadInput;
  }

  /**
   * Cache prepared statement result.
   *
   * @return cache prepare results
   */
  public boolean cachePrepStmts() {
    return cachePrepStmts;
  }

  /**
   * implements transaction replay failover
   *
   * @return true if transaction must be replayed on failover.
   */
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

  /**
   * geometry default decoding implementation
   *
   * @return geometry default type
   */
  public String geometryDefaultType() {
    return geometryDefaultType;
  }

  /**
   * Restrict authentication plugin to comma separated plugin list
   *
   * @return authorized authentication list
   */
  public String restrictedAuth() {
    return restrictedAuth;
  }

  /**
   * Execute initial command when connection is established
   *
   * @return initial SQL command
   */
  public String initSql() {
    return initSql;
  }

  /**
   * datatype Encoder/decoder list
   *
   * @return codec list
   */
  public Codec<?>[] codecs() {
    return codecs;
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

    if (password == null) {
      return initialUrl.equals(that.initialUrl) && that.password == null;
    }
    return initialUrl.equals(that.initialUrl) && password.equals(that.password);
  }

  @SuppressWarnings("rawtypes")
  private void loadCodecs() {
    ServiceLoader<Codec> loader =
        ServiceLoader.load(Codec.class, Configuration.class.getClassLoader());
    List<Codec<?>> result = new ArrayList<>();
    loader.iterator().forEachRemaining(result::add);
    codecs = result.toArray(new Codec<?>[0]);
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
    private String timezone;
    private String connectionTimeZone;
    private String connectionCollation;
    private Boolean forceConnectionTimeZoneToSession;
    private Boolean preserveInstants;
    private Boolean autocommit;
    private Boolean useMysqlMetadata;
    private Boolean nullDatabaseMeansCurrent;
    private String useCatalogTerm;
    private Boolean createDatabaseIfNotExist;
    private Boolean useLocalSessionState;
    private Boolean returnMultiValuesGeneratedIds;
    private Boolean jdbcCompliantTruncation;
    private Boolean oldModeNoPrecisionTimestamp;
    private Boolean permitRedirect;
    private Boolean pinGlobalTxToPhysicalConnection;
    private Boolean permitNoResults;
    private Integer defaultFetchSize;
    private Integer maxQuerySizeToLog;
    private Integer maxAllowedPacket;
    private String geometryDefaultType;
    private String restrictedAuth;
    private String initSql;
    private String transactionIsolation;

    // socket
    private String socketFactory;
    private Integer connectTimeout;
    private String pipe;
    private String localSocket;
    private Boolean tcpKeepAlive;
    private Boolean uuidAsString;
    private Integer tcpKeepIdle;
    private Integer tcpKeepCount;
    private Integer tcpKeepInterval;
    private Boolean tcpAbortiveClose;
    private String localSocketAddress;
    private Integer socketTimeout;
    private Boolean useReadAheadInput;
    private String tlsSocketType;

    // SSL
    private String sslMode;
    private String serverSslCert;
    private String keyStore;
    private String trustStore;
    private String keyStorePassword;
    private String trustStorePassword;
    private String keyPassword;
    private String keyStoreType;
    private String trustStoreType;
    private String enabledSslCipherSuites;
    private String enabledSslProtocolSuites;
    private Boolean fallbackToSystemKeyStore;
    private Boolean fallbackToSystemTrustStore;
    // protocol
    private Boolean allowMultiQueries;
    private Boolean allowLocalInfile;
    private Boolean useCompression;
    private Boolean useAffectedRows;
    private Boolean useBulkStmts;
    private Boolean useBulkStmtsForInserts;
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
    private Boolean disconnectOnExpiredPasswords;

    // meta
    private Boolean blankTableNameMeta;
    private Boolean tinyInt1isBit;

    private Boolean transformedBitIsBoolean;
    private Boolean yearIsDateType;
    private Boolean dumpQueriesOnException;
    private Boolean includeInnodbStatusInDeadlockExceptions;
    private Boolean includeThreadDumpInDeadlockExceptions;

    // HA options
    private Integer retriesAllDown;
    private String galeraAllowedState;
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

    // MySQL sha authentication
    private String serverRsaPublicKeyFile;
    private Boolean allowPublicKeyRetrieval;

    /**
     * set user to authenticate to server
     *
     * @param user user
     * @return this {@link Builder}
     */
    public Builder user(String user) {
      this.user = nullOrEmpty(user);
      return this;
    }

    /**
     * Server SSL certificate (path or file content)
     *
     * @param serverSslCert set Server SSL certificate (path or file content)
     * @return this {@link Builder}
     */
    public Builder serverSslCert(String serverSslCert) {
      this.serverSslCert = nullOrEmpty(serverSslCert);
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
     * File path of the trustStore file that contain trusted certificates (similar to java System
     * property \"javax.net.ssl.trustStore\")
     *
     * @param trustStore client trust store certificates
     * @return this {@link Builder}
     */
    public Builder trustStore(String trustStore) {
      this.trustStore = nullOrEmpty(trustStore);
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
     * Client truststore password
     *
     * @param trustStorePassword client truststore password
     * @return this {@link Builder}
     */
    public Builder trustStorePassword(String trustStorePassword) {
      this.trustStorePassword = nullOrEmpty(trustStorePassword);
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

    /**
     * Key store type
     *
     * @param keyStoreType key store type
     * @return this {@link Builder}
     */
    public Builder keyStoreType(String keyStoreType) {
      this.keyStoreType = nullOrEmpty(keyStoreType);
      return this;
    }

    /**
     * trust store type
     *
     * @param trustStoreType trust store type
     * @return this {@link Builder}
     */
    public Builder trustStoreType(String trustStoreType) {
      this.trustStoreType = nullOrEmpty(trustStoreType);
      return this;
    }

    /**
     * User password
     *
     * @param password password
     * @return this {@link Builder}
     */
    public Builder password(String password) {
      this.password = nullOrEmpty(password);
      return this;
    }

    /**
     * Set ssl protocol list to user (comma separated)
     *
     * @param enabledSslProtocolSuites set possible SSL(TLS) protocol to use
     * @return this {@link Builder}
     */
    public Builder enabledSslProtocolSuites(String enabledSslProtocolSuites) {
      this.enabledSslProtocolSuites = nullOrEmpty(enabledSslProtocolSuites);
      return this;
    }

    /**
     * Indicate if keystore system properties can be used.
     *
     * @param fallbackToSystemKeyStore set if keystore system properties can be used.
     * @return this {@link Builder}
     */
    public Builder fallbackToSystemKeyStore(Boolean fallbackToSystemKeyStore) {
      this.fallbackToSystemKeyStore = fallbackToSystemKeyStore;
      return this;
    }

    /**
     * Indicate if system default truststore can be used.
     *
     * @param fallbackToSystemTrustStore indicate if system default truststore can be used..
     * @return this {@link Builder}
     */
    public Builder fallbackToSystemTrustStore(Boolean fallbackToSystemTrustStore) {
      this.fallbackToSystemTrustStore = fallbackToSystemTrustStore;
      return this;
    }

    /**
     * Set default database
     *
     * @param database database
     * @return this {@link Builder}
     */
    public Builder database(String database) {
      this.database = database;
      return this;
    }

    /**
     * Set failover High-availability mode
     *
     * @param haMode High-availability mode
     * @return this {@link Builder}
     */
    public Builder haMode(HaMode haMode) {
      this._haMode = haMode;
      return this;
    }

    /**
     * Add Host to possible addresses to connect
     *
     * @param host hostname or IP
     * @param port port
     * @return this {@link Builder}
     */
    public Builder addHost(String host, int port) {
      this._addresses.add(HostAddress.from(nullOrEmpty(host), port));
      return this;
    }

    /**
     * Add Host to possible addresses to connect
     *
     * @param host hostname or IP
     * @param port port
     * @param sslMode ssl mode. possible values disable/trust/verify-ca/verify-full
     * @return this {@link Builder}
     */
    public Builder addHost(String host, int port, String sslMode) {
      this._addresses.add(HostAddress.from(nullOrEmpty(host), port, sslMode));
      return this;
    }

    /**
     * Add Host to possible addresses to connect
     *
     * @param host hostname or IP
     * @param port port
     * @param master is master or replica
     * @return this {@link Builder}
     */
    public Builder addHost(String host, int port, boolean master) {
      this._addresses.add(HostAddress.from(nullOrEmpty(host), port, master));
      return this;
    }

    /**
     * Add Host to possible addresses to connect
     *
     * @param host hostname or IP
     * @param port port
     * @param master is master or replica
     * @param sslMode ssl mode. possible values disable/trust/verify-ca/verify-full
     * @return this {@link Builder}
     */
    public Builder addHost(String host, int port, boolean master, String sslMode) {
      this._addresses.add(HostAddress.from(nullOrEmpty(host), port, master, sslMode));
      return this;
    }

    /**
     * Add a windows pipe host
     *
     * @param pipe windows pipe path
     * @return this {@link Builder}
     */
    public Builder addPipeHost(String pipe) {
      this._addresses.add(HostAddress.pipe(pipe));
      return this;
    }

    /**
     * Add a unix socket host
     *
     * @param localSocket unix socket path
     * @return this {@link Builder}
     */
    public Builder addLocalSocketHost(String localSocket) {
      this._addresses.add(HostAddress.localSocket(localSocket));
      return this;
    }

    /**
     * add host addresses
     *
     * @param hostAddress host addresses
     * @return this {@link Builder}
     */
    public Builder addresses(HostAddress... hostAddress) {
      this._addresses = new ArrayList<>();
      this._addresses.addAll(Arrays.asList(hostAddress));
      return this;
    }

    /**
     * add host addresses
     *
     * @param hostAddress host addresses
     * @return this {@link Builder}
     */
    public Builder addresses(List<HostAddress> hostAddress) {
      this._addresses.addAll(hostAddress);
      return this;
    }

    /**
     * Socket factory
     *
     * @param socketFactory socket factory
     * @return this {@link Builder}
     */
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
     * Indicate if TCP keep-alive must be enabled.
     *
     * @param tcpKeepAlive value
     * @return this {@link Builder}
     */
    public Builder tcpKeepAlive(Boolean tcpKeepAlive) {
      this.tcpKeepAlive = tcpKeepAlive;
      return this;
    }

    /**
     * Indicate if UUID fields must returns as String
     *
     * @param uuidAsString value
     * @return this {@link Builder}
     */
    public Builder uuidAsString(Boolean uuidAsString) {
      this.uuidAsString = uuidAsString;
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
     *   <li>'default' will return org.mariadb.mariadb.jdbc.type Object
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
     * permit to execute an SQL command on connection creation
     *
     * @param initSql initial SQL command
     * @return this {@link Builder}
     */
    public Builder initSql(String initSql) {
      this.initSql = initSql;
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
     * Indicate to compress exchanges with the database through gzip. This permits better
     * performance when the database is not in the same location.
     *
     * @param useCompression to enable/disable compression
     * @return this {@link Builder}
     */
    public Builder useCompression(Boolean useCompression) {
      this.useCompression = useCompression;
      return this;
    }

    /**
     * Set blank table name for metadata (old oracle compatibility)
     *
     * @param blankTableNameMeta use blank table name
     * @return this {@link Builder}
     */
    public Builder blankTableNameMeta(Boolean blankTableNameMeta) {
      this.blankTableNameMeta = blankTableNameMeta;
      return this;
    }

    /**
     * On connection creation, indicate behavior when password is expired. When true (default) throw
     * an expired password error When false, connection succeed in "sandbox" mode, only queries
     * related to password change are allowed
     *
     * @return this {@link Builder}
     */
    public Builder disconnectOnExpiredPasswords(Boolean disconnectOnExpiredPasswords) {
      this.disconnectOnExpiredPasswords = disconnectOnExpiredPasswords;
      return this;
    }

    /**
     * set credential plugin type
     *
     * @param credentialType credential plugin type
     * @return this {@link Builder}
     */
    public Builder credentialType(String credentialType) {
      this.credentialType = nullOrEmpty(credentialType);
      return this;
    }

    /**
     * Set ssl model
     *
     * @param sslMode ssl requirement
     * @return this {@link Builder}
     */
    public Builder sslMode(String sslMode) {
      this.sslMode = sslMode;
      return this;
    }

    /**
     * force default transaction isolation, not using server default
     *
     * @param transactionIsolation indicate default transaction isolation
     * @return this {@link Builder}
     */
    public Builder transactionIsolation(String transactionIsolation) {
      this.transactionIsolation = nullOrEmpty(transactionIsolation);
      return this;
    }

    /**
     * set possible cipher list (comma separated), not using default java cipher list
     *
     * @param enabledSslCipherSuites ssl cipher list
     * @return this {@link Builder}
     */
    public Builder enabledSslCipherSuites(String enabledSslCipherSuites) {
      this.enabledSslCipherSuites = nullOrEmpty(enabledSslCipherSuites);
      return this;
    }

    /**
     * set connection session variables (comma separated)
     *
     * @param sessionVariables session variable list
     * @return this {@link Builder}
     */
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

    /**
     * Year datatype to be considered as date
     *
     * @param yearIsDateType indicate if Year must be considered as Date
     * @return this {@link Builder}
     */
    public Builder yearIsDateType(Boolean yearIsDateType) {
      this.yearIsDateType = yearIsDateType;
      return this;
    }

    /**
     * Force timezone to use this timezone, not default java one
     *
     * @param timezone default timezone
     * @return this {@link Builder}
     */
    public Builder timezone(String timezone) {
      this.timezone = nullOrEmpty(timezone);
      return this;
    }

    /**
     * indicate what timestamp timezone to use in exchanges with server possible value are
     * LOCAL|SERVER|user-defined time zone
     *
     * @param connectionTimeZone default timezone
     * @return this {@link Builder}
     */
    public Builder connectionTimeZone(String connectionTimeZone) {
      this.connectionTimeZone = nullOrEmpty(connectionTimeZone);
      return this;
    }

    /**
     * indicate what utf8mb4 collation to use. if not set, server default collation for utf8mb4 will
     * be used
     *
     * @param connectionCollation utf8mb4 collation to use
     * @return this {@link Builder}
     */
    public Builder connectionCollation(String connectionCollation) {
      this.connectionCollation = nullOrEmpty(connectionCollation);
      return this;
    }

    /**
     * Indicate if connectionTimeZone must be forced to session
     *
     * @param forceConnectionTimeZoneToSession must connector force connection timezone
     * @return this {@link Builder}
     */
    public Builder forceConnectionTimeZoneToSession(Boolean forceConnectionTimeZoneToSession) {
      this.forceConnectionTimeZoneToSession = forceConnectionTimeZoneToSession;
      return this;
    }

    /**
     * Indicate if connection must preserve instants
     *
     * @param preserveInstants must connector preserve instants
     * @return this {@link Builder}
     */
    public Builder preserveInstants(Boolean preserveInstants) {
      this.preserveInstants = preserveInstants;
      return this;
    }

    /**
     * Must queries be dump on exception stracktrace.
     *
     * @param dumpQueriesOnException must queries be dump on exception
     * @return this {@link Builder}
     */
    public Builder dumpQueriesOnException(Boolean dumpQueriesOnException) {
      this.dumpQueriesOnException = dumpQueriesOnException;
      return this;
    }

    /**
     * If using server prepared statement, set LRU prepare cache size
     *
     * @param prepStmtCacheSize prepare cache size
     * @return this {@link Builder}
     */
    public Builder prepStmtCacheSize(Integer prepStmtCacheSize) {
      this.prepStmtCacheSize = prepStmtCacheSize;
      return this;
    }

    /**
     * Indicate server to return affected rows in place of found rows. This impact the return number
     * of rows affected by update
     *
     * @param useAffectedRows Indicate to user affected rows in place of found rows
     * @return this {@link Builder}
     */
    public Builder useAffectedRows(Boolean useAffectedRows) {
      this.useAffectedRows = useAffectedRows;
      return this;
    }

    /**
     * Indicate to use Client or Server prepared statement
     *
     * @param useServerPrepStmts use Server prepared statement
     * @return this {@link Builder}
     */
    public Builder useServerPrepStmts(Boolean useServerPrepStmts) {
      this.useServerPrepStmts = useServerPrepStmts;
      return this;
    }

    /**
     * Additional connection attributes to identify connection
     *
     * @param connectionAttributes additional connection attributes
     * @return this {@link Builder}
     */
    public Builder connectionAttributes(String connectionAttributes) {
      this.connectionAttributes = nullOrEmpty(connectionAttributes);
      return this;
    }

    /**
     * Use server dedicated bulk batch command
     *
     * @param useBulkStmts use server bulk batch command.
     * @return this {@link Builder}
     */
    public Builder useBulkStmts(Boolean useBulkStmts) {
      this.useBulkStmts = useBulkStmts;
      return this;
    }

    /**
     * Use server dedicated bulk batch command for insert (if useBulkStmts is enabled,
     * useBulkStmtsForInserts will be enabled as well)
     *
     * @param useBulkStmtsForInserts use server bulk batch command.
     * @return this {@link Builder}
     */
    public Builder useBulkStmtsForInserts(Boolean useBulkStmtsForInserts) {
      this.useBulkStmtsForInserts = useBulkStmtsForInserts;
      return this;
    }

    /**
     * Disable pipeline
     *
     * @param disablePipeline disable pipeline.
     * @return this {@link Builder}
     */
    public Builder disablePipeline(Boolean disablePipeline) {
      this.disablePipeline = disablePipeline;
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
     * Permit indicating in DatabaseMetadata if null value must be considered current schema/catalog
     *
     * @param nullDatabaseMeansCurrent indicating in DatabaseMetadata if null value must be
     *     considered current schema/catalog
     * @return this {@link Builder}
     */
    public Builder nullDatabaseMeansCurrent(Boolean nullDatabaseMeansCurrent) {
      this.nullDatabaseMeansCurrent = nullDatabaseMeansCurrent;
      return this;
    }

    /**
     * "schema" and "database" are server synonymous. Connector historically get/set database using
     * Connection.setCatalog()/getCatalog(), setSchema()/getSchema() being no-op This parameter
     * indicate to change that behavior to use Schema in place of Catalog. Behavior will change
     *
     * <ul>
     *   <li>database change will be done with either Connection.setCatalog()/getCatalog() or
     *       Connection.setSchema()/getSchema()
     *   <li>DatabaseMetadata methods that use catalog or schema filtering
     *   <li>ResultsetMetadata database will be retrieved
     * </ul>
     *
     * @param useCatalogTerm use CATALOG/SCHEMA
     * @return this {@link Builder}
     */
    public Builder useCatalogTerm(String useCatalogTerm) {
      this.useCatalogTerm = useCatalogTerm;
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
     * indicate if connector can use local state to avoid unnecessary queries. This means
     * application must use JDBC dedicated methods, like connection.setTransactionIsolation and
     * never queries like "SET SESSION TRANSACTION ISOLATION LEVEL X" directly
     *
     * @param useLocalSessionState can driver rely on local state
     * @return this {@link Builder}
     */
    public Builder useLocalSessionState(Boolean useLocalSessionState) {
      this.useLocalSessionState = useLocalSessionState;
      return this;
    }

    /**
     * indicate if connector must return multi-generated ids. (For connector 2.x compatibility)
     *
     * @param returnMultiValuesGeneratedIds must return multi-values generated ids
     * @return this {@link Builder}
     */
    public Builder returnMultiValuesGeneratedIds(Boolean returnMultiValuesGeneratedIds) {
      this.returnMultiValuesGeneratedIds = returnMultiValuesGeneratedIds;
      return this;
    }

    /**
     * indicate if connector must force sql_mode strict mode for jdbc compliance
     *
     * @param jdbcCompliantTruncation must force sql_mode strict mode for jdbc compliance
     * @return this {@link Builder}
     */
    public Builder jdbcCompliantTruncation(Boolean jdbcCompliantTruncation) {
      this.jdbcCompliantTruncation = jdbcCompliantTruncation;
      return this;
    }

    /**
     * Force Timestamp string representation compatible 2.7 version Timestamp string
     * representation will then correspond to Timestamp.toString() in place of taking field
     * precision
     *
     * @param oldModeNoPrecisionTimestamp force 2.7 timestamp to string behavior
     * @return this {@link Builder}
     */
    public Builder oldModeNoPrecisionTimestamp(Boolean oldModeNoPrecisionTimestamp) {
      this.oldModeNoPrecisionTimestamp = oldModeNoPrecisionTimestamp;
      return this;
    }

    /**
     * indicate if connector must redirect connection when receiving server redirect information
     *
     * @param permitRedirect must redirect when required
     * @return this {@link Builder}
     */
    public Builder permitRedirect(Boolean permitRedirect) {
      this.permitRedirect = permitRedirect;
      return this;
    }

    /**
     * Indicate if for XA transaction, connector must reuse same connection.
     *
     * @param pinGlobalTxToPhysicalConnection force reuse of same connection
     * @return this {@link Builder}
     */
    public Builder pinGlobalTxToPhysicalConnection(Boolean pinGlobalTxToPhysicalConnection) {
      this.pinGlobalTxToPhysicalConnection = pinGlobalTxToPhysicalConnection;
      return this;
    }

    /**
     * Indicate if Statement/PreparedStatement.executeQuery for command that produce no result will
     * return an exception or just an empty result-set When enabled, command not returning no data
     * will end returning an empty result-set When disabled, command not returning no data will end
     * throwing an exception
     *
     * @param permitNoResults force reuse of same connection
     * @return this {@link Builder}
     */
    public Builder permitNoResults(Boolean permitNoResults) {
      this.permitNoResults = permitNoResults;
      return this;
    }

    /**
     * On dead-lock exception must add innodb status in exception error message. If enabled, an
     * additional command will be done to retrieve innodb status when dead-lock occurs.
     *
     * @param includeInnodbStatusInDeadlockExceptions Must dead-lock exception must add innodb
     *     status in exception error message
     * @return this {@link Builder}
     */
    public Builder includeInnodbStatusInDeadlockExceptions(
        Boolean includeInnodbStatusInDeadlockExceptions) {
      this.includeInnodbStatusInDeadlockExceptions = includeInnodbStatusInDeadlockExceptions;
      return this;
    }

    /**
     * Dead-lock error will contain threads information
     *
     * @param includeThreadDumpInDeadlockExceptions must dead-lock error contain treads informations
     * @return this {@link Builder}
     */
    public Builder includeThreadDumpInDeadlockExceptions(
        Boolean includeThreadDumpInDeadlockExceptions) {
      this.includeThreadDumpInDeadlockExceptions = includeThreadDumpInDeadlockExceptions;
      return this;
    }

    /**
     * set service principal name (GSSAPI)
     *
     * @param servicePrincipalName service principal name (GSSAPI)
     * @return this {@link Builder}
     */
    public Builder servicePrincipalName(String servicePrincipalName) {
      this.servicePrincipalName = nullOrEmpty(servicePrincipalName);
      return this;
    }

    /**
     * Set default fetch size
     *
     * @param defaultFetchSize default fetch size
     * @return this {@link Builder}
     */
    public Builder defaultFetchSize(Integer defaultFetchSize) {
      this.defaultFetchSize = defaultFetchSize;
      return this;
    }

    /**
     * Permit to defined default tls plugin type
     *
     * @param tlsSocketType default tls socket plugin to use
     * @return this {@link Builder}
     */
    public Builder tlsSocketType(String tlsSocketType) {
      this.tlsSocketType = nullOrEmpty(tlsSocketType);
      return this;
    }

    /**
     * Set the log size limit for query
     *
     * @param maxQuerySizeToLog set query size limit
     * @return this {@link Builder}
     */
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

    /**
     * When failover occurs, how many connection attempt before throwing error when reconnecting
     *
     * @param retriesAllDown number of attemps to reconnect
     * @return this {@link Builder}
     */
    public Builder retriesAllDown(Integer retriesAllDown) {
      this.retriesAllDown = retriesAllDown;
      return this;
    }

    /**
     * Indicate galera allowed state (comma separated), permitting to validate if galera node is
     * synchronized
     *
     * @param galeraAllowedState galera allowed state
     * @return this {@link Builder}
     */
    public Builder galeraAllowedState(String galeraAllowedState) {
      this.galeraAllowedState = nullOrEmpty(galeraAllowedState);
      return this;
    }

    /**
     * Create pool if not existing, or get a connection for the pool associate with this connection
     * string if existing.
     *
     * @param pool use pool
     * @return this {@link Builder}
     */
    public Builder pool(Boolean pool) {
      this.pool = pool;
      return this;
    }

    /**
     * set pool name
     *
     * @param poolName pool name
     * @return this {@link Builder}
     */
    public Builder poolName(String poolName) {
      this.poolName = nullOrEmpty(poolName);
      return this;
    }

    /**
     * Set the limit number of connection in pool.
     *
     * @param maxPoolSize maximum connection size in pool.
     * @return this {@link Builder}
     */
    public Builder maxPoolSize(Integer maxPoolSize) {
      this.maxPoolSize = maxPoolSize;
      return this;
    }

    /**
     * Minimum pool size.
     *
     * @param minPoolSize minimum pool size
     * @return this {@link Builder}
     */
    public Builder minPoolSize(Integer minPoolSize) {
      this.minPoolSize = minPoolSize;
      return this;
    }

    /**
     * Set the maximum idle time of a connection indicating that connection must be released
     *
     * @param maxIdleTime maximum idle time of a connection in pool
     * @return this {@link Builder}
     */
    public Builder maxIdleTime(Integer maxIdleTime) {
      this.maxIdleTime = maxIdleTime;
      return this;
    }

    /**
     * Must pool register JMX information
     *
     * @param registerJmxPool register pool to JMX
     * @return this {@link Builder}
     */
    public Builder registerJmxPool(Boolean registerJmxPool) {
      this.registerJmxPool = registerJmxPool;
      return this;
    }

    /**
     * Pool will validate connection before giving it. This amount of time indicate that recently
     * use connection can skip validation 0 means connection will be validated each time (even is
     * just used)
     *
     * @param poolValidMinDelay time limit indicating that connection in pool must be validated
     * @return this {@link Builder}
     */
    public Builder poolValidMinDelay(Integer poolValidMinDelay) {
      this.poolValidMinDelay = poolValidMinDelay;
      return this;
    }

    /**
     * Indicate that connection returned to pool must be RESETed like having proper connection
     * state.
     *
     * @param useResetConnection use reset connection when returning connection to pool.
     * @return this {@link Builder}
     */
    public Builder useResetConnection(Boolean useResetConnection) {
      this.useResetConnection = useResetConnection;
      return this;
    }

    /**
     * MySQL Authentication RSA server file, for mysql authentication
     *
     * @param serverRsaPublicKeyFile server RSA public key file
     * @return this {@link Builder}
     */
    public Builder serverRsaPublicKeyFile(String serverRsaPublicKeyFile) {
      this.serverRsaPublicKeyFile = nullOrEmpty(serverRsaPublicKeyFile);
      return this;
    }

    /**
     * Allow RSA server file retrieval from MySQL server
     *
     * @param allowPublicKeyRetrieval Allow RSA server file retrieval from MySQL server
     * @return this {@link Builder}
     */
    public Builder allowPublicKeyRetrieval(Boolean allowPublicKeyRetrieval) {
      this.allowPublicKeyRetrieval = allowPublicKeyRetrieval;
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
}
