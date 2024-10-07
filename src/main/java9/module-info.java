import org.mariadb.jdbc.plugin.codec.*;

module org.mariadb.jdbc {
  requires transitive java.sql;
  requires transitive java.management;
  requires transitive java.naming;
  requires transitive java.security.jgss;
  requires transitive jdk.net;
  requires static waffle.jna;
  requires static software.amazon.awssdk.services.rds;
  requires static software.amazon.awssdk.regions;
  requires static software.amazon.awssdk.auth;
  requires static com.sun.jna;
  requires static com.sun.jna.platform;
  requires static org.slf4j;

  exports org.mariadb.jdbc;
  exports org.mariadb.jdbc.client;
  exports org.mariadb.jdbc.client.util;
  exports org.mariadb.jdbc.client.socket;
  exports org.mariadb.jdbc.message;
  exports org.mariadb.jdbc.type;
  exports org.mariadb.jdbc.export;
  exports org.mariadb.jdbc.util.constants;
  exports org.mariadb.jdbc.util.timeout;
  exports org.mariadb.jdbc.plugin;
  exports org.mariadb.jdbc.plugin.codec;
  exports org.mariadb.jdbc.plugin.authentication.standard;
  exports org.mariadb.jdbc.plugin.authentication.addon;
  exports org.mariadb.jdbc.plugin.credential.aws;
  exports org.mariadb.jdbc.plugin.credential.env;
  exports org.mariadb.jdbc.plugin.credential.system;
  exports org.mariadb.jdbc.plugin.tls.main;

  uses java.sql.Driver;
  uses org.mariadb.jdbc.plugin.CredentialPlugin;
  uses org.mariadb.jdbc.plugin.Codec;
  uses org.mariadb.jdbc.plugin.AuthenticationPlugin;
  uses org.mariadb.jdbc.plugin.TlsSocketPlugin;

  provides java.sql.Driver with
      org.mariadb.jdbc.Driver;
  provides org.mariadb.jdbc.plugin.AuthenticationPlugin with
      org.mariadb.jdbc.plugin.authentication.addon.ClearPasswordPlugin,
      org.mariadb.jdbc.plugin.authentication.addon.SendGssApiAuthPacket,
      org.mariadb.jdbc.plugin.authentication.standard.Ed25519PasswordPlugin,
      org.mariadb.jdbc.plugin.authentication.standard.NativePasswordPlugin,
      org.mariadb.jdbc.plugin.authentication.standard.SendPamAuthPacket,
      org.mariadb.jdbc.plugin.authentication.standard.CachingSha2PasswordPlugin;
  provides org.mariadb.jdbc.plugin.Codec with
      BigDecimalCodec,
      BigIntegerCodec,
      BitSetCodec,
      BlobCodec,
      BooleanCodec,
      ByteArrayCodec,
      ByteCodec,
      ClobCodec,
      DateCodec,
      DoubleCodec,
      DurationCodec,
      FloatCodec,
      GeometryCollectionCodec,
      IntCodec,
      LineStringCodec,
      LocalDateCodec,
      LocalDateTimeCodec,
      LocalTimeCodec,
      LongCodec,
      MultiLinestringCodec,
      MultiPointCodec,
      MultiPolygonCodec,
      PointCodec,
      PolygonCodec,
      ReaderCodec,
      ShortCodec,
      StreamCodec,
      StringCodec,
      TimeCodec,
      TimestampCodec,
      ZonedDateTimeCodec;
  provides org.mariadb.jdbc.plugin.CredentialPlugin with
      org.mariadb.jdbc.plugin.credential.aws.AwsIamCredentialPlugin,
      org.mariadb.jdbc.plugin.credential.env.EnvCredentialPlugin,
      org.mariadb.jdbc.plugin.credential.system.PropertiesCredentialPlugin;
  provides org.mariadb.jdbc.plugin.TlsSocketPlugin with
      org.mariadb.jdbc.plugin.tls.main.DefaultTlsSocketPlugin;
}
