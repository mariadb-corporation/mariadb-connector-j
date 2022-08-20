import org.tidb.jdbc.plugin.codec.*;

module org.tidb.jdbc {
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

  exports org.tidb.jdbc;
  exports org.tidb.jdbc.client;
  exports org.tidb.jdbc.client.util;
  exports org.tidb.jdbc.client.socket;
  exports org.tidb.jdbc.message;
  exports org.tidb.jdbc.export;
  exports org.tidb.jdbc.plugin;
  exports org.tidb.jdbc.plugin.codec;
  exports org.tidb.jdbc.plugin.authentication.standard;
  exports org.tidb.jdbc.plugin.authentication.addon;
  exports org.tidb.jdbc.plugin.credential.aws;
  exports org.tidb.jdbc.plugin.credential.env;
  exports org.tidb.jdbc.plugin.credential.system;
  exports org.tidb.jdbc.plugin.tls.main;

  uses java.sql.Driver;
  uses org.tidb.jdbc.plugin.CredentialPlugin;
  uses org.tidb.jdbc.plugin.Codec;
  uses org.tidb.jdbc.plugin.AuthenticationPlugin;
  uses org.tidb.jdbc.plugin.TlsSocketPlugin;

  provides java.sql.Driver with
      org.tidb.jdbc.Driver;
  provides org.tidb.jdbc.plugin.AuthenticationPlugin with
      org.tidb.jdbc.plugin.authentication.addon.ClearPasswordPlugin,
      org.tidb.jdbc.plugin.authentication.addon.SendGssApiAuthPacket,
      org.tidb.jdbc.plugin.authentication.standard.Ed25519PasswordPlugin,
      org.tidb.jdbc.plugin.authentication.standard.NativePasswordPlugin,
      org.tidb.jdbc.plugin.authentication.standard.SendPamAuthPacket,
      org.tidb.jdbc.plugin.authentication.standard.CachingSha2PasswordPlugin;
  provides org.tidb.jdbc.plugin.Codec with
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
      IntCodec,
      LocalDateCodec,
      LocalDateTimeCodec,
      LocalTimeCodec,
      LongCodec,
      ReaderCodec,
      ShortCodec,
      StreamCodec,
      StringCodec,
      TimeCodec,
      TimestampCodec,
      ZonedDateTimeCodec;
  provides org.tidb.jdbc.plugin.CredentialPlugin with
      org.tidb.jdbc.plugin.credential.aws.AwsIamCredentialPlugin,
      org.tidb.jdbc.plugin.credential.env.EnvCredentialPlugin,
      org.tidb.jdbc.plugin.credential.system.PropertiesCredentialPlugin;
  provides org.tidb.jdbc.plugin.TlsSocketPlugin with
      org.tidb.jdbc.plugin.tls.main.DefaultTlsSocketPlugin;
}
