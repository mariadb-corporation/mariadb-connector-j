import com.singlestore.jdbc.plugin.codec.*;

module com.singlestore.jdbc {
  requires transitive java.sql;
  requires transitive java.management;
  requires transitive java.naming;
  requires transitive java.security.jgss;
  requires transitive jdk.net;
  requires waffle.jna;
  requires static software.amazon.awssdk.services.rds;
  requires static software.amazon.awssdk.regions;
  requires static software.amazon.awssdk.auth;
  requires static com.sun.jna;
  requires static com.sun.jna.platform;
  requires static org.slf4j;

  exports com.singlestore.jdbc;
  exports com.singlestore.jdbc.client;
  exports com.singlestore.jdbc.client.socket;
  exports com.singlestore.jdbc.message;
  exports com.singlestore.jdbc.type;
  exports com.singlestore.jdbc.export;
  exports com.singlestore.jdbc.plugin;
  exports com.singlestore.jdbc.plugin.codec;
  exports com.singlestore.jdbc.plugin.authentication.standard;
  exports com.singlestore.jdbc.plugin.authentication.addon;
  exports com.singlestore.jdbc.plugin.credential.aws;
  exports com.singlestore.jdbc.plugin.credential.env;
  exports com.singlestore.jdbc.plugin.credential.system;

  uses java.sql.Driver;
  uses com.singlestore.jdbc.plugin.CredentialPlugin;
  uses com.singlestore.jdbc.plugin.Codec;
  uses com.singlestore.jdbc.plugin.AuthenticationPlugin;
  uses com.singlestore.jdbc.plugin.TlsSocketPlugin;

  provides java.sql.Driver with
      com.singlestore.jdbc.Driver;
  provides com.singlestore.jdbc.plugin.AuthenticationPlugin with
      com.singlestore.jdbc.plugin.authentication.addon.ClearPasswordPlugin,
      com.singlestore.jdbc.plugin.authentication.addon.SendGssApiAuthPacket,
      com.singlestore.jdbc.plugin.authentication.standard.Ed25519PasswordPlugin,
      com.singlestore.jdbc.plugin.authentication.standard.NativePasswordPlugin,
      com.singlestore.jdbc.plugin.authentication.standard.SendPamAuthPacket;
  provides com.singlestore.jdbc.plugin.Codec with
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
      InstantCodec,
      IntCodec,
      LineStringCodec,
      LocalDateCodec,
      LocalDateTimeCodec,
      LocalTimeCodec,
      LongCodec,
      OffsetDateTimeCodec,
      PointCodec,
      PolygonCodec,
      ReaderCodec,
      ShortCodec,
      StreamCodec,
      StringCodec,
      TimeCodec,
      TimestampCodec,
      UuidCodec,
      ZonedDateTimeCodec;
  provides com.singlestore.jdbc.plugin.CredentialPlugin with
      com.singlestore.jdbc.plugin.credential.aws.AwsIamCredentialPlugin,
      com.singlestore.jdbc.plugin.credential.browser.BrowserCredentialPlugin,
      com.singlestore.jdbc.plugin.credential.env.EnvCredentialPlugin,
      com.singlestore.jdbc.plugin.credential.token.JwtCredentialPlugin,
      com.singlestore.jdbc.plugin.credential.system.PropertiesCredentialPlugin;
  provides com.singlestore.jdbc.plugin.TlsSocketPlugin with
      com.singlestore.jdbc.client.tls.DefaultTlsSocketPlugin;
}
