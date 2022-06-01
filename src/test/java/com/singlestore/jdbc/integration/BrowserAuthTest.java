// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2021-2022 SingleStore, Inc.

package com.singlestore.jdbc.integration;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.Common;
import com.singlestore.jdbc.Statement;
import com.singlestore.jdbc.plugin.credential.CredentialPluginLoader;
import com.singlestore.jdbc.plugin.credential.browser.BrowserCredentialPlugin;
import com.singlestore.jdbc.plugin.credential.browser.TokenWaiterServer;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.*;

public class BrowserAuthTest extends Common {
  /*
  The tests are performed using MockBrowserCredentialPlugin instead of the regular one.
  There are two differences between them:
  1) a local address is used instead of the portal one
  2) we just send a request with a pre-defined user-password instead of opening the browser

  Keys used to sign the JWTs used in these tests. The public key is duplicated in
  scripts/jwt/jwt_auth_config.json which is used to tell SingleStore to accept the tokens

  -----BEGIN RSA PRIVATE KEY-----
  MIIJJwIBAAKCAgEAxSS8EPCSBiti+RFAK9A+y9zZfiaf97QZUwDKxoWOQoPP4lK0
  o41pPXfTyOIFst1AW8aNZ0ulqNu6s/LbJ2X8FLli4giSOOmRkXq31lXe1fheAMJ4
  3S2rwHBJJqihMXXBTXbVOFu2nJP68myPw7ZMTd4Y2EsjSrgeAhdS17QKj5vZQ6xv
  CrF8Fa523Zyz1+TXgvYEIuhC1uorT0+P5VuT8SRC4DC0n6yU3x+8Xkx6R9BWzG8w
  gZIL3WTaFjtZN5TJ6JS+5sJWzWRPyJUYaGnAH5362BxIox53WnN0DnKNrZ3wAxxm
  Rd2+3D33UEHGNDl5WQeW2Ov2AL+Zpqqeasz7Emhf8rSZBw8w2X1DMigrSbdadL6f
  anC0brltZx6XlqGFBvBTc6wt1ZZaDDz70lSZqsh1CkdWf9hxlq5d19Q9Onguo5y3
  uJHok1ixGXWgOQq4aSKRf775vN/iQtsUWb8POtPp+LXsJ6GCE7TZUIrpQmsAF8j5
  lxLTDFFyjJUirSaHRJqwUOGoJnkgCqGbm9rFaG67JWaTaFmWaQeW17Lc5xqCscuS
  vrw5K8XTVgiHhFFNnay8LO/iiBjYtpz8kKbXfXDt3LARfBbnz4xrtg6OEF+jz9bF
  SdUeHSf7ql2SArW20W+1pTGqaLw8hAb7mbZ5tXuINPufsprQ8/I9PjNXMV0CAwEA
  AQKCAgBhkMaKU6TQ7NP0k7cAd/U8CzaQGil8+2K1E2VHTn2TKYzOY0QG1UtKIm1r
  s4BCfwEE6oS8pFF9+hCyUfRn0S8qSn1HhBpplB54sxUcPC8mEd7j3VrXi2y+tlNd
  kIMF6VMbNT5cv/bmEs5U/6k+oI+u0cXV9YmnxusC+ewD2JSJcgXaWhIyZpgUWt10
  28KdjCGkLIDrjarWldmNTMDYL7RN2TZHoZMimtSqgBhHSu4RcGgkkLqexVqd3PWZ
  nxGOUlKCimrX5UH0MDrT+AW2Vu/ANf0Yyxafs8o5t32uUL8RN2K7B2kOFqoIcZpd
  289ttSv7Bah//ncm86vlMfdov71ZBP+CVOrwPy6gtkPnv76m0CiUx3638z0sYI1K
  6AHz7WkcjfOSJTYHVnGVgkiHdIZ6iEFyb8pD3jFbhlIeRPGZWCfuxOshwG6pV6Xd
  K3M1xq8mJwFZMt0l6h+5WpIRY1dnppXbjZyxBjCyTjk0IjZOo/8bklXFx1UQp8CH
  5K1Xx+5B0UCkFCNn6RbDlWXubBHlQZcVGUzVf+O5zC2WOZBqAjPyIO0nVmtYrDIS
  f2kPAcYlVXdPGDgTBdEfe3jW1GmFQWeE/4CmNZTzjJGCGMGJK5OgNKFSg20FxrxX
  UQSTPJpeXwiL3gF73LgKB3ltSLHi8pFJb6syoLGmq1tLHX46+QKCAQEA+/CJqteX
  FLKKWgL85Kd5DySVtNsOC9rxlae7gDyaBV0N6CioJt8BGyl/uSOxX+J85eBQUhtg
  mlnc3HEmn6xQXt+JgoTaoG2ng2JxIFhAOTFNn0hhxo4ApDrBYrGxDJz5g2kK+IBG
  sG6KFReXqDBCtpVBBIkW/+26IJdmFALDoFG4J/p5SGLlR7nscUpDeSXhxi8HboEA
  JymiEYT+7yjhDUosSbGK5qgbB/B3xzQ0YgzXp48QdkcUOEsYoCFHguoB4VgwS03p
  u87j/pOhQbCTahyDkwT2kIyHdYVMXnfECg3PxG3SB6n1UsqsdeAswu2vx1+lUknT
  kyumA+bycAiFRwKCAQEAyFId8PIoyHoMOuCf0kN/t2KuFeD4J17R0UcSW9e2MbAU
  QRMJVQiD5/6eUpN+YmWCqlYA5+LltxDwrbbrtx3VO1lfDGcQzxRUlgcgCwVwwraZ
  xGwUzA9JcZTyFaYs9L2oFK3OkSPXszKhxOutppMrSYhw36zhZqYtc1pK+eCHNyMk
  7ZGCIK+irsv6ozooO/yvFxPax/0S7n0OD8ow+5iXUw7YF6FkzjR8/UK/DO9+o3Sv
  QpABZ+r02U5qeT85UCAcVT/nybOpas+Z+fAF8ZIAIZSnjXGPdB6XOvJFM6+sEsbd
  onLx4QtgLt0uKAZQWKmHabXURnab3JhpkG1S6GC2OwKCAQBt/7m70+Fs8f8iCcfs
  9YoPqIOMsU/SsUdldhSRiuQcj2JxCL9SKW/MMjRH22OoX7T0kRnAn59wBOg/f0/D
  y3JT2fmp+OOTxAytep+15ZI05mfjsbCvBnUVP2oL81VAEpGGZKibkzZJ9hln2CMp
  Fdkq6sO2fTyDhYIMlM3G0uYi60sieWPWzQcaZ/zqAeivznBjHUl7X+t3LeBLEexU
  814/dTEdA92Hk8Iplz5UxWBRpxXJXNdtLN+RLIiV8bHNYOptPxnm5x+0FkLJdh+k
  FLpoTAbOfA5DUngaQZb0cAox8ZHTS7e2DOjFuyPNW5FvkmN7AzGlWgJ8cURM09rq
  O24lAoIBABr6RCIA2tE07pS3T47HnFmcJom3xHO451Th120a/eRvLCsfXzBedzU1
  Kyk/x9OEjDZYYsLX4cvnsiIS8me00tStUomfD7pzqHiT+RLC5s6yPL8hNyPMIz3y
  qy+TM5a6O/qc9abCRvhRJ0wX2UkHpNrAT0MwSyLB2nkgfdxtCoi4aO69m+K/BI+5
  1MVKvcRmYUYgXGR2hqgrm0sxFausfySmaR+1kpfapcKNzKD3V/y3aCr0rdvK3rKt
  RtWRWCycRnSMqLCXS4eg8cGhO4uu9+mN1YrM8l7XB9LeccdmLyxQL+UCyeRe3dMx
  4ldtkkB+hEgOPspGivMIa58Rugqli6UCggEARGjelbYSFs43LU34crgtMMlS3lYh
  jSAHzmVOYRucV0Hf4jfPqF0c4Gkw4ZrGtU82drrDzWkSdlpCKcMtP5bx2aBFcnO8
  bVk/bbWVI7n2aHXfCeVE27+C6sAoKWyvT06uQ8tCOFbKpaSL5swCd0DMy2JezaG9
  cg5gLz0TwqXzrCfDGRHfH+7/Ujsg2svqOuGP0341GfvPr7b/UpSpQ2WVRmumBMUq
  bmS4WHANv5I7aP+bE+/sazgT+zvxgDIetjnAz61YpSVRNWu8s2//9T4ZstPGrOz2
  N6rVJjbyoZmyMZ5Uv2e1azh3xpZuqRzizWuQ8HpZLCzld2J5wvpdNMKVYw==
  -----END RSA PRIVATE KEY-----

  -----BEGIN PUBLIC KEY-----
  MIICIjANBgkqhkiG9w0BAQEFAAOCAg8AMIICCgKCAgEAxSS8EPCSBiti+RFAK9A+
  y9zZfiaf97QZUwDKxoWOQoPP4lK0o41pPXfTyOIFst1AW8aNZ0ulqNu6s/LbJ2X8
  FLli4giSOOmRkXq31lXe1fheAMJ43S2rwHBJJqihMXXBTXbVOFu2nJP68myPw7ZM
  Td4Y2EsjSrgeAhdS17QKj5vZQ6xvCrF8Fa523Zyz1+TXgvYEIuhC1uorT0+P5VuT
  8SRC4DC0n6yU3x+8Xkx6R9BWzG8wgZIL3WTaFjtZN5TJ6JS+5sJWzWRPyJUYaGnA
  H5362BxIox53WnN0DnKNrZ3wAxxmRd2+3D33UEHGNDl5WQeW2Ov2AL+Zpqqeasz7
  Emhf8rSZBw8w2X1DMigrSbdadL6fanC0brltZx6XlqGFBvBTc6wt1ZZaDDz70lSZ
  qsh1CkdWf9hxlq5d19Q9Onguo5y3uJHok1ixGXWgOQq4aSKRf775vN/iQtsUWb8P
  OtPp+LXsJ6GCE7TZUIrpQmsAF8j5lxLTDFFyjJUirSaHRJqwUOGoJnkgCqGbm9rF
  aG67JWaTaFmWaQeW17Lc5xqCscuSvrw5K8XTVgiHhFFNnay8LO/iiBjYtpz8kKbX
  fXDt3LARfBbnz4xrtg6OEF+jz9bFSdUeHSf7ql2SArW20W+1pTGqaLw8hAb7mbZ5
  tXuINPufsprQ8/I9PjNXMV0CAwEAAQ==
  -----END PUBLIC KEY-----
   */
  @BeforeAll
  public static void beforeAll2() throws SQLException {
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE USER jwt_user IDENTIFIED WITH authentication_jwt");
    stmt.execute("GRANT ALL PRIVILEGES ON test.* TO jwt_user");
  }

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP USER IF EXISTS jwt_user");
  }

  @Test
  @Disabled
  public void browserAuth() throws SQLException {
    BrowserCredentialPlugin credPlugin =
        (BrowserCredentialPlugin) CredentialPluginLoader.get("BROWSER_SSO");
    credPlugin.clearKeyring();
    credPlugin.clearLocalCache();
    String connString =
        "jdbc:singlestore://"
            + "svc-4c25892d-c8d3-4bf3-ab15-1aceeb54a9ff-ddl.aws-oregon-2.svc.singlestore.com:3306/test"
            + "?credentialType=BROWSER_SSO"
            + "&sslMode=trust";

    try (java.sql.Connection connection = DriverManager.getConnection(connString)) {
      ResultSet rs = connection.createStatement().executeQuery("select 1");
      assertTrue(rs.next());
    }
  }

  @Test
  public void mockBrowser() throws IOException, SQLException {
    /*{
       "email": "test-email@gmail.com",
       "sub": "wrong_user",
       "username": "jwt_user",
       "exp": 1916239022 (year 2030)
    }*/
    String jwt =
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6InRlc3QtZW1haWxAZ21haWwuY29tIiwic3ViIjoid3Jvbmd"
            + "fdXNlciIsInVzZXJuYW1lIjoiand0X3VzZXIiLCJleHAiOjE5MTYyMzkwMjJ9.rSUfkgB8MhxazNAxZU8Wa2BIVqcxs3vBnT"
            + "EDqNLT9yhP4gbMBz0EzAIiAFQe8A1yeeNhvwfHP2GDLYhi3c88HtdI2P6T00a90x7RCrmD7mWWgdA7OTrdxKNX3CsuVmthaG"
            + "ExDAJDe3i_dPfZxFNHmYAX_4KBugZTwQOvsKir7sKPBi9atnTPm9dGqapYWWIcDyMGNk5GD50Pzxgncc2VMfx2AcVmzANIK2"
            + "E7SOCRsN96YL0BkTb34CW2NeH001bnoIEjEJeQI3lEbCVafjTbBXHWptbwL2j9aoiV0XzjkT00-GdtUt1i6DfQO-EWF0J_IC"
            + "_79wEiGfOnM5waWi-LDQ0FnXjV1FIpnOiJab9meIB11sW5MFn2U8q0yMareRHJQ43ZWg5uAnAf2ugm71EsTQtbmKGgDsTzt2"
            + "UglhiNpnONzOEDCzz61FiVUTgWu0wMYzUgitgMJYvaDUit3F2OfQw4x--60VWKhB-q4EGm0DvPgFHMspxcZKNFlqJH3Qfgk8"
            + "LDtJBI0kPpSJoYKbS9n1SmmfVL1UZOTsZNutIYNuN2CWo_D_TJNFKMys6sI7OIQ5QtYyHZyW1wShrR2V2Kwj6IxXpA2XxQf2"
            + "emCRhCNGl5js73ljVnI0HsPLcEzEreRUQWOxgHCuB4dk2QgBj7EiZl57Cm0GywEqDlwf-XX1g";
    MockHttpServer ssoServer = new MockHttpServer(jwt, false, 1, 0);
    try {
      // make sure no creds are cached
      BrowserCredentialPlugin credPlugin =
          (BrowserCredentialPlugin) CredentialPluginLoader.get("MOCK_BROWSER_SSO");
      credPlugin.clearKeyring();
      credPlugin.clearLocalCache();

      String connString =
          String.format("jdbc:singlestore://%s:%s/", hostname, port)
              + sharedConn.getCatalog()
              + "?credentialType=MOCK_BROWSER_SSO"
              + "&sslMode=trust";

      try (java.sql.Connection connection = DriverManager.getConnection(connString)) {
        ResultSet rs = connection.createStatement().executeQuery("select 1");
        assertTrue(rs.next());
      }

      // should not query for token again (verified by the MockHttpServer)
      try (java.sql.Connection connection = DriverManager.getConnection(connString)) {
        ResultSet rs = connection.createStatement().executeQuery("select 1");
        assertTrue(rs.next());
      }
    } finally {
      ssoServer.stop();
    }
  }

  @Test
  public void mockBrowserKeystore() throws IOException, SQLException {
    // keystore cannot be used on CircleCI
    Assumptions.assumeFalse("CIRCLE_CI".equals(System.getenv("TEST_ENVIRON")));
    /*{
       "email": "test-email@gmail.com",
       "username": "jwt_user",
       "exp": 1916239022 (year 2030)
    }*/
    String jwt =
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6InRlc3QtZW1haWxAZ21haWwuY29tIiwidXNlcm5hbWUiOiJ"
            + "qd3RfdXNlciIsImV4cCI6MTkxNjIzOTAyMn0.Olpk3wlI07zqo98Ya7KOdE6Bmux_Kdp8ZMB1qylQO-SPFwnV3FqQyY40KQJ"
            + "6c6D1FtedRmfWPo9b6FUIAawEPfFGxY95OjfnhdACEKz-gZcNhWCSRlBIIgwqwj22mebLRbu64_Y9t7j8a_ld0vs7q6rtxfJ"
            + "WatEQUwXxhWs3eYHfTeLHs-f-y9iixqohqjCDzWgQ-LYeflCVlNMWYJFo9lzy8cEushuHDiRhiaSESC_QTGWgV74tI1ORqzr"
            + "oxViM8n9uuKzU-Ez6oLQMocFdFkO7ohXr-SSdFQ5otHjLZDfsJgBxp5llh16o5FnFl1HGUF3u3XXMIdM-q-aNIed4mKlAkti"
            + "MTtuKszgndgK5Lt40dfg5gyw4zRApWeLgJQR0zWOk292KHuGPrzAZvedsWoP0hcVksiST87n3N4u1BfYAjh9t6CrbHKeMkkv"
            + "NoCZt8czApXp5G2Hd0jxae01cVSxEQ8s7jWJMBbCeC5RuAYaazANFw3AEl_rd_KP9MsBJu1MFr7O1VIWOfG7PDrTVOa_oY9N"
            + "j7ozv3V4dRk5s4UEE_ZhCxLNPt3XS6kotHWiXCS-Jkos3vEMAm6It77KrAPXDSxPbeDEVD5fvnAPRNUrkXTfafdlzmYY_1o-"
            + "9QQmi3UjhWZ_bBEMFVV3UFjYsPBLn8WVFzWOSSGV-JnGAE3A";
    MockHttpServer ssoServer = new MockHttpServer(jwt, false, 1, 0);
    try {
      // make sure no creds are cached
      BrowserCredentialPlugin credPlugin =
          (BrowserCredentialPlugin) CredentialPluginLoader.get("MOCK_BROWSER_SSO");
      credPlugin.clearKeyring();
      credPlugin.clearLocalCache();

      String connString =
          String.format("jdbc:singlestore://%s:%s/", hostname, port)
              + sharedConn.getCatalog()
              + "?credentialType=MOCK_BROWSER_SSO"
              + "&sslMode=trust";

      try (java.sql.Connection connection = DriverManager.getConnection(connString)) {
        ResultSet rs = connection.createStatement().executeQuery("select 1");
        assertTrue(rs.next());
      }

      // remove creds from in-memory storage, they should be stored in the keyring
      credPlugin.clearLocalCache();

      // should not query for token again (verified by the MockHttpServer)
      try (java.sql.Connection connection = DriverManager.getConnection(connString)) {
        ResultSet rs = connection.createStatement().executeQuery("select 1");
        assertTrue(rs.next());
      }

      // this time remove creds from the keyring, they should be stored in-memory
      credPlugin.clearKeyring();

      // should not query for token again (verified by the MockHttpServer)
      try (java.sql.Connection connection = DriverManager.getConnection(connString)) {
        ResultSet rs = connection.createStatement().executeQuery("select 1");
        assertTrue(rs.next());
      }
    } finally {
      ssoServer.stop();
    }
  }

  @Test
  public void mockBrowserError() throws IOException, SQLException {
    /*{
       "email": "test-email@gmail.com",
       "username": "wrong_user",
       "exp": 1916239022
    }*/
    String jwt =
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6InRlc3QtZW1haWxAZ21haWwuY29tIiwidXNlcm5hbWUiOiJ3cm9"
            + "uZ191c2VyIiwiZXhwIjoxOTE2MjM5MDIyfQ.Dwg0CLnhzSV718S7zxStCWiEpqDNrDmUhTOOS-ZnsisQk0S66s9Fxghl9Te0dmBj"
            + "a_IHhXUmJTPrirATSdAA4Ez4rLc2j5hvf57jsiPR9ImtobG0cwkjUiRV2CPtyqgWAfSbOnu8_bbitkqL4SEB5-fruaaASMTGPNdT"
            + "vouNMC0t-PhUbLGmDdf779UjBzMR0mI_n7_NO57-p5M5crHYuvS_CnBzokRf4OjUco0w1pd0ovri0Uz-qo9gdHc_s1-3YGkNzGwG"
            + "VoPjLx4EdETvq6F_3nj06Dt2sP51BkpccuL3uvRZMLu-8bqsZ2I08ZHgpLmoO8OWfvbIclT9xMpWEkvFrFCAu3CZXIZ-PGlUzQsv"
            + "5cN0siGDRmlILj1rtIDIYiNudNnEge0-7oxSPi4zx_cw-X8AaJZyUkBe-9sd4dmbrpjCSoEL32ACKUuXQ3KTdI2XvoxfFA-9Lh7B"
            + "Hjgjp3EEYsZ-gr9BngqkX-K8viXQOXuIAJxxAAgteVpgvTu8iMcqb1Tiv-YK7z4BDvlPUwPkQHNCeZS--MaPsWibkOhs9uxrpbIb"
            + "F3wEjbfJQ1y35gl-q39LNfwPf38ytn_ZhIUPMLUGyoi0tM6l7X23sGb3fDdY6wvD4PabeboMVAxsISjp_bqkBkdBf9ZQ2Z0ZOtq2"
            + "ZBXcKvCzOBCge-Z9woQ";
    MockHttpServer ssoServer = new MockHttpServer(jwt, false, 2, 0);

    try {
      // make sure no creds are cached
      BrowserCredentialPlugin credPlugin =
          (BrowserCredentialPlugin) CredentialPluginLoader.get("MOCK_BROWSER_SSO");
      credPlugin.clearKeyring();
      credPlugin.clearLocalCache();

      String connString =
          String.format("jdbc:singlestore://%s:%s/", hostname, port)
              + sharedConn.getCatalog()
              + "?credentialType=MOCK_BROWSER_SSO"
              + "&sslMode=trust";

      assertThrowsContains(
          SQLException.class,
          () -> DriverManager.getConnection(connString),
          "Access denied for user 'wrong_user'@");
    } finally {
      ssoServer.stop();
    }
  }

  @Test
  public void mockBrowserBadJWT() throws IOException, SQLException {
    /*{
       "email": "test-email@gmail.com",
       "exp": 1916239022
    }*/
    String noLoginJwt =
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6InRlc3QtZW1haWxAZ21haWwuY29tIiwiZXhwIjoxOTE2MjM"
            + "5MDIyfQ.hwY3NahUOD7Lkyl5-sVAUw5LCht1XVJSsqkq-tSiXigNe_A2sVtbsciFu5MRD32RW_6ZfsNYOm-lzeiRi6k3seSm"
            + "GmWwi3jhKBdtTOgkUPuzqvSZPlWhXW2cOhk44KKQgEepen4zcJqCTLmt6JvDizgUPkjjT9Ns2_j33b8ymsGcwiuANbG1iCJI"
            + "VoxKIePmfsfhAN1YkUOXAlQaFBkGb4PGm3MBf77Hg_Ph7EbIXw6B3dtW_Fzm9S8hzM-9Yyp4eFB-ysq_dOra5bFvSJWxIG4z"
            + "ZDY9Ulmyes_fT97VYz3jysclU9stIz1vR4JDhaZwkOEsQzruQfRTzRqwWaIClZLd2ZZuuf-_ZZbfSZLr8O0dH1BcnhRGsNo5"
            + "m8ZRu-PD7p78G5mYKw3pAtcCWn5-4ZAZPN7vEEvjeqwXhwXpdt3uOQuY4ysd8tCX3aAiKEXlrBc_A1iFtBRjGV38i8lYwnFy"
            + "jsbVNvt_khNjVrAaZ5oTyeTH0x5Zhvlh4aQw1uOcIQG6k3yR0gHVCHM620_VeLEBRliSxlyLZwU9AQOh0uQpYvGAZbshXLSs"
            + "CBh8h6P5tlgexJrb9dDyV1gvaZInPp1ASTJ-GwZ_xTXS6uqYrlHym-W30_7_ws8tFwLpMavsRYUQIksilO7ULK0vbGV9eD4V"
            + "DETvEny0BhwBsI1YoB0";
    /*{
       "email": "test-email@gmail.com",
       "exp": 1000000000
    }*/
    String expiredJwt =
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6InRlc3QtZW1haWxAZ21haWwuY29tIiwiZXhwIjoxMDAwMDA"
            + "wMDAwfQ.Wgd3K7iRbeLK1sfnX3z0mSbhD0vJXxfLW7skaSpEj7V3kNgwNh1zcQvTJUsWrxtcTmJNsaIcNDpU_PaTIag2o3HZ"
            + "o_b80Y0jfldtSjnET3g6x5dWtjYGg1xItEldLsh-MhObupWJSH7QGFuJYgofsLZ-8b9DYiJRSDiIhCKkmp88VYEM2spGlyjs"
            + "kjJxEqUaHyi483x5yIfLUeeXhCSIsySUtSzUkaPz8DS2mtG8_NzytVBlehu9JiegBJ9mXXDgcLYrhgp2mhTRLGD_0jtZNZPS"
            + "dQMjoikktRUzsTXgirogSCPUvbk6rT1Q6Z7SlowtWwaKPQxv4GPp-1CMfuwWioR8lviMpvO9yBVwXmE_iSzqPq7Y18Pm3JSb"
            + "VQr2A8v3RoeSoYOHQaIzqq5oUvvH4pVimh5L0goI7jEUnln7qc51e1OkPPLySuzynommke86KPTbVaoPzou3vuGNnzeGfqyt"
            + "Qm4Su54uQrgn6-J8LgS3l-Fxe91B2hUc1kT3cj6kNf1C6XaY0EyzFGkXoOmdI9uiCRzKOqyTtK2EVpMGlVe3B8yFd7lmBf49"
            + "8o-QbGENxYTHz4psy6ZlnBIzjtpN3-D9NwcC7eEipDB8B5VxEW2aLJndnvBIb70ll6j3FeNv19-5S4Ek3xuouRUJNXSnt7ne"
            + "BUFDtzchHZjkpjmutuo";

    // make sure no creds are cached
    BrowserCredentialPlugin credPlugin =
        (BrowserCredentialPlugin) CredentialPluginLoader.get("MOCK_BROWSER_SSO");
    credPlugin.clearKeyring();
    credPlugin.clearLocalCache();

    String connString =
        String.format("jdbc:singlestore://%s:%s/", hostname, port)
            + sharedConn.getCatalog()
            + "?credentialType=MOCK_BROWSER_SSO"
            + "&sslMode=trust";

    MockHttpServer ssoServer = new MockHttpServer(noLoginJwt, false, 1, 0);
    try {
      DriverManager.getConnection(connString);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Could not acquire JWT"));
      assertTrue(e.getCause().getMessage().contains("Could not verify claims"));
      assertTrue(
          e.getCause()
              .getCause()
              .getMessage()
              .contains("One of claims 'sub' and 'username' must be present in the JWT"));
    } finally {
      ssoServer.stop();
    }

    ssoServer = new MockHttpServer(expiredJwt, false, 1, 0);
    try {
      DriverManager.getConnection(connString);
      fail();
    } catch (SQLException e) {
      assertTrue(e.getMessage().contains("Could not acquire JWT"));
      assertTrue(e.getCause().getMessage().contains("Could not verify claims"));
      assertTrue(e.getCause().getCause().getMessage().contains("The Token has expired"));
    } finally {
      ssoServer.stop();
    }
  }

  @Test
  public void mockBrowserParallelized() throws SQLException, IOException, InterruptedException {
    /*{
       "email": "test-email@gmail.com",
       "sub": "wrong_user",
       "username": "jwt_user",
       "exp": 1916239022 (year 2030)
    }*/
    String jwt =
        "eyJhbGciOiJSUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6InRlc3QtZW1haWxAZ21haWwuY29tIiwic3ViIjoid3Jvbmd"
            + "fdXNlciIsInVzZXJuYW1lIjoiand0X3VzZXIiLCJleHAiOjE5MTYyMzkwMjJ9.rSUfkgB8MhxazNAxZU8Wa2BIVqcxs3vBnT"
            + "EDqNLT9yhP4gbMBz0EzAIiAFQe8A1yeeNhvwfHP2GDLYhi3c88HtdI2P6T00a90x7RCrmD7mWWgdA7OTrdxKNX3CsuVmthaG"
            + "ExDAJDe3i_dPfZxFNHmYAX_4KBugZTwQOvsKir7sKPBi9atnTPm9dGqapYWWIcDyMGNk5GD50Pzxgncc2VMfx2AcVmzANIK2"
            + "E7SOCRsN96YL0BkTb34CW2NeH001bnoIEjEJeQI3lEbCVafjTbBXHWptbwL2j9aoiV0XzjkT00-GdtUt1i6DfQO-EWF0J_IC"
            + "_79wEiGfOnM5waWi-LDQ0FnXjV1FIpnOiJab9meIB11sW5MFn2U8q0yMareRHJQ43ZWg5uAnAf2ugm71EsTQtbmKGgDsTzt2"
            + "UglhiNpnONzOEDCzz61FiVUTgWu0wMYzUgitgMJYvaDUit3F2OfQw4x--60VWKhB-q4EGm0DvPgFHMspxcZKNFlqJH3Qfgk8"
            + "LDtJBI0kPpSJoYKbS9n1SmmfVL1UZOTsZNutIYNuN2CWo_D_TJNFKMys6sI7OIQ5QtYyHZyW1wShrR2V2Kwj6IxXpA2XxQf2"
            + "emCRhCNGl5js73ljVnI0HsPLcEzEreRUQWOxgHCuB4dk2QgBj7EiZl57Cm0GywEqDlwf-XX1g";
    MockHttpServer ssoServer = new MockHttpServer(jwt, false, 1, 0);
    try {
      // make sure no creds are cached
      BrowserCredentialPlugin credPlugin =
          (BrowserCredentialPlugin) CredentialPluginLoader.get("MOCK_BROWSER_SSO");
      credPlugin.clearKeyring();
      credPlugin.clearLocalCache();

      String connString =
          String.format("jdbc:singlestore://%s:%s/", hostname, port)
              + sharedConn.getCatalog()
              + "?credentialType=MOCK_BROWSER_SSO"
              + "&sslMode=trust";

      int threadNum = 4;
      Thread[] threads = new Thread[threadNum];
      AtomicInteger errorCount = new AtomicInteger(0);
      for (int i = 0; i < threadNum; ++i) {
        threads[i] =
            new Thread(
                () -> {
                  try (java.sql.Connection connection = DriverManager.getConnection(connString)) {
                    for (int j = 0; j < 10; ++j) {
                      ResultSet rs = connection.createStatement().executeQuery("select 1");
                      assertTrue(rs.next());
                    }
                  } catch (SQLException e) {
                    errorCount.incrementAndGet();
                  }
                });
      }

      for (int i = 0; i < threadNum; ++i) {
        threads[i].start();
      }
      for (int i = 0; i < threadNum; ++i) {
        threads[i].join();
      }

      assertEquals(0, errorCount.get());
    } finally {
      ssoServer.stop();
    }
  }

  @Test
  public void mockBrowserTimeout() throws SQLException {
    // make sure no creds are cached
    BrowserCredentialPlugin credPlugin =
        (BrowserCredentialPlugin) CredentialPluginLoader.get("MOCK_BROWSER_SSO");
    credPlugin.clearKeyring();
    credPlugin.clearLocalCache();

    try {
      TokenWaiterServer.WAIT_TIMEOUT = 1;

      String connString =
          String.format("jdbc:singlestore://%s:%s/", hostname, port)
              + sharedConn.getCatalog()
              + "?credentialType=MOCK_BROWSER_SSO"
              + "&sslMode=trust";

      assertThrowsContains(
          SQLException.class,
          () -> DriverManager.getConnection(connString),
          "Timed out waiting for JWT");
    } finally {
      TokenWaiterServer.WAIT_TIMEOUT = 300;
    }
  }

  private static class MockHttpServer {
    private final HttpServer server;
    private int packetsLeft;

    public MockHttpServer(
        String jwt, boolean shouldHaveEmail, int expectedPackets, int shouldReceiveErrorWithCode)
        throws IOException {
      server = HttpServer.create(new InetSocketAddress(18087), 0);
      packetsLeft = expectedPackets;

      String path = "/";
      server.createContext(
          path,
          exchange -> {
            try {
              if (packetsLeft <= 0) {
                throw new IOException("Got an unexpected packet");
              }
              packetsLeft -= 1;

              Map<String, String> params =
                  URLEncodedUtils.parse(exchange.getRequestURI(), StandardCharsets.UTF_8).stream()
                      .collect(Collectors.toMap(NameValuePair::getName, NameValuePair::getValue));
              assertNotNull(params.get("returnTo"));
              if (shouldHaveEmail) {
                assertNotNull(params.get("email"));
              }

              HttpClient httpclient = HttpClients.createDefault();
              HttpPost httppost = new HttpPost(params.get("returnTo"));
              StringEntity entity = new StringEntity(jwt);
              httppost.setEntity(entity);
              HttpResponse response = httpclient.execute(httppost);

              assertEquals(response.getFirstHeader("Access-Control-Allow-Origin").getValue(), "*");
              if (shouldReceiveErrorWithCode != 0) {
                assertEquals(response.getStatusLine().getStatusCode(), shouldReceiveErrorWithCode);
                assertNotNull(response.getEntity());
              } else {
                assertEquals(response.getStatusLine().getStatusCode(), 204);
                assertNull(response.getEntity());
              }
              exchange.sendResponseHeaders(204, -1);
            } catch (Exception e) {
              exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
              exchange.sendResponseHeaders(400, 0);
              exchange.getResponseBody().write(e.getMessage().getBytes(StandardCharsets.UTF_8));
              exchange.getResponseBody().close();
            } finally {
              exchange.close();
            }
          });
      server.start();
    }

    public void stop() {
      server.stop(0);
    }
  }
}
