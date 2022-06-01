// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2021-2022 SingleStore, Inc.

package com.singlestore.jdbc.plugin.credential.browser;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTDecodeException;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.auth0.jwt.exceptions.SignatureGenerationException;
import com.auth0.jwt.exceptions.SignatureVerificationException;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.singlestore.jdbc.plugin.credential.Credential;
import com.singlestore.jdbc.util.log.Logger;
import com.singlestore.jdbc.util.log.Loggers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

public class TokenWaiterServer {
  private static final Logger logger = Loggers.getLogger(TokenWaiterServer.class);
  // time to wait for a JWT to be received before throwing in seconds.
  // Public for test purposes
  public static int WAIT_TIMEOUT = 300;
  private final CountDownLatch latch = new CountDownLatch(1);
  private final String listenPath;
  private final HttpServer server;
  private ExpiringCredential credential;
  private IOException handleException;

  public TokenWaiterServer() throws SQLException {
    try {
      server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    } catch (IOException e) {
      throw new SQLException(
          "Could not create a local HTTP server while using identity plugin 'BROWSER_SSO'", e);
    }

    String path = "/" + randomAlphanumeric(20);
    server.createContext(path, new RequestHandler(this));
    listenPath = "http://127.0.0.1:" + server.getAddress().getPort() + path;
    server.start();
  }

  public ExpiringCredential WaitForCredential()
      throws InterruptedException, TimeoutException, IOException {
    try {
      boolean result = latch.await(WAIT_TIMEOUT, TimeUnit.SECONDS);
      if (!result) {
        throw new TimeoutException();
      }
      if (handleException != null) {
        throw handleException;
      }
      return credential;
    } finally {
      server.stop(0);
    }
  }

  public String getListenPath() {
    return listenPath;
  }

  public void setCredential(ExpiringCredential cred) {
    credential = cred;
    latch.countDown();
  }

  public void setHandleException(IOException e) {
    handleException = e;
    latch.countDown();
  }

  // from https://www.baeldung.com/java-random-string 'Generate Random Alphanumeric String With Java
  // 8'
  private String randomAlphanumeric(int len) {
    Random random = new Random();
    return random
        .ints(48, 123)
        .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
        .limit(len)
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();
  }

  private static class RequestHandler implements HttpHandler {
    private final TokenWaiterServer server;

    public RequestHandler(TokenWaiterServer server) {
      this.server = server;
    }

    public void handle(HttpExchange exchange) throws IOException {
      exchange.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
      if (!exchange.getRequestMethod().equals("POST")) {
        error(exchange, 400, "POST expected");
        server.setHandleException(
            new IOException("POST request expected, got " + exchange.getRequestMethod()));
        return;
      }

      // read the whole response
      String raw;
      try {
        raw =
            new BufferedReader(new InputStreamReader(exchange.getRequestBody()))
                .lines()
                .parallel()
                .collect(Collectors.joining("\n"));
      } catch (Exception e) {
        error(exchange, 500, "Bad read from request");
        server.setHandleException(new IOException("Bad read from request: ", e));
        return;
      }

      DecodedJWT jwt;
      try {
        jwt = JWT.decode(raw);
      } catch (JWTDecodeException e) {
        error(exchange, 400, "Could not parse claims: " + e.getMessage());
        server.setHandleException(new IOException("Could not parse claims: ", e));
        return;
      }

      JWTVerifier ver =
          JWT.require(new DummyAlgorithm(jwt.getAlgorithm())).withClaimPresence("email").build();
      try {
        ver.verify(jwt);
        // a bug in the jwt lib doesn't allow us to check whether expiration is specified through
        // JWT.require().
        // Though if 'exp' is in the JWT, it is correctly validated against the current time.
        if (jwt.getExpiresAt() == null) {
          throw new JWTVerificationException("The Claim 'exp' is not present in the JWT.");
        }
        if (jwt.getClaim("sub").isNull() && jwt.getClaim("username").isNull()) {
          throw new JWTVerificationException(
              "One of claims 'sub' and 'username' must be present in the JWT.");
        }
      } catch (JWTVerificationException e) {
        error(exchange, 400, "Could not verify claims: " + e.getMessage());
        server.setHandleException(new IOException("Could not verify claims: ", e));
        return;
      }

      exchange.sendResponseHeaders(204, -1);
      exchange.close();

      server.setCredential(
          new ExpiringCredential(
              new Credential(
                  jwt.getClaim("username").isNull()
                      ? jwt.getClaim("sub").asString()
                      : jwt.getClaim("username").asString(),
                  jwt.getToken()),
              jwt.getClaim("email").asString(),
              jwt.getExpiresAt().toInstant()));
    }

    private void error(HttpExchange exchange, int code, String errorMsg) throws IOException {
      exchange.getResponseHeaders().set("Content-Type", "text/plain; charset=utf-8");
      exchange.getResponseHeaders().set("X-Content-Type-Options", "nosniff");
      exchange.sendResponseHeaders(code, 0);
      exchange.getResponseBody().write(errorMsg.getBytes(StandardCharsets.UTF_8));
      exchange.getResponseBody().close();
    }
  }

  private static class DummyAlgorithm extends Algorithm {
    public DummyAlgorithm(String name) {
      super(name, "Does not do any signature verification. Used to only verify claims for a token");
    }

    @Override
    public void verify(DecodedJWT decodedJWT) throws SignatureVerificationException {}

    @Override
    public byte[] sign(byte[] bytes) throws SignatureGenerationException {
      return null;
    }
  }
}
