// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2021-2022 SingleStore, Inc.

package com.singlestore.jdbc.unit.plugin;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.plugin.credential.browser.ExpiringCredential;
import com.singlestore.jdbc.plugin.credential.browser.TokenWaiterServer;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.time.Instant;
import java.util.concurrent.TimeoutException;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.Test;

public class BrowserAuthTest {

  @Test
  public void TokenWaiterServerTest()
      throws SQLException, IOException, InterruptedException, TimeoutException {
    /*{
      "email": "test-email@gmail.com",
      "username": "test-user",
      "sub": "wrong-user",
      "exp": 1916239022 (year 2030)
    }*/
    String jwt =
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6InRlc3QtZW1haWxAZ21haWwuY29tIiwidXNlcm5hbWUiOiJ0ZXN"
            + "0LXVzZXIiLCJzdWIiOiJ3cm9uZy11c2VyIiwiZXhwIjoxOTE2MjM5MDIyfQ.8gqqlJ7rhmWGROr4BTCRn8WWK891Ti42yxyj0Tn_EGc";
    TokenWaiterServer server = new TokenWaiterServer();
    String path = server.getListenPath();
    HttpClient httpclient = HttpClients.createDefault();
    HttpPost httppost = new HttpPost(path);

    StringEntity entity = new StringEntity(jwt);
    httppost.setEntity(entity);
    HttpResponse response = httpclient.execute(httppost);

    assertEquals(response.getStatusLine().getStatusCode(), 204);
    assertNull(response.getEntity());
    assertEquals(response.getFirstHeader("Access-Control-Allow-Origin").getValue(), "*");
    ExpiringCredential credential = server.WaitForCredential();
    assertEquals("test-email@gmail.com", credential.getEmail());
    assertEquals("test-user", credential.getCredential().getUser());
    assertEquals(jwt, credential.getCredential().getPassword());
    assertEquals(Instant.ofEpochSecond(1916239022), credential.getExpiration());
  }

  @Test
  public void TokenWaiterServerSubOnly()
      throws SQLException, IOException, InterruptedException, TimeoutException {
    /*{
      "email": "test-email@gmail.com",
      "sub": "wrong-user",
      "exp": 1916239022 (year 2030)
    }*/
    String jwt =
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6InRlc3QtZW1haWxAZ21haWwuY29tIiwic3ViIjoid3Jvbmc"
            + "tdXNlciIsImV4cCI6MTkxNjIzOTAyMn0.klj0qsIDlNfSg4CDZ2HjIuxuV101k-hfYDBUc0wzt_w";
    TokenWaiterServer server = new TokenWaiterServer();
    String path = server.getListenPath();
    HttpClient httpclient = HttpClients.createDefault();
    HttpPost httppost = new HttpPost(path);

    StringEntity entity = new StringEntity(jwt);
    httppost.setEntity(entity);
    HttpResponse response = httpclient.execute(httppost);

    assertEquals(response.getStatusLine().getStatusCode(), 204);
    assertNull(response.getEntity());
    assertEquals(response.getFirstHeader("Access-Control-Allow-Origin").getValue(), "*");
    ExpiringCredential credential = server.WaitForCredential();
    assertEquals("test-email@gmail.com", credential.getEmail());
    assertEquals("wrong-user", credential.getCredential().getUser());
    assertEquals(jwt, credential.getCredential().getPassword());
    assertEquals(Instant.ofEpochSecond(1916239022), credential.getExpiration());
  }

  @Test
  public void TokenWaiterServerTimeout() throws SQLException {
    TokenWaiterServer server = new TokenWaiterServer();
    TokenWaiterServer.WAIT_TIMEOUT = 1;
    assertThrows(TimeoutException.class, server::WaitForCredential);
  }

  @Test
  public void TokenWaiterServerErrorsTest() throws SQLException, IOException {
    TokenWaiterServer server = new TokenWaiterServer();
    String path = server.getListenPath();
    HttpClient httpclient = HttpClients.createDefault();

    // non-POST request
    HttpGet httpGet = new HttpGet(path);
    HttpResponse response = httpclient.execute(httpGet);
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    assertNotNull(response.getEntity());
    response.getEntity().writeTo(stream);
    assertEquals("POST expected", stream.toString());
    assertEquals(400, response.getStatusLine().getStatusCode());
    assertEquals(response.getFirstHeader("Access-Control-Allow-Origin").getValue(), "*");

    // invalid JWT
    String jwt = "wrong-jwt";
    HttpPost httppost = new HttpPost(path);

    StringEntity entity = new StringEntity(jwt);
    httppost.setEntity(entity);
    response = httpclient.execute(httppost);
    stream = new ByteArrayOutputStream();
    assertNotNull(response.getEntity());
    response.getEntity().writeTo(stream);
    assertEquals(
        "Could not parse claims: The token was expected to have 3 parts, but got 1.",
        stream.toString());
    assertEquals(400, response.getStatusLine().getStatusCode());
    assertEquals(response.getFirstHeader("Access-Control-Allow-Origin").getValue(), "*");

    // missing JWT claims
    /*{
      "not-email": "test-email@gmail.com",
      "dbUsername": "test-user",
      "exp": 1916239022
    }*/
    jwt =
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJub3QtZW1haWwiOiJ0ZXN0LWVtYWlsQGdtYWlsLmNvbSIsImRiVXNlcm5hb"
            + "WUiOiJ0ZXN0LXVzZXIiLCJleHAiOjE5MTYyMzkwMjJ9.B4Ry70_6jDvK0n-qEzAnihgsKnL0PivYbPY2BLSandc";
    httppost = new HttpPost(path);

    entity = new StringEntity(jwt);
    httppost.setEntity(entity);
    response = httpclient.execute(httppost);
    stream = new ByteArrayOutputStream();
    assertNotNull(response.getEntity());
    response.getEntity().writeTo(stream);
    assertEquals(
        "Could not verify claims: The Claim 'email' is not present in the JWT.", stream.toString());
    assertEquals(400, response.getStatusLine().getStatusCode());
    assertEquals(response.getFirstHeader("Access-Control-Allow-Origin").getValue(), "*");

    // expired JWT
    /*{
      "email": "test-email@gmail.com",
      "dbUsername": "test-user",
      "exp": 1000000000
    }*/
    jwt =
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6InRlc3QtZW1haWxAZ21haWwuY29tIiwiZGJVc2VybmFtZSI6In"
            + "Rlc3QtdXNlciIsImV4cCI6MTAwMDAwMDAwMH0.yemofu_U02SMEtVg8Mu7XJ9J8clbCuhdacAR0jWY4X0";
    httppost = new HttpPost(path);

    entity = new StringEntity(jwt);
    httppost.setEntity(entity);
    response = httpclient.execute(httppost);
    stream = new ByteArrayOutputStream();
    assertNotNull(response.getEntity());
    response.getEntity().writeTo(stream);
    assertTrue(stream.toString().contains("Could not verify claims: The Token has expired on"));
    assertEquals(400, response.getStatusLine().getStatusCode());
    assertEquals(response.getFirstHeader("Access-Control-Allow-Origin").getValue(), "*");

    // no expiration JWT
    /*{
      "email": "test-email@gmail.com",
      "dbUsername": "test-user",
    }*/
    jwt =
        "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJlbWFpbCI6InRlc3QtZW1haWxAZ21haWwuY29tIiwiZGJVc2VybmFtZSI6In"
            + "Rlc3QtdXNlciJ9.KqJ5aJVDK08y91zrUvlpV3pONgNgYE1iNRRtiUS5HWk";
    httppost = new HttpPost(path);

    entity = new StringEntity(jwt);
    httppost.setEntity(entity);
    response = httpclient.execute(httppost);
    stream = new ByteArrayOutputStream();
    assertNotNull(response.getEntity());
    response.getEntity().writeTo(stream);
    assertEquals(
        "Could not verify claims: The Claim 'exp' is not present in the JWT.", stream.toString());
    assertEquals(400, response.getStatusLine().getStatusCode());
    assertEquals(response.getFirstHeader("Access-Control-Allow-Origin").getValue(), "*");
  }
}
