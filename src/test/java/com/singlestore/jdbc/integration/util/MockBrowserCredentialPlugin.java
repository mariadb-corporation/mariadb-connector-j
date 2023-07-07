package com.singlestore.jdbc.integration.util;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.singlestore.jdbc.Configuration;
import com.singlestore.jdbc.HostAddress;
import com.singlestore.jdbc.plugin.CredentialPlugin;
import com.singlestore.jdbc.plugin.credential.browser.BrowserCredentialGenerator;
import com.singlestore.jdbc.plugin.credential.browser.BrowserCredentialPlugin;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.sql.SQLException;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;

public class MockBrowserCredentialPlugin extends BrowserCredentialPlugin {
  private static final String baseURL = "http://127.0.0.1:18087";

  @Override
  public String type() {
    return "MOCK_BROWSER_SSO";
  }

  @Override
  public CredentialPlugin initialize(Configuration conf, String userName, HostAddress hostAddress)
      throws SQLException {
    this.generator = new MockBrowserCredentialGenerator(baseURL);
    return this;
  }
}

class MockBrowserCredentialGenerator extends BrowserCredentialGenerator {

  public MockBrowserCredentialGenerator(String baseURL) {
    super(baseURL);
  }

  @Override
  protected void openBrowser(String url) throws SQLException {
    HttpClient httpclient = HttpClients.createDefault();
    HttpPost httppost = new HttpPost(url);

    try {
      HttpResponse response = httpclient.execute(httppost);
      if (response.getStatusLine().getStatusCode() != 204) {
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        assertNotNull(response.getEntity());
        response.getEntity().writeTo(stream);
        throw new SQLException(stream.toString());
      }
    } catch (IOException ignored) {
    }
  }
}
