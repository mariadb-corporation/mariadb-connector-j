package org.mariadb.jdbc;

import static org.junit.Assert.assertEquals;

import java.sql.SQLException;
import org.junit.Test;
import org.mariadb.jdbc.internal.failover.impl.AuroraListener;
import org.mariadb.jdbc.internal.util.pool.GlobalStateInfo;

public class AuroraListenerTest {

  public static final String EXPECTED_CLUSTER_DNS_SUFFIX =
      "ZZZZZZZZZZZZ.us-east-1.rds.amazonaws.com";

  @Test
  public void testWithWriterCluster() throws SQLException {
    GlobalStateInfo globalInfo = new GlobalStateInfo();
    UrlParser urlParser =
        UrlParser.parse(
            "jdbc:mysql:aurora://XXXX.cluster-ZZZZZZZZZZZZ.us-east-1.rds.amazonaws.com:3306/XXXX");
    AuroraListener listener = new AuroraListener(urlParser, globalInfo);

    assertEquals(listener.getClusterDnsSuffix(), EXPECTED_CLUSTER_DNS_SUFFIX);
  }

  @Test
  public void testWithReaderCluster() throws SQLException {
    GlobalStateInfo globalInfo = new GlobalStateInfo();
    UrlParser urlParser =
        UrlParser.parse(
            "jdbc:mysql:aurora://XXXX.cluster-ro-ZZZZZZZZZZZZ.us-east-1.rds.amazonaws.com:3306/XXXX");
    AuroraListener listener = new AuroraListener(urlParser, globalInfo);

    assertEquals(listener.getClusterDnsSuffix(), EXPECTED_CLUSTER_DNS_SUFFIX);
  }
}
