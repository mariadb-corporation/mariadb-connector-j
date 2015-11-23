package org.mariadb.jdbc;

import org.junit.*;

import java.io.*;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import static org.junit.Assert.assertEquals;

public class SslTest extends BaseTest {
    String serverCertificatePath;

    /**
     * Enable Crypto.
     */
    @BeforeClass
    public static void enableCrypto() {
        try {
            Field field = Class.forName("javax.crypto.JceSecurity").getDeclaredField("isRestricted");
            field.setAccessible(true);
            field.set(null, java.lang.Boolean.FALSE);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    /**
     * Check requirement.
     *
     * @throws SQLException exception exception
     */
    @Before
    public void checkSsl() throws SQLException {
        boolean isJava7 = System.getProperty("java.version").contains("1.7.");
        Assume.assumeTrue(haveSsl(sharedConnection));
        //Skip SSL test on java 7 since SSL stream size JDK-6521495).
        Assume.assumeFalse(isJava7);
        ResultSet rs = sharedConnection.createStatement().executeQuery("select @@ssl_cert");
        rs.next();
        serverCertificatePath = rs.getString(1);
        log.trace("Server certificate path: {}", serverCertificatePath);
        rs.close();
    }

    @Test
    public void useSsl() throws Exception {
        Assume.assumeTrue(haveSsl(sharedConnection));
        //Skip SSL test on java 7 since SSL stream size JDK-6521495).
        Assume.assumeFalse(System.getProperty("java.version").contains("1.7."));
        Connection connection = setConnection("&useSSL=true&trustServerCertificate=true");
        try {
            connection.createStatement().execute("select 1");
        } finally {
            connection.close();
        }
    }

    private String getServerCertificate() {
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(serverCertificatePath)));
            StringBuilder sb = new StringBuilder();
            String line = null;
            while ((line = br.readLine()) != null) {
                sb.append(line);
                sb.append("\n");
            }
            return sb.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (Exception e) {
                    //eat exception
                }
            }
        }
    }

    private void saveToFile(String path, String contents) {
        PrintWriter out = null;
        try {
            out = new PrintWriter(new FileOutputStream(path));
            out.print(contents);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (Exception e) {
                    //eat exception
                }
            }
        }
    }

    private Connection createConnection(Properties info) throws SQLException {
        String jdbcUrl = connUri;
        Properties connProps = new Properties(info);
        connProps.setProperty("user", username);
        if (password != null) {
            connProps.setProperty("password", password);
        }
        return openNewConnection(jdbcUrl, connProps);
    }

    /**
     * Test connection.
     *
     * @param info connection properties
     * @param sslExpected is SSL expected
     * @throws SQLException exception
     */
    public void testConnect(Properties info, boolean sslExpected) throws SQLException {
        Connection conn = null;
        Statement stmt = null;

        conn = createConnection(info);
        // First do a basic select test:
        stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery("SELECT 1");
        rs.next();
        Assert.assertTrue(rs.getInt(1) == 1);
        rs.close();

        // Then check if SSL matches what is expected
        rs = stmt.executeQuery("SHOW STATUS LIKE 'Ssl_cipher'");
        rs.next();
        String sslCipher = rs.getString(2);
        boolean sslActual = sslCipher != null && sslCipher.length() > 0;
        Assert.assertEquals("sslExpected does not match", sslExpected, sslActual);

    }

    @Test
    public void testConnectNonSsl() throws SQLException {
        Properties info = new Properties();
        testConnect(info, false);
    }

    @Test
    public void testTrustedServer() throws SQLException {
        Properties info = new Properties();
        info.setProperty("useSSL", "true");
        info.setProperty("trustServerCertificate", "true");
        testConnect(info, true);
    }

    @Test
    public void testServerCertString() throws SQLException {
        Properties info = new Properties();
        info.setProperty("useSSL", "true");
        info.setProperty("serverSslCert", getServerCertificate());
        testConnect(info, true);
    }

    @Test(expected = SQLException.class)
    public void testBadServerCertString() throws SQLException {
        Properties info = new Properties();
        info.setProperty("useSSL", "true");
        info.setProperty("serverSslCert", "foobar");
        testConnect(info, true);
    }

    @Test
    public void testServerCertFile() throws SQLException {
        Properties info = new Properties();
        info.setProperty("useSSL", "true");
        info.setProperty("serverSslCert", serverCertificatePath);
        testConnect(info, true);
    }

    @Test
    public void testServerCertClasspathFile() throws SQLException {
        Assume.assumeTrue(new File("target/classes").isDirectory());
        Properties info = new Properties();
        info.setProperty("useSSL", "true");
        // Copy the valid server certificate to a known location on the classpath:
        String classpathFilename = "server-ssl-cert.pem";
        saveToFile("target/classes/" + classpathFilename, getServerCertificate());
        info.setProperty("serverSslCert", "classpath:" + classpathFilename);
        testConnect(info, true);
    }

    @Test(expected = SQLException.class)
    public void testWrongServerCert() throws Throwable {
        Properties info = new Properties();
        info.setProperty("useSSL", "true");
        info.setProperty("serverSslCert", "classpath:ssl/wrong-server.crt");
        testConnect(info, true);
    }

    @Test
    public void conc71() throws SQLException {
        try {
            Properties info = new Properties();
            info.setProperty("serverSslCert", getServerCertificate());
            info.setProperty("useSSL", "true");
            Connection conn = createConnection(info);
            assertEquals("testj", conn.getCatalog());
            conn.close();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
