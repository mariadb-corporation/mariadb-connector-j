// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc.integration.codec;

import static org.junit.jupiter.api.Assertions.*;

import java.io.OutputStream;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.MariaDbSqlXml;
import org.mariadb.jdbc.Statement;
import org.w3c.dom.Document;

public class XmlCodecTest extends CommonCodecTest {

  private static final String XML1 = "<root><a attr=\"1\">é€😀</a><b/></root>";
  private static final String XML2 = "<?xml version=\"1.0\"?><x>2</x>";

  @AfterAll
  public static void drop() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP TABLE IF EXISTS XmlCodec");
    stmt.execute("DROP TABLE IF EXISTS XmlParamCodec");
  }

  @BeforeAll
  public static void beforeAll2() throws SQLException {
    Assumptions.assumeTrue(isMariaDBServer() && minVersion(12, 3, 1));
    drop();
    Statement stmt = sharedConn.createStatement();
    stmt.execute("CREATE TABLE XmlCodec (t1 XMLTYPE, t2 XMLTYPE, t3 TEXT, t4 XMLTYPE)");
    stmt.execute("INSERT INTO XmlCodec VALUES ('" + XML1 + "', '" + XML2 + "', '<t/>', null)");
    stmt.execute(
        "CREATE TABLE XmlParamCodec(id int not null primary key auto_increment, t1 XMLTYPE)");
    stmt.execute("FLUSH TABLES");
  }

  private ResultSet get() throws SQLException {
    Statement stmt = sharedConn.createStatement();
    stmt.execute("START TRANSACTION");
    ResultSet rs =
        stmt.executeQuery(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from XmlCodec");
    assertTrue(rs.next());
    sharedConn.commit();
    return rs;
  }

  private ResultSet getPrepare(Connection con) throws SQLException {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("START TRANSACTION");
    PreparedStatement preparedStatement =
        con.prepareStatement(
            "select t1 as t1alias, t2 as t2alias, t3 as t3alias, t4 as t4alias from XmlCodec"
                + " WHERE 1 > ?");
    preparedStatement.closeOnCompletion();
    preparedStatement.setInt(1, 0);
    ResultSet rs = preparedStatement.executeQuery();
    assertTrue(rs.next());
    con.commit();
    return rs;
  }

  @Test
  public void getObject() throws Exception {
    getObject(get());
    getObject(getPrepare(sharedConn));
    getObject(getPrepare(sharedConnBinary));
  }

  private void getObject(ResultSet rs) throws Exception {
    Object o = rs.getObject(1);
    assertInstanceOf(SQLXML.class, o);
    assertEquals(XML1, ((SQLXML) o).getString());
    assertEquals(XML2, ((SQLXML) rs.getObject("t2alias")).getString());
    // a plain text column stays a String
    assertEquals("<t/>", rs.getObject(3));
    assertNull(rs.getObject(4));
    assertTrue(rs.wasNull());

    assertEquals(XML1, rs.getObject(1, SQLXML.class).getString());
    assertEquals(XML1, rs.getObject(1, String.class));
    // any text column can be read as SQLXML
    assertEquals("<t/>", rs.getObject(3, SQLXML.class).getString());
    assertNull(rs.getObject(4, SQLXML.class));
  }

  @Test
  public void getSQLXML() throws Exception {
    getSQLXML(get());
    getSQLXML(getPrepare(sharedConn));
    getSQLXML(getPrepare(sharedConnBinary));
  }

  private void getSQLXML(ResultSet rs) throws Exception {
    SQLXML xml = rs.getSQLXML(1);
    assertEquals(XML1, xml.getString());
    // readable once
    assertThrowsContains(SQLException.class, xml::getString, "only be read once");
    assertEquals(XML2, rs.getSQLXML("t2alias").getString());
    assertEquals("<t/>", rs.getSQLXML(3).getString());
    assertNull(rs.getSQLXML(4));
    assertTrue(rs.wasNull());
    assertNull(rs.getSQLXML("t4alias"));

    // text getters still work on an XML column
    assertEquals(XML1, rs.getString(1));
    assertEquals(XML1, rs.getNString(1));
    assertEquals(XML1, rs.getClob(1).getSubString(1, XML1.length()));
    assertArrayEquals(XML1.getBytes(StandardCharsets.UTF_8), rs.getBytes(1));
    assertReaderEquals(new java.io.StringReader(XML1), rs.getCharacterStream(1));
  }

  @Test
  public void getSources() throws Exception {
    getSources(get());
    getSources(getPrepare(sharedConnBinary));
  }

  private void getSources(ResultSet rs) throws Exception {
    StreamSource stream = rs.getSQLXML(1).getSource(StreamSource.class);
    java.io.StringWriter sw = new java.io.StringWriter();
    stream.getReader().transferTo(sw);
    assertEquals(XML1, sw.toString());

    DOMSource dom = rs.getSQLXML(1).getSource(DOMSource.class);
    Document doc = (Document) dom.getNode();
    assertEquals("root", doc.getDocumentElement().getTagName());
    assertEquals("é€😀", doc.getElementsByTagName("a").item(0).getTextContent());
    assertEquals(
        "1",
        doc.getElementsByTagName("a").item(0).getAttributes().getNamedItem("attr").getNodeValue());

    SAXSource sax = rs.getSQLXML(1).getSource(SAXSource.class);
    assertNotNull(sax.getXMLReader());
    DOMResult saxDom = new DOMResult();
    TransformerFactory.newInstance().newTransformer().transform(sax, saxDom);
    assertEquals(
        "é€😀", ((Document) saxDom.getNode()).getElementsByTagName("a").item(0).getTextContent());

    StAXSource stax = rs.getSQLXML(1).getSource(StAXSource.class);
    assertNotNull(stax.getXMLStreamReader());
    stax.getXMLStreamReader().next();
    assertEquals("root", stax.getXMLStreamReader().getLocalName());

    // default source
    assertInstanceOf(StreamSource.class, rs.getSQLXML(1).getSource(null));
    // external entities are not resolved
    SQLXML xxe =
        new MariaDbSqlXml("<!DOCTYPE x [<!ENTITY e SYSTEM \"file:///etc/hostname\">]><x>&e;</x>");
    try {
      Document xxeDoc = (Document) xxe.getSource(DOMSource.class).getNode();
      assertEquals("", xxeDoc.getDocumentElement().getTextContent());
    } catch (SQLException e) {
      // rejecting the document is fine too
    }
  }

  @Test
  public void metadata() throws Exception {
    metadata(get());
    metadata(getPrepare(sharedConnBinary));
    try (ResultSet rs = sharedConn.getMetaData().getTypeInfo()) {
      boolean found = false;
      while (rs.next()) {
        if ("XMLTYPE".equals(rs.getString("TYPE_NAME"))) {
          found = true;
          assertEquals(Types.SQLXML, rs.getInt("DATA_TYPE"));
        }
      }
      assertTrue(found);
    }
    try (ResultSet rs = sharedConn.getMetaData().getColumns(null, null, "XmlCodec", "t1")) {
      assertTrue(rs.next());
      assertEquals("XMLTYPE", rs.getString("TYPE_NAME").toUpperCase());
    }
  }

  private void metadata(ResultSet rs) throws Exception {
    ResultSetMetaData meta = rs.getMetaData();
    assertEquals("XML", meta.getColumnTypeName(1));
    assertEquals(Types.SQLXML, meta.getColumnType(1));
    assertEquals("java.sql.SQLXML", meta.getColumnClassName(1));
    assertEquals("TEXT", meta.getColumnTypeName(3));
    assertEquals(Types.VARCHAR, meta.getColumnType(3));
    assertEquals("XML", meta.getColumnTypeName(4));
  }

  @Test
  public void sendParam() throws Exception {
    sendParam(sharedConn);
    sendParam(sharedConnBinary);
    try (Connection con = createCon("transactionReplay=true")) {
      sendParam(con);
    }
  }

  private void sendParam(Connection con) throws Exception {
    java.sql.Statement stmt = con.createStatement();
    stmt.execute("TRUNCATE TABLE XmlParamCodec");
    stmt.execute("START TRANSACTION");
    try (PreparedStatement prep =
        con.prepareStatement("INSERT INTO XmlParamCodec(t1) VALUES (?)")) {
      // setSQLXML with each writing method
      SQLXML xml = con.createSQLXML();
      xml.setString(XML1);
      prep.setSQLXML(1, xml);
      prep.execute();

      xml = con.createSQLXML();
      try (Writer writer = xml.setCharacterStream()) {
        writer.write(XML2);
      }
      prep.setSQLXML(1, xml);
      prep.execute();

      xml = con.createSQLXML();
      try (OutputStream os = xml.setBinaryStream()) {
        os.write(XML1.getBytes(StandardCharsets.UTF_8));
      }
      prep.setSQLXML(1, xml);
      prep.execute();

      xml = con.createSQLXML();
      StreamResult streamResult = xml.setResult(StreamResult.class);
      streamResult.getWriter().write(XML2);
      prep.setSQLXML(1, xml);
      prep.execute();

      xml = con.createSQLXML();
      DOMResult domResult = xml.setResult(DOMResult.class);
      Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().newDocument();
      org.w3c.dom.Element root = doc.createElement("dom");
      root.setTextContent("é");
      doc.appendChild(root);
      Transformer transformer = TransformerFactory.newInstance().newTransformer();
      transformer.transform(new DOMSource(doc), domResult);
      prep.setSQLXML(1, xml);
      prep.execute();

      xml = con.createSQLXML();
      SAXResult saxResult = xml.setResult(SAXResult.class);
      transformer.setOutputProperty("omit-xml-declaration", "yes");
      transformer.transform(new StreamSource(new java.io.StringReader("<sax>1</sax>")), saxResult);
      prep.setSQLXML(1, xml);
      prep.execute();

      // setObject
      prep.setObject(1, new MariaDbSqlXml(XML1));
      prep.execute();
      prep.setObject(1, new MariaDbSqlXml(XML2), Types.SQLXML);
      prep.execute();
      // a String is accepted by the server for an XMLTYPE column
      prep.setString(1, XML1);
      prep.execute();
      prep.setNull(1, Types.SQLXML);
      prep.execute();
      prep.setSQLXML(1, null);
      prep.execute();

      // writable once, not readable before a value is set
      SQLXML empty = con.createSQLXML();
      assertThrowsContains(SQLException.class, empty::getString, "not readable");
      empty.setString(XML1);
      assertThrowsContains(SQLException.class, () -> empty.setString(XML2), "not writable");
      empty.free();
      assertThrowsContains(SQLException.class, empty::getString, "freed");

      // the server validates the content
      prep.setString(1, "not xml");
      assertThrowsContains(SQLException.class, prep::execute, "XMLTYPE");
    }

    ResultSet rs = con.createStatement().executeQuery("SELECT t1 FROM XmlParamCodec ORDER BY id");
    assertTrue(rs.next());
    assertEquals(XML1, rs.getSQLXML(1).getString());
    assertTrue(rs.next());
    assertEquals(XML2, rs.getSQLXML(1).getString());
    assertTrue(rs.next());
    assertEquals(XML1, rs.getSQLXML(1).getString());
    assertTrue(rs.next());
    assertEquals(XML2, rs.getSQLXML(1).getString());
    assertTrue(rs.next());
    assertEquals("<dom>é</dom>", rs.getString(1));
    assertTrue(rs.next());
    assertEquals("<sax>1</sax>", rs.getString(1));
    assertTrue(rs.next());
    assertEquals(XML1, rs.getString(1));
    assertTrue(rs.next());
    assertEquals(XML2, rs.getString(1));
    assertTrue(rs.next());
    assertEquals(XML1, rs.getString(1));
    assertTrue(rs.next());
    assertNull(rs.getSQLXML(1));
    assertTrue(rs.next());
    assertNull(rs.getObject(1));
    assertFalse(rs.next());
    con.commit();
  }

  @Test
  public void updatable() throws Exception {
    java.sql.Statement stmt = sharedConn.createStatement();
    stmt.execute("TRUNCATE TABLE XmlParamCodec");
    stmt.execute("INSERT INTO XmlParamCodec(t1) VALUES ('<x/>')");
    try (Statement st =
        sharedConn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
      ResultSet rs = st.executeQuery("SELECT id, t1 FROM XmlParamCodec");
      assertTrue(rs.next());
      SQLXML xml = sharedConn.createSQLXML();
      xml.setString(XML2);
      rs.updateSQLXML(2, xml);
      rs.updateRow();
      rs.moveToInsertRow();
      rs.updateInt(1, 10);
      xml = sharedConn.createSQLXML();
      xml.setString(XML1);
      rs.updateSQLXML("t1", xml);
      rs.insertRow();
    }
    ResultSet rs = stmt.executeQuery("SELECT t1 FROM XmlParamCodec ORDER BY id");
    assertTrue(rs.next());
    assertEquals(XML2, rs.getSQLXML(1).getString());
    assertTrue(rs.next());
    assertEquals(XML1, rs.getSQLXML(1).getString());
    assertFalse(rs.next());
  }

  @Test
  public void callable() throws Exception {
    java.sql.Statement stmt = sharedConn.createStatement();
    stmt.execute("DROP PROCEDURE IF EXISTS xmlProc");
    stmt.execute("CREATE PROCEDURE xmlProc(IN i XMLTYPE, OUT o XMLTYPE) BEGIN SET o = i; END");
    try (CallableStatement call = sharedConn.prepareCall("{call xmlProc(?, ?)}")) {
      SQLXML xml = sharedConn.createSQLXML();
      xml.setString(XML1);
      call.setSQLXML(1, xml);
      call.registerOutParameter(2, Types.SQLXML);
      call.execute();
      assertEquals(XML1, call.getSQLXML(2).getString());
    }
    try (CallableStatement call = sharedConn.prepareCall("{call xmlProc(?, ?)}")) {
      call.setSQLXML("i", new MariaDbSqlXml(XML2));
      call.registerOutParameter("o", Types.SQLXML);
      call.execute();
      assertEquals(XML2, call.getSQLXML("o").getString());
    }
    stmt.execute("DROP PROCEDURE xmlProc");
  }
}
