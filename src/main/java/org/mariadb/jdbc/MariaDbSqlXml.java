// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2026 MariaDB plc
package org.mariadb.jdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.sql.SQLXML;
import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLOutputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.sax.SAXResult;
import javax.xml.transform.sax.SAXSource;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stax.StAXResult;
import javax.xml.transform.stax.StAXSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;

/**
 * {@link SQLXML} implementation holding the XML value as a UTF-8 string.
 *
 * <p>An instance read from a result-set is readable once; an instance created by {@link
 * java.sql.Connection#createSQLXML()} is writable once through one of the {@code setXxx} methods,
 * after which it is readable. XML parsing (DOM, SAX and StAX sources) is done with external entity
 * resolution disabled.
 */
public class MariaDbSqlXml implements SQLXML {

  private String value;
  private boolean readable;
  private boolean writable;
  private boolean freed;
  private ByteArrayOutputStream pendingBytes;
  private StringWriter pendingChars;
  private DOMResult pendingDom;

  /** Create an empty, writable value. */
  public MariaDbSqlXml() {
    this.writable = true;
  }

  /**
   * Create a readable value.
   *
   * @param value XML content
   */
  public MariaDbSqlXml(String value) {
    this.value = value;
    this.readable = true;
  }

  private void checkFreed() throws SQLException {
    if (freed) throw new SQLException("SQLXML object has been freed");
  }

  private void checkReadable() throws SQLException {
    checkFreed();
    collectPending();
    if (!readable) {
      throw new SQLException(
          value == null
              ? "SQLXML object is not readable: no value has been set"
              : "SQLXML object is not readable anymore: it can only be read once");
    }
    readable = false;
  }

  private void checkWritable() throws SQLException {
    checkFreed();
    if (!writable) throw new SQLException("SQLXML object is not writable: value already set");
    writable = false;
  }

  /** Content written through a stream, writer or DOM result is captured when read. */
  private void collectPending() throws SQLException {
    if (pendingBytes != null) {
      value = pendingBytes.toString(StandardCharsets.UTF_8);
      pendingBytes = null;
      readable = true;
    } else if (pendingChars != null) {
      value = pendingChars.toString();
      pendingChars = null;
      readable = true;
    } else if (pendingDom != null) {
      value = serialize(new DOMSource(pendingDom.getNode()));
      pendingDom = null;
      readable = true;
    }
  }

  /**
   * XML content, without read-once state change.
   *
   * @return XML content
   * @throws SQLException if the object is freed or no value has been set
   */
  public String getValue() throws SQLException {
    checkFreed();
    collectPending();
    if (value == null) throw new SQLException("SQLXML object has no value");
    return value;
  }

  @Override
  public void free() {
    freed = true;
    value = null;
    pendingBytes = null;
    pendingChars = null;
    pendingDom = null;
  }

  @Override
  public InputStream getBinaryStream() throws SQLException {
    checkReadable();
    return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
  }

  @Override
  public OutputStream setBinaryStream() throws SQLException {
    checkWritable();
    pendingBytes = new ByteArrayOutputStream();
    return pendingBytes;
  }

  @Override
  public Reader getCharacterStream() throws SQLException {
    checkReadable();
    return new StringReader(value);
  }

  @Override
  public Writer setCharacterStream() throws SQLException {
    checkWritable();
    pendingChars = new StringWriter();
    return pendingChars;
  }

  @Override
  public String getString() throws SQLException {
    checkReadable();
    return value;
  }

  @Override
  public void setString(String value) throws SQLException {
    checkWritable();
    if (value == null) throw new SQLException("value cannot be null");
    this.value = value;
    this.readable = true;
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Source> T getSource(Class<T> sourceClass) throws SQLException {
    checkReadable();
    try {
      if (sourceClass == null || StreamSource.class.equals(sourceClass)) {
        return (T) new StreamSource(new StringReader(value));
      }
      if (DOMSource.class.equals(sourceClass)) {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_DTD, "");
        factory.setAttribute(XMLConstants.ACCESS_EXTERNAL_SCHEMA, "");
        factory.setExpandEntityReferences(false);
        DocumentBuilder builder = factory.newDocumentBuilder();
        return (T) new DOMSource(builder.parse(new InputSource(new StringReader(value))));
      }
      if (SAXSource.class.equals(sourceClass)) {
        SAXParserFactory factory = SAXParserFactory.newInstance();
        factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        factory.setFeature("http://xml.org/sax/features/external-general-entities", false);
        factory.setFeature("http://xml.org/sax/features/external-parameter-entities", false);
        XMLReader reader = factory.newSAXParser().getXMLReader();
        return (T) new SAXSource(reader, new InputSource(new StringReader(value)));
      }
      if (StAXSource.class.equals(sourceClass)) {
        XMLInputFactory factory = XMLInputFactory.newInstance();
        factory.setProperty(XMLInputFactory.IS_SUPPORTING_EXTERNAL_ENTITIES, false);
        factory.setProperty(XMLInputFactory.SUPPORT_DTD, false);
        return (T) new StAXSource(factory.createXMLStreamReader(new StringReader(value)));
      }
    } catch (ParserConfigurationException
        | SAXException
        | IOException
        | XMLStreamException
        | IllegalArgumentException e) {
      throw new SQLException("Failed to create " + sourceClass.getName() + " from XML value", e);
    }
    throw new SQLException("Unsupported Source type " + sourceClass.getName());
  }

  @Override
  @SuppressWarnings("unchecked")
  public <T extends Result> T setResult(Class<T> resultClass) throws SQLException {
    checkWritable();
    try {
      if (resultClass == null || StreamResult.class.equals(resultClass)) {
        pendingChars = new StringWriter();
        return (T) new StreamResult(pendingChars);
      }
      if (DOMResult.class.equals(resultClass)) {
        pendingDom = new DOMResult();
        return (T) pendingDom;
      }
      if (SAXResult.class.equals(resultClass)) {
        SAXTransformerFactory factory = (SAXTransformerFactory) TransformerFactory.newInstance();
        factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
        TransformerHandler handler = factory.newTransformerHandler();
        handler.getTransformer().setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        handler.getTransformer().setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        pendingChars = new StringWriter();
        handler.setResult(new StreamResult(pendingChars));
        return (T) new SAXResult(handler);
      }
      if (StAXResult.class.equals(resultClass)) {
        pendingChars = new StringWriter();
        return (T)
            new StAXResult(XMLOutputFactory.newInstance().createXMLStreamWriter(pendingChars));
      }
    } catch (TransformerException | XMLStreamException e) {
      throw new SQLException("Failed to create " + resultClass.getName() + " for XML value", e);
    }
    throw new SQLException("Unsupported Result type " + resultClass.getName());
  }

  private static String serialize(Source source) throws SQLException {
    try {
      TransformerFactory factory = TransformerFactory.newInstance();
      factory.setFeature(XMLConstants.FEATURE_SECURE_PROCESSING, true);
      Transformer transformer = factory.newTransformer();
      transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
      transformer.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
      StringWriter writer = new StringWriter();
      transformer.transform(source, new StreamResult(writer));
      return writer.toString();
    } catch (TransformerException e) {
      throw new SQLException("Failed to serialize XML value", e);
    }
  }

  @Override
  public String toString() {
    return value;
  }
}
