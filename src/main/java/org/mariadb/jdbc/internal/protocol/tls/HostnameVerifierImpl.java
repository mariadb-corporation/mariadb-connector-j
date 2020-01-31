/*
 *
 * MariaDB Client for Java
 *
 * Copyright (c) 2012-2014 Monty Program Ab.
 * Copyright (c) 2015-2020 MariaDB Corporation Ab.
 *
 * This library is free software; you can redistribute it and/or modify it under
 * the terms of the GNU Lesser General Public License as published by the Free
 * Software Foundation; either version 2.1 of the License, or (at your option)
 * any later version.
 *
 * This library is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 * for more details.
 *
 * You should have received a copy of the GNU Lesser General Public License along
 * with this library; if not, write to Monty Program Ab info@montyprogram.com.
 *
 * This particular MariaDB Client for Java file is work
 * derived from a Drizzle-JDBC. Drizzle-JDBC file which is covered by subject to
 * the following copyright and notice provisions:
 *
 * Copyright (c) 2009-2011, Marcus Eriksson
 *
 * Redistribution and use in source and binary forms, with or without modification,
 * are permitted provided that the following conditions are met:
 * Redistributions of source code must retain the above copyright notice, this list
 * of conditions and the following disclaimer.
 *
 * Redistributions in binary form must reproduce the above copyright notice, this
 * list of conditions and the following disclaimer in the documentation and/or
 * other materials provided with the distribution.
 *
 * Neither the name of the driver nor the names of its contributors may not be
 * used to endorse or promote products derived from this software without specific
 * prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS  AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
 * WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE DISCLAIMED.
 * IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT,
 * INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT
 * NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
 * WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 *
 */

package org.mariadb.jdbc.internal.protocol.tls;

import org.mariadb.jdbc.internal.logging.Logger;
import org.mariadb.jdbc.internal.logging.LoggerFactory;
import org.mariadb.jdbc.internal.util.Utils;

import javax.naming.InvalidNameException;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.security.auth.x500.X500Principal;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.cert.Certificate;
import java.security.cert.CertificateParsingException;
import java.security.cert.X509Certificate;
import java.util.*;

public class HostnameVerifierImpl implements HostnameVerifier {

  private static final Logger logger = LoggerFactory.getLogger(HostnameVerifierImpl.class);

  /**
   * DNS verification : Matching is performed using the matching rules specified by [RFC2459]. If
   * more than one identity of a given type is present in the certificate (e.g., more than one
   * dNSName name, a match in any one of the set is considered acceptable.) Names may contain the
   * wildcard character * which is considered to match any single domain name component or component
   * fragment. E.g., *.a.com matches foo.a.com but not bar.foo.a.com. f*.com matches foo.com but not
   * bar.com.
   *
   * @param hostname hostname
   * @param tlsDnsPattern DNS pattern (may contain wildcard)
   * @return true if matching
   */
  private static boolean matchDns(String hostname, String tlsDnsPattern) throws SSLException {
    boolean hostIsIp = Utils.isIPv4(hostname) || Utils.isIPv6(hostname);
    StringTokenizer hostnameSt = new StringTokenizer(hostname.toLowerCase(Locale.ROOT), ".");
    StringTokenizer templateSt = new StringTokenizer(tlsDnsPattern.toLowerCase(Locale.ROOT), ".");
    if (hostnameSt.countTokens() != templateSt.countTokens()) {
      return false;
    }

    try {
      while (hostnameSt.hasMoreTokens()) {
        if (!matchWildCards(hostIsIp, hostnameSt.nextToken(), templateSt.nextToken())) {
          return false;
        }
      }
    } catch (SSLException exception) {
      throw new SSLException(
          normalizedHostMsg(hostname)
              + " doesn't correspond to certificate CN \""
              + tlsDnsPattern
              + "\" : wildcards not possible for IPs");
    }
    return true;
  }

  private static boolean matchWildCards(boolean hostIsIp, String hostnameToken, String tlsDnsToken)
      throws SSLException {
    int wildcardIndex = tlsDnsToken.indexOf("*");
    String token = hostnameToken;
    if (wildcardIndex != -1) {
      if (hostIsIp) {
        throw new SSLException("WildCards not possible when using IP's");
      }
      boolean first = true;
      String beforeWildcard;
      String afterWildcard = tlsDnsToken;

      while (wildcardIndex != -1) {
        beforeWildcard = afterWildcard.substring(0, wildcardIndex);
        afterWildcard = afterWildcard.substring(wildcardIndex + 1);

        int beforeStartIdx = token.indexOf(beforeWildcard);
        if ((beforeStartIdx == -1) || (first && beforeStartIdx != 0)) {
          return false;
        }

        first = false;

        token = token.substring(beforeStartIdx + beforeWildcard.length());
        wildcardIndex = afterWildcard.indexOf("*");
      }
      return token.endsWith(afterWildcard);
    }

    // no wildcard -> token must be equal.
    return token.equals(tlsDnsToken);
  }

  private static String extractCommonName(String principal) throws SSLException {
    if (principal == null) {
      return null;
    }
    try {
      LdapName ldapName = new LdapName(principal);

      for (Rdn rdn : ldapName.getRdns()) {
        if (rdn.getType().equalsIgnoreCase("CN")) {
          Object obj = rdn.getValue();
          if (obj != null) {
            return obj.toString();
          }
        }
      }
      return null;
    } catch (InvalidNameException e) {
      throw new SSLException("DN value \"" + principal + "\" is invalid");
    }
  }

  private static String normaliseAddress(String hostname) {
    try {
      if (hostname == null) {
        return null;
      }
      InetAddress inetAddress = InetAddress.getByName(hostname);
      return inetAddress.getHostAddress();
    } catch (UnknownHostException unexpected) {
      return hostname;
    }
  }

  private static String normalizedHostMsg(String normalizedHost) {
    StringBuilder msg = new StringBuilder();
    if (Utils.isIPv4(normalizedHost)) {
      msg.append("IPv4 host \"");
    } else if (Utils.isIPv6(normalizedHost)) {
      msg.append("IPv6 host \"");
    } else {
      msg.append("DNS host \"");
    }
    msg.append(normalizedHost).append("\"");
    return msg.toString();
  }

  private SubjectAltNames getSubjectAltNames(X509Certificate cert)
      throws CertificateParsingException {
    Collection<List<?>> entries = cert.getSubjectAlternativeNames();
    SubjectAltNames subjectAltNames = new SubjectAltNames();
    if (entries != null) {
      for (List<?> entry : entries) {
        if (entry.size() >= 2) {
          int type = (Integer) entry.get(0);

          if (type == 2) { // DNS
            String altNameDns = (String) entry.get(1);
            if (altNameDns != null) {
              String normalizedSubjectAlt = altNameDns.toLowerCase(Locale.ROOT);
              subjectAltNames.add(new GeneralName(normalizedSubjectAlt, Extension.DNS));
            }
          }

          if (type == 7) { // IP
            String altNameIp = (String) entry.get(1);
            if (altNameIp != null) {
              subjectAltNames.add(new GeneralName(altNameIp, Extension.IP));
            }
          }
        }
      }
    }
    return subjectAltNames;
  }

  @Override
  public boolean verify(String host, SSLSession session) {
    return verify(host, session, -1);
  }

  /**
   * Verification, like HostnameVerifier.verify() with an additional server thread id to identify
   * connection in logs.
   *
   * @param host host to connect (DNS/IP)
   * @param session SSL session
   * @param serverThreadId connection id to identify connection in logs
   * @return true if valid
   */
  public boolean verify(String host, SSLSession session, long serverThreadId) {
    try {
      Certificate[] certs = session.getPeerCertificates();
      X509Certificate cert = (X509Certificate) certs[0];
      verify(host, cert, serverThreadId);
      return true;
    } catch (SSLException ex) {
      if (logger.isDebugEnabled()) {
        logger.debug(ex.getMessage(), ex);
      }
      return false;
    }
  }

  /**
   * Verification that throw an exception with a detailed error message in case of error.
   *
   * @param host hostname
   * @param cert certificate
   * @param serverThreadId server thread Identifier to identify connection in logs
   * @throws SSLException exception
   */
  public void verify(String host, X509Certificate cert, long serverThreadId) throws SSLException {
    if (host == null) {
      return; // no validation if no host (possible for name pipe)
    }
    String lowerCaseHost = host.toLowerCase(Locale.ROOT);
    try {
      // ***********************************************************
      // RFC 6125 : check Subject Alternative Name (SAN)
      // ***********************************************************
      SubjectAltNames subjectAltNames = getSubjectAltNames(cert);
      if (!subjectAltNames.isEmpty()) {

        // ***********************************************************
        // Host is IPv4 : Check corresponding entries in subject alternative names
        // ***********************************************************
        if (Utils.isIPv4(lowerCaseHost)) {
          for (GeneralName entry : subjectAltNames.getGeneralNames()) {
            if (logger.isTraceEnabled()) {
              logger.trace(
                  "Conn={}. IPv4 verification of hostname : type={} value={} to {}",
                  serverThreadId,
                  entry.extension,
                  entry.value,
                  lowerCaseHost);
            }

            if (entry.extension == Extension.IP && lowerCaseHost.equals(entry.value)) {
              return;
            }
          }
        } else if (Utils.isIPv6(lowerCaseHost)) {
          // ***********************************************************
          // Host is IPv6 : Check corresponding entries in subject alternative names
          // ***********************************************************
          String normalisedHost = normaliseAddress(lowerCaseHost);
          for (GeneralName entry : subjectAltNames.getGeneralNames()) {
            if (logger.isTraceEnabled()) {
              logger.trace(
                  "Conn={}. IPv6 verification of hostname : type={} value={} to {}",
                  serverThreadId,
                  entry.extension,
                  entry.value,
                  lowerCaseHost);
            }

            if (entry.extension == Extension.IP
                && !Utils.isIPv4(entry.value)
                && normalisedHost.equals(normaliseAddress(entry.value))) {
              return;
            }
          }
        } else {
          // ***********************************************************
          // Host is not IP = DNS : Check corresponding entries in alternative subject names
          // ***********************************************************
          for (GeneralName entry : subjectAltNames.getGeneralNames()) {
            if (logger.isTraceEnabled()) {
              logger.trace(
                  "Conn={}. DNS verification of hostname : type={} value={} to {}",
                  serverThreadId,
                  entry.extension,
                  entry.value,
                  lowerCaseHost);
            }

            if (entry.extension == Extension.DNS
                && matchDns(lowerCaseHost, entry.value.toLowerCase(Locale.ROOT))) {
              return;
            }
          }
        }
      }

      // ***********************************************************
      // RFC 2818 : legacy fallback using CN (recommendation is using alt-names)
      // ***********************************************************
      X500Principal subjectPrincipal = cert.getSubjectX500Principal();
      String cn = extractCommonName(subjectPrincipal.getName(X500Principal.RFC2253));

      if (cn == null) {
        if (subjectAltNames.isEmpty()) {
          throw new SSLException(
              "CN not found in certificate principal \"{}"
                  + subjectPrincipal
                  + "\" and certificate doesn't contain SAN");
        } else {
          throw new SSLException(
              "CN not found in certificate principal \""
                  + subjectPrincipal
                  + "\" and "
                  + normalizedHostMsg(lowerCaseHost)
                  + " doesn't correspond to "
                  + subjectAltNames.toString());
        }
      }

      String normalizedCn = cn.toLowerCase(Locale.ROOT);
      if (logger.isTraceEnabled()) {
        logger.trace(
            "Conn={}. DNS verification of hostname : CN={} to {}",
            serverThreadId,
            normalizedCn,
            lowerCaseHost);
      }
      if (!matchDns(lowerCaseHost, normalizedCn)) {
        String errorMsg =
            normalizedHostMsg(lowerCaseHost)
                + " doesn't correspond to certificate CN \""
                + normalizedCn
                + "\"";
        if (!subjectAltNames.isEmpty()) {
          errorMsg += " and " + subjectAltNames.toString();
        }
        throw new SSLException(errorMsg);
      }

    } catch (CertificateParsingException cpe) {
      throw new SSLException("certificate parsing error : " + cpe.getMessage());
    }
  }

  private enum Extension {
    DNS,
    IP
  }

  private class GeneralName {

    private final String value;
    private final Extension extension;

    public GeneralName(String value, Extension extension) {
      this.value = value;
      this.extension = extension;
    }

    @Override
    public String toString() {
      return "{" + extension + ":\"" + value + "\"}";
    }
  }

  private class SubjectAltNames {

    private final List<GeneralName> generalNames = new ArrayList<>();

    @Override
    public String toString() {
      if (isEmpty()) {
        return "SAN[-empty-]";
      }

      StringBuilder sb = new StringBuilder("SAN[");
      boolean first = true;

      for (GeneralName generalName : generalNames) {
        if (!first) {
          sb.append(",");
        }
        first = false;
        sb.append(generalName.toString());
      }
      sb.append("]");
      return sb.toString();
    }

    public List<GeneralName> getGeneralNames() {
      return generalNames;
    }

    public void add(GeneralName generalName) {
      generalNames.add(generalName);
    }

    public boolean isEmpty() {
      return generalNames.isEmpty();
    }
  }
}
