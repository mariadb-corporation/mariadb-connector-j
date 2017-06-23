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
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.StringTokenizer;

public class HostnameVerifierImpl implements HostnameVerifier {

    private static Logger logger = LoggerFactory.getLogger(HostnameVerifierImpl.class);

    /**
     * DNS verification :
     * Matching is performed using the matching rules specified by
     * [RFC2459].  If more than one identity of a given type is present in
     * the certificate (e.g., more than one dNSName name, a match in any one
     * of the set is considered acceptable.) Names may contain the wildcard
     * character * which is considered to match any single domain name
     * component or component fragment. E.g., *.a.com matches foo.a.com but
     * not bar.foo.a.com. f*.com matches foo.com but not bar.com.
     *
     * @param hostname      hostname
     * @param tlsDnsPattern DNS pattern (may contain wildcard)
     * @return true if matching
     */
    private static boolean matchDns(String hostname, String tlsDnsPattern) {
        boolean hostIsIp = Utils.isIPv4(hostname) || Utils.isIPv6(hostname);
        StringTokenizer hostnameSt = new StringTokenizer(hostname.toLowerCase(), ".");
        StringTokenizer templateSt = new StringTokenizer(tlsDnsPattern.toLowerCase(), ".");
        if (hostnameSt.countTokens() != templateSt.countTokens()) return false;

        while (hostnameSt.hasMoreTokens()) {
            if (!matchWildCards(hostIsIp, hostnameSt.nextToken(), templateSt.nextToken())) return false;
        }
        return true;
    }

    private static boolean matchWildCards(boolean hostIsIp, String hostnameToken, String tlsDnsToken) {
        int wildcardIndex = tlsDnsToken.indexOf("*");
        if (wildcardIndex != -1) {
            if (hostIsIp) return false;
            boolean first = true;
            String beforeWildcard;
            String afterWildcard = tlsDnsToken;

            while (wildcardIndex != -1) {
                beforeWildcard = afterWildcard.substring(0, wildcardIndex);
                afterWildcard = afterWildcard.substring(wildcardIndex + 1);

                int beforeStartIdx = hostnameToken.indexOf(beforeWildcard);
                if ((beforeStartIdx == -1) || (first && beforeStartIdx != 0)) return false;

                first = false;

                hostnameToken = hostnameToken.substring(beforeStartIdx + beforeWildcard.length());
                wildcardIndex = afterWildcard.indexOf("*");
            }
            return hostnameToken.endsWith(afterWildcard);
        }

        //no wildcard -> token must be equal.
        return hostnameToken.equals(tlsDnsToken);

    }

    static String extractCommonName(String principal) throws SSLException {
        if (principal == null) return null;
        try {
            LdapName ldapName = new LdapName(principal);

            for (Rdn rdn : ldapName.getRdns()) {
                if (rdn.getType().equalsIgnoreCase("CN")) {
                    Object obj = rdn.getValue();
                    if (obj != null) return obj.toString();
                }
            }
            return null;
        } catch (InvalidNameException e) {
            throw new SSLException("DN value \"" + principal + "\" is invalid");
        }
    }

    private static String normaliseAddress(String hostname) {
        try {
            if (hostname == null) return hostname;
            InetAddress inetAddress = InetAddress.getByName(hostname);
            return inetAddress.getHostAddress();
        } catch (UnknownHostException unexpected) {
            return hostname;
        }
    }

    @Override
    public boolean verify(String host, SSLSession session) {
        try {
            Certificate[] certs = session.getPeerCertificates();
            X509Certificate cert = (X509Certificate) certs[0];
            return verify(host, cert);
        } catch (SSLException ex) {
            if (logger.isDebugEnabled()) logger.debug(ex.getMessage(), ex);
            return false;
        }
    }

    /**
     * Export very using certificat for testing purpose.
     *
     * @param host  hostname
     * @param cert  certificate
     * @return true if hostname correspond to certificate
     * @throws SSLException exception
     */
    public boolean verify(String host, X509Certificate cert) throws SSLException {
        try {
            Collection<List<?>> entries = cert.getSubjectAlternativeNames();
            if (entries != null && !entries.isEmpty()) {

                //***********************************************************
                // Host is IPv4 : Check corresponding entries in alternative subject names
                //***********************************************************
                if (Utils.isIPv4(host)) {
                    for (List<?> entry : entries) {
                        if (entry.size() >= 2) {
                            int type = (Integer) entry.get(0);
                            if (type == 7) { //IP
                                String altNameIp = (String) entry.get(1);
                                if (host.equals(altNameIp)) return true;
                            }
                        }
                    }
                    return false;

                }

                //***********************************************************
                // Host is IPv6 : Check corresponding entries in alternative subject names
                //***********************************************************
                if (Utils.isIPv6(host)) {
                    String normalisedHost = normaliseAddress(host);
                    for (List<?> entry : entries) {
                        if (entry.size() >= 2) {
                            int type = (Integer) entry.get(0);
                            if (type == 7) { //IP
                                String altNameIp = (String) entry.get(1);
                                if (!Utils.isIPv4(altNameIp)) {
                                    String normalizedSubjectAlt = normaliseAddress(altNameIp);
                                    if (normalisedHost.equals(normalizedSubjectAlt)) {
                                        return true;
                                    }
                                }
                            }
                        }
                    }
                    return false;
                }

                //***********************************************************
                // Host is not IP = DNS : Check corresponding entries in alternative subject names
                //***********************************************************
                String normalizedHost = host.toLowerCase(Locale.ROOT);
                for (List<?> entry : entries) {
                    if (entry.size() >= 2) {
                        int type = (Integer) entry.get(0);
                        if (type == 2) { //DNS
                            String normalizedSubjectAlt = ((String) entry.get(1)).toLowerCase(Locale.ROOT);
                            if (matchDns(normalizedHost, normalizedSubjectAlt)) {
                                return true;
                            }
                        }
                    }
                }
                return false;
            }
        } catch (CertificateParsingException cpe) {
            // ignore error
        }

        //***********************************************************
        // no alternative subject names, check using CN
        //***********************************************************
        X500Principal subjectPrincipal = cert.getSubjectX500Principal();
        String cn = extractCommonName(subjectPrincipal.getName(X500Principal.RFC2253));
        if (cn == null) return false;
        String normalizedHost = host.toLowerCase(Locale.ROOT);
        String normalizedCn = cn.toLowerCase(Locale.ROOT);
        if (!matchDns(normalizedHost, normalizedCn)) return false;
        return true;

    }
}
