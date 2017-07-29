package org.mariadb.jdbc.internal.protocol.tls;


import org.junit.Test;

import javax.net.ssl.SSLException;
import javax.security.auth.x500.X500Principal;

import java.io.ByteArrayInputStream;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;

import static org.junit.Assert.*;

public class HostnameVerifierImplTest {
    HostnameVerifierImpl verifier = new HostnameVerifierImpl();

    private static X509Certificate getCertificate(String certString) throws CertificateException {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        return (X509Certificate) cf.generateCertificate(new ByteArrayInputStream(certString.getBytes()));
    }

    private void verifyExceptionEqual(String host, X509Certificate cert, String exceptionMessage) {
        try {
            verifier.verify(host, cert);
            fail("must have failed");
        } catch (SSLException exception) {
            assertEquals(exceptionMessage, exception.getMessage());
        }
    }

    //generating certificate example
    //      openssl genrsa -out "/home/osboxes/key" 2048
    //      openssl req -new -utf8 -sha1 \
    //            -key /home/osboxes/key \
    //            -subj "/C=US/ST=CA/O=Acme, Inc./CN=mariadb.org" \
    //            -reqexts SAN \
    //            -config <(cat /etc/ssl/openssl.cnf \
    //                    <(printf "\n[SAN]\nsubjectAltName=DNS:mariadbtest.org,DNS:www.mariadbtest.org")) \
    //            -out domain.csr


    @Test public void verifyCn() throws Exception {
        // CN=test.com
        X509Certificate cert = getCertificate(""
                + "-----BEGIN CERTIFICATE-----\n"
                + "MIIC+zCCAeOgAwIBAgIJANin/585wAXHMA0GCSqGSIb3DQEBBQUAMBMxETAPBgNV\n"
                + "BAMMCHRlc3QuY29tMCAXDTE3MDYyMzEzNTI1NloYDzIxMTcwNTMwMTM1MjU2WjAT\n"
                + "MREwDwYDVQQDDAh0ZXN0LmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC\n"
                + "ggEBAMQHS31pL/fMkcQBo5zZV2Hi1Jdc1vIIv65d+XeFmv+n/vv+X5Z5NKLc7i01\n"
                + "SFPoTEr1HG7R6Xvl27UtGm9Z6fmgsCbCRImUCG1yPER20fAgWNhKkoGgvOM8PxZz\n"
                + "AS0dWgaukBnG9EDVQQrLu+bHuHji8qysGiYQGvSBy/QLSMWjfkSjyFv8I2pT0jLi\n"
                + "eghgROl3IprRcsiebC/Bv1iJ06s8BD1C9ErzmPxqHaChdzGFATm+G4opcnBxzPuN\n"
                + "DVE9CaLUS4Q5SixB9TRTQ2LyryEtUOUnnDyoktrX3LzkTmr2dhT8MIgRMsNkJD5w\n"
                + "CpITvLchBXCdj0lcn5NMb0Rt/AsCAwEAAaNQME4wHQYDVR0OBBYEFJMoFo+HhyIt\n"
                + "WA6QZmedeN2/qBU/MB8GA1UdIwQYMBaAFJMoFo+HhyItWA6QZmedeN2/qBU/MAwG\n"
                + "A1UdEwQFMAMBAf8wDQYJKoZIhvcNAQEFBQADggEBAGpBkEJ3nv1kCnqQbrU3WYmv\n"
                + "zl+kNc5xVZdLWMarjvWwBE1cnDHkXzJpl5cyWcHuPyWAi40/edN7gRfpNINWfc0A\n"
                + "9YWo2PVBlBdklqzsewDV3gipFHkCgBTlGXaPXjCLLNxphYwdsble1hu/XXNvNY8v\n"
                + "9EPxgN0rTnBj85gme/+Hrjq2kH15jyqK5rdylOjCZELs5g8cc49M6sw/sY70GsGA\n"
                + "UEjb+vAN7FxXzXzH4oqIeycnxP+/MA82iieew7nlOMlYrppM6igjP0CUzw4ys6lG\n"
                + "8QdWBcm2Ybo4XFjOnC98VlQl+WBu4CiToxjGphDmsMIO3Hf5PSTRwTKxtuWn45Y=\n"
                + "-----END CERTIFICATE-----\n");
        verifier.verify("test.com", cert);
        verifyExceptionEqual("a.test.com", cert, "host \"a.test.com\" doesn't correspond to "
                + "certificate CN \"test.com\"");
        verifyExceptionEqual("other.com", cert, "host \"other.com\" doesn't correspond to "
                + "certificate CN \"test.com\"");
    }

    @Test public void verifyNonAsciiCn() throws Exception {
        // CN=😎.com = "\uD83D\uDE0E"
        X509Certificate cert = getCertificate(""
                + "-----BEGIN CERTIFICATE-----\n"
                + "MIIDWTCCAkGgAwIBAgIJAI38v686DwcOMA0GCSqGSIb3DQEBBQUAMEIxCzAJBgNV\n"
                + "BAYTAlVTMQswCQYDVQQIDAJDQTETMBEGA1UECgwKQWNtZSwgSW5jLjERMA8GA1UE\n"
                + "AwwI8J+Yji5jb20wIBcNMTcwNjIzMTQyNzQ2WhgPMjExNzA1MzAxNDI3NDZaMEIx\n"
                + "CzAJBgNVBAYTAlVTMQswCQYDVQQIDAJDQTETMBEGA1UECgwKQWNtZSwgSW5jLjER\n"
                + "MA8GA1UEAwwI8J+Yji5jb20wggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIB\n"
                + "AQDEB0t9aS/3zJHEAaOc2Vdh4tSXXNbyCL+uXfl3hZr/p/77/l+WeTSi3O4tNUhT\n"
                + "6ExK9Rxu0el75du1LRpvWen5oLAmwkSJlAhtcjxEdtHwIFjYSpKBoLzjPD8WcwEt\n"
                + "HVoGrpAZxvRA1UEKy7vmx7h44vKsrBomEBr0gcv0C0jFo35Eo8hb/CNqU9Iy4noI\n"
                + "YETpdyKa0XLInmwvwb9YidOrPAQ9QvRK85j8ah2goXcxhQE5vhuKKXJwccz7jQ1R\n"
                + "PQmi1EuEOUosQfU0U0Ni8q8hLVDlJ5w8qJLa19y85E5q9nYU/DCIETLDZCQ+cAqS\n"
                + "E7y3IQVwnY9JXJ+TTG9EbfwLAgMBAAGjUDBOMB0GA1UdDgQWBBSTKBaPh4ciLVgO\n"
                + "kGZnnXjdv6gVPzAfBgNVHSMEGDAWgBSTKBaPh4ciLVgOkGZnnXjdv6gVPzAMBgNV\n"
                + "HRMEBTADAQH/MA0GCSqGSIb3DQEBBQUAA4IBAQBknxZ3ihZHmcyrV3H0pNdc+jxB\n"
                + "xU0T1T1nOLVpgqh+N0m+WFyGdcZlwPcmuD2a5jFn7uIdh8qfq11T8R+OmJqrBxoo\n"
                + "RSACSAgRSQjPfnN8wi4i8hFLKXOQw43UomsSuNixdGIsMWrDh02e1Q0/g/TD7S62\n"
                + "JCRksTlBS/+qw+w384vEd4djq7HuT8/hs2RC6Hm9EQkipgNn9+2z40vJ/jgcuPIF\n"
                + "x51XCozrD1yW9JK/YyBnjYk04iEfQLW7+pGMJOcsX7x9EGwpEg1gsDg2mM0EEIwU\n"
                + "d6DHlYvpD9JkzyEScg8Supztoc2aGbGE4SHBKB1riTLBAHWqqwas4sGSgZxu\n"
                + "-----END CERTIFICATE-----\n");
        verifier.verify("\uD83D\uDE0E.com", cert);
        verifyExceptionEqual("a.\uD83D\uDE0E.com", cert, "host \"a.\uD83D\uDE0E.com\" doesn't "
                + "correspond to certificate CN \"\uD83D\uDE0E.com\"");

    }

    @Test public void verifySubjectAlt() throws Exception {
        // CN=mariadb.org, subjectAlt=other.org,www.other.org

//        openssl genrsa -out ca.key 2048
//        openssl req -new -x509 -days 36500 -key ca.key -subj "/C=CN/ST=GD/L=SZ/O=Acme, Inc./CN=Acme Root CA" -out ca.crt
//
//        openssl req -newkey rsa:2048 -nodes -keyout server.key -subj "/C=CN/ST=GD/L=SZ/O=Acme, Inc./CN=*.mariadb.org" \
//                  -out server.csr
//        openssl x509 -req -extfile <(printf "subjectAltName=DNS:other.org,DNS:www.other.org") -days 36500 \
//                  -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt
        X509Certificate cert = getCertificate(""
                + "-----BEGIN CERTIFICATE-----\n"
                + "MIIDUzCCAjugAwIBAgIJAPMG38xrY9DcMA0GCSqGSIb3DQEBCwUAMFMxCzAJBgNV\n"
                + "BAYTAkNOMQswCQYDVQQIDAJHRDELMAkGA1UEBwwCU1oxEzARBgNVBAoMCkFjbWUs\n"
                + "IEluYy4xFTATBgNVBAMMDEFjbWUgUm9vdCBDQTAgFw0xNzA2MjMxNjEyNTlaGA8y\n"
                + "MTE3MDUzMDE2MTI1OVowVDELMAkGA1UEBhMCQ04xCzAJBgNVBAgMAkdEMQswCQYD\n"
                + "VQQHDAJTWjETMBEGA1UECgwKQWNtZSwgSW5jLjEWMBQGA1UEAwwNKi5tYXJpYWRi\n"
                + "Lm9yZzCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBANZnnLoS2JKQNr/d\n"
                + "ccRT1pNVKHykAVJHhZiIfjqqKEstjob30VZjll5exQ6iTHaS73qXG1/PfXhEl8Tc\n"
                + "7R3VlE7dHxPE+FhWSCcdsJlSEpa9h0TOkJ6H4V1iD+bTwlfEesLqXCzLkaBz7hsw\n"
                + "L6mzVDJ4Lucrstf2YgBEoXjzj8G+ECiz1Vx1GX1jU2yoRYk/LcGUgsbxMxZOFcKG\n"
                + "JyCmjbRe7xJozhu3m/1bf8eCyHg/+Tpiw1VGwPNFe6mb2SI9pYnk9l0pjzFy5yxJ\n"
                + "nFeYj5cLBZHwM5K2aHiOvvBeXvYz9RIrWI98zsXZFjzD00+Js3X/jC4nRtkHV/JC\n"
                + "COvAwRUCAwEAAaMnMCUwIwYDVR0RBBwwGoIJb3RoZXIub3Jngg13d3cub3RoZXIu\n"
                + "b3JnMA0GCSqGSIb3DQEBCwUAA4IBAQDEswEEw2VXv6+EKIz3ysN7kRNXs09TWiYd\n"
                + "bhGIVWHK4oRMjbUkPQWNftD+VvRyW1mZPZ0Tn/kXPnUsYCuF/UFLautmIAa16/el\n"
                + "WJc9EA4yM42CByW/DEUfvFVgaBoJysLNhA2O/1VC/UmC2TNjiwXAO3AOJTVgdS1/\n"
                + "nj34C3SJgbtmMu/ToCILMcjkaKJPD2/1AaIioBOSxvwdseM399eVjZIhf9bQCSHU\n"
                + "fDrV4El/nb5nr4j7AvHtIhbPtpJOmKCAbZRwKc+ZgrH6ZyapyZfpmNmlwZcuC4DM\n"
                + "SJmVrJfl1GdaXyxsljClcXM9MDQYm9r9wcchc3dSVR+k6wz2+vbw\n"
                + "-----END CERTIFICATE-----\n");


        verifyExceptionEqual("mariadb.org", cert, "DNS host \"mariadb.org\" not found in "
                + "certificate alt-names certificate SubjectAltNames[{\"other.org\"|DNS},{\"www.other.org\"|DNS}]");
        verifyExceptionEqual("a.mariadb.org", cert, "DNS host \"a.mariadb.org\" not found in "
                + "certificate alt-names certificate SubjectAltNames[{\"other.org\"|DNS},{\"www.other.org\"|DNS}]");
        verifier.verify("other.org", cert);
        verifyExceptionEqual("a.other.org", cert, "DNS host \"a.other.org\" not found in "
                + "certificate alt-names certificate SubjectAltNames[{\"other.org\"|DNS},{\"www.other.org\"|DNS}]");
        verifier.verify("www.other.org", cert);
    }

    @Test public void verifySubjectAltOnly() throws Exception {
        // subjectAlt=foo.com
        X509Certificate cert = getCertificate(""
                + "-----BEGIN CERTIFICATE-----\n"
                + "MIIESjCCAzKgAwIBAgIJAIz+EYMBU6aYMA0GCSqGSIb3DQEBBQUAMIGiMQswCQYD\n"
                + "VQQGEwJDQTELMAkGA1UECBMCQkMxEjAQBgNVBAcTCVZhbmNvdXZlcjEWMBQGA1UE\n"
                + "ChMNd3d3LmN1Y2JjLmNvbTEUMBIGA1UECxQLY29tbW9uc19zc2wxHTAbBgNVBAMU\n"
                + "FGRlbW9faW50ZXJtZWRpYXRlX2NhMSUwIwYJKoZIhvcNAQkBFhZqdWxpdXNkYXZp\n"
                + "ZXNAZ21haWwuY29tMB4XDTA2MTIxMTE2MjYxMFoXDTI4MTEwNTE2MjYxMFowgZIx\n"
                + "CzAJBgNVBAYTAlVTMREwDwYDVQQIDAhNYXJ5bGFuZDEUMBIGA1UEBwwLRm9yZXN0\n"
                + "IEhpbGwxFzAVBgNVBAoMDmh0dHBjb21wb25lbnRzMRowGAYDVQQLDBF0ZXN0IGNl\n"
                + "cnRpZmljYXRlczElMCMGCSqGSIb3DQEJARYWanVsaXVzZGF2aWVzQGdtYWlsLmNv\n"
                + "bTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAMhjr5aCPoyp0R1iroWA\n"
                + "fnEyBMGYWoCidH96yGPFjYLowez5aYKY1IOKTY2BlYho4O84X244QrZTRl8kQbYt\n"
                + "xnGh4gSCD+Z8gjZ/gMvLUlhqOb+WXPAUHMB39GRyzerA/ZtrlUqf+lKo0uWcocxe\n"
                + "Rc771KN8cPH3nHZ0rV0Hx4ZAZy6U4xxObe4rtSVY07hNKXAb2odnVqgzcYiDkLV8\n"
                + "ilvEmoNWMWrp8UBqkTcpEhYhCYp3cTkgJwMSuqv8BqnGd87xQU3FVZI4tbtkB+Kz\n"
                + "jD9zz8QCDJAfDjZHR03KNQ5mxOgXwxwKw6lGMaiVJTxpTKqym93whYk93l3ocEe5\n"
                + "5c0CAwEAAaOBkDCBjTAJBgNVHRMEAjAAMCwGCWCGSAGG+EIBDQQfFh1PcGVuU1NM\n"
                + "IEdlbmVyYXRlZCBDZXJ0aWZpY2F0ZTAdBgNVHQ4EFgQUnxR3vz86tso4gkJIFiza\n"
                + "0Mteh9gwHwYDVR0jBBgwFoAUe5raj5CZTlLSrNuzA1LKh6YNPg0wEgYDVR0RBAsw\n"
                + "CYIHZm9vLmNvbTANBgkqhkiG9w0BAQUFAAOCAQEAjl78oMjzFdsMy6F1sGg/IkO8\n"
                + "tF5yUgPgFYrs41yzAca7IQu6G9qtFDJz/7ehh/9HoG+oqCCIHPuIOmS7Sd0wnkyJ\n"
                + "Y7Y04jVXIb3a6f6AgBkEFP1nOT0z6kjT7vkA5LJ2y3MiDcXuRNMSta5PYVnrX8aZ\n"
                + "yiqVUNi40peuZ2R8mAUSBvWgD7z2qWhF8YgDb7wWaFjg53I36vWKn90ZEti3wNCw\n"
                + "qAVqixM+J0qJmQStgAc53i2aTMvAQu3A3snvH/PHTBo+5UL72n9S1kZyNCsVf1Qo\n"
                + "n8jKTiRriEM+fMFlcgQP284EBFzYHyCXFb9O/hMjK2+6mY9euMB1U1aFFzM/Bg==\n"
                + "-----END CERTIFICATE-----\n");
        verifier.verify("foo.com", cert);
        verifyExceptionEqual("a.foo.com", cert, "DNS host \"a.foo.com\" not found in certificate "
                + "alt-names certificate SubjectAltNames[{\"foo.com\"|DNS}]");
    }

    @Test public void verifyMultipleCn() throws Exception {
        // CN=test1.org, CN=test2.org
        X509Certificate cert = getCertificate(""
                + "-----BEGIN CERTIFICATE-----\n"
                + "MIIDgzCCAmugAwIBAgIJAPqfD+J8D4gqMA0GCSqGSIb3DQEBBQUAMFcxCzAJBgNV\n"
                + "BAYTAlVTMQswCQYDVQQIDAJDQTETMBEGA1UECgwKQWNtZSwgSW5jLjESMBAGA1UE\n"
                + "AwwJdGVzdDEub3JnMRIwEAYDVQQDDAl0ZXN0Mi5vcmcwIBcNMTcwNjIzMTYxNDIx\n"
                + "WhgPMjExNzA1MzAxNjE0MjFaMFcxCzAJBgNVBAYTAlVTMQswCQYDVQQIDAJDQTET\n"
                + "MBEGA1UECgwKQWNtZSwgSW5jLjESMBAGA1UEAwwJdGVzdDEub3JnMRIwEAYDVQQD\n"
                + "DAl0ZXN0Mi5vcmcwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDEB0t9\n"
                + "aS/3zJHEAaOc2Vdh4tSXXNbyCL+uXfl3hZr/p/77/l+WeTSi3O4tNUhT6ExK9Rxu\n"
                + "0el75du1LRpvWen5oLAmwkSJlAhtcjxEdtHwIFjYSpKBoLzjPD8WcwEtHVoGrpAZ\n"
                + "xvRA1UEKy7vmx7h44vKsrBomEBr0gcv0C0jFo35Eo8hb/CNqU9Iy4noIYETpdyKa\n"
                + "0XLInmwvwb9YidOrPAQ9QvRK85j8ah2goXcxhQE5vhuKKXJwccz7jQ1RPQmi1EuE\n"
                + "OUosQfU0U0Ni8q8hLVDlJ5w8qJLa19y85E5q9nYU/DCIETLDZCQ+cAqSE7y3IQVw\n"
                + "nY9JXJ+TTG9EbfwLAgMBAAGjUDBOMB0GA1UdDgQWBBSTKBaPh4ciLVgOkGZnnXjd\n"
                + "v6gVPzAfBgNVHSMEGDAWgBSTKBaPh4ciLVgOkGZnnXjdv6gVPzAMBgNVHRMEBTAD\n"
                + "AQH/MA0GCSqGSIb3DQEBBQUAA4IBAQANlc974MeEIjEG8PzjDuiCbImZU/vxmBu1\n"
                + "QD4mOfTjoixx/o9w/TbtnYhlugH3Nb2biaIx+2VnQAjk6euNBdFXW1cIawstrYGn\n"
                + "KKEbZgQ7rgWfqyXIUK5NgX5jqxv5iW2xQE9nFGum8ouy8t+Nwi5F5uPGlhw/POnZ\n"
                + "SLdP5i67GJN/Ho2HCfYOWm8STo0S7jmxtGoLcZ/EPaM3DaqLQYTdjtNKuotw1YuF\n"
                + "A94gKVaU6XS6EdDGc6oSfKAR/pqKnWAmDc0ofvYniojquzm4fUO3JgzXN/xTDPUc\n"
                + "GiY3dV92GD9wZfbUWsQRzLizRzIrsvZfCn/LLeUvOQPuCCeLzIxD\n"
                + "-----END CERTIFICATE-----\n");
        verifier.verify("test1.org", cert);
        verifyExceptionEqual("test2.org", cert, "host \"test2.org\" doesn't correspond to "
                + "certificate CN \"test1.org\"");
    }

    @Test public void verifyWilcardCn() throws Exception {
        // CN=*.foo.com
        X509Certificate cert = getCertificate(""
                + "-----BEGIN CERTIFICATE-----\n"
                + "MIIESDCCAzCgAwIBAgIJAIz+EYMBU6aUMA0GCSqGSIb3DQEBBQUAMIGiMQswCQYD\n"
                + "VQQGEwJDQTELMAkGA1UECBMCQkMxEjAQBgNVBAcTCVZhbmNvdXZlcjEWMBQGA1UE\n"
                + "ChMNd3d3LmN1Y2JjLmNvbTEUMBIGA1UECxQLY29tbW9uc19zc2wxHTAbBgNVBAMU\n"
                + "FGRlbW9faW50ZXJtZWRpYXRlX2NhMSUwIwYJKoZIhvcNAQkBFhZqdWxpdXNkYXZp\n"
                + "ZXNAZ21haWwuY29tMB4XDTA2MTIxMTE2MTU1NVoXDTI4MTEwNTE2MTU1NVowgaYx\n"
                + "CzAJBgNVBAYTAlVTMREwDwYDVQQIEwhNYXJ5bGFuZDEUMBIGA1UEBxMLRm9yZXN0\n"
                + "IEhpbGwxFzAVBgNVBAoTDmh0dHBjb21wb25lbnRzMRowGAYDVQQLExF0ZXN0IGNl\n"
                + "cnRpZmljYXRlczESMBAGA1UEAxQJKi5mb28uY29tMSUwIwYJKoZIhvcNAQkBFhZq\n"
                + "dWxpdXNkYXZpZXNAZ21haWwuY29tMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIB\n"
                + "CgKCAQEAyGOvloI+jKnRHWKuhYB+cTIEwZhagKJ0f3rIY8WNgujB7PlpgpjUg4pN\n"
                + "jYGViGjg7zhfbjhCtlNGXyRBti3GcaHiBIIP5nyCNn+Ay8tSWGo5v5Zc8BQcwHf0\n"
                + "ZHLN6sD9m2uVSp/6UqjS5ZyhzF5FzvvUo3xw8fecdnStXQfHhkBnLpTjHE5t7iu1\n"
                + "JVjTuE0pcBvah2dWqDNxiIOQtXyKW8Sag1YxaunxQGqRNykSFiEJindxOSAnAxK6\n"
                + "q/wGqcZ3zvFBTcVVkji1u2QH4rOMP3PPxAIMkB8ONkdHTco1DmbE6BfDHArDqUYx\n"
                + "qJUlPGlMqrKb3fCFiT3eXehwR7nlzQIDAQABo3sweTAJBgNVHRMEAjAAMCwGCWCG\n"
                + "SAGG+EIBDQQfFh1PcGVuU1NMIEdlbmVyYXRlZCBDZXJ0aWZpY2F0ZTAdBgNVHQ4E\n"
                + "FgQUnxR3vz86tso4gkJIFiza0Mteh9gwHwYDVR0jBBgwFoAUe5raj5CZTlLSrNuz\n"
                + "A1LKh6YNPg0wDQYJKoZIhvcNAQEFBQADggEBAH0ipG6J561UKUfgkeW7GvYwW98B\n"
                + "N1ZooWX+JEEZK7+Pf/96d3Ij0rw9ACfN4bpfnCq0VUNZVSYB+GthQ2zYuz7tf/UY\n"
                + "A6nxVgR/IjG69BmsBl92uFO7JTNtHztuiPqBn59pt+vNx4yPvno7zmxsfI7jv0ww\n"
                + "yfs+0FNm7FwdsC1k47GBSOaGw38kuIVWqXSAbL4EX9GkryGGOKGNh0qvAENCdRSB\n"
                + "G9Z6tyMbmfRY+dLSh3a9JwoEcBUso6EWYBakLbq4nG/nvYdYvG9ehrnLVwZFL82e\n"
                + "l3Q/RK95bnA6cuRClGusLad0e6bjkBzx/VQ3VarDEpAkTLUGVAa0CLXtnyc=\n"
                + "-----END CERTIFICATE-----\n");
        verifyExceptionEqual("foo.com", cert, "host \"foo.com\" doesn't correspond to certificate "
                + "CN \"*.foo.com\"");
        verifier.verify("www.foo.com", cert);
        verifier.verify("\u82b1\u5b50.foo.com", cert);
        verifyExceptionEqual("a.b.foo.com", cert, "host \"a.b.foo.com\" doesn't correspond to "
                + "certificate CN \"*.foo.com\"");
    }

    @Test public void verifyWilcardCnOnTld() throws Exception {
        // It's the CA's responsibility to not issue broad-matching certificates!
        // CN=*.co.jp
        X509Certificate cert = getCertificate(""
                + "-----BEGIN CERTIFICATE-----\n"
                + "MIIERjCCAy6gAwIBAgIJAIz+EYMBU6aVMA0GCSqGSIb3DQEBBQUAMIGiMQswCQYD\n"
                + "VQQGEwJDQTELMAkGA1UECBMCQkMxEjAQBgNVBAcTCVZhbmNvdXZlcjEWMBQGA1UE\n"
                + "ChMNd3d3LmN1Y2JjLmNvbTEUMBIGA1UECxQLY29tbW9uc19zc2wxHTAbBgNVBAMU\n"
                + "FGRlbW9faW50ZXJtZWRpYXRlX2NhMSUwIwYJKoZIhvcNAQkBFhZqdWxpdXNkYXZp\n"
                + "ZXNAZ21haWwuY29tMB4XDTA2MTIxMTE2MTYzMFoXDTI4MTEwNTE2MTYzMFowgaQx\n"
                + "CzAJBgNVBAYTAlVTMREwDwYDVQQIEwhNYXJ5bGFuZDEUMBIGA1UEBxMLRm9yZXN0\n"
                + "IEhpbGwxFzAVBgNVBAoTDmh0dHBjb21wb25lbnRzMRowGAYDVQQLExF0ZXN0IGNl\n"
                + "cnRpZmljYXRlczEQMA4GA1UEAxQHKi5jby5qcDElMCMGCSqGSIb3DQEJARYWanVs\n"
                + "aXVzZGF2aWVzQGdtYWlsLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC\n"
                + "ggEBAMhjr5aCPoyp0R1iroWAfnEyBMGYWoCidH96yGPFjYLowez5aYKY1IOKTY2B\n"
                + "lYho4O84X244QrZTRl8kQbYtxnGh4gSCD+Z8gjZ/gMvLUlhqOb+WXPAUHMB39GRy\n"
                + "zerA/ZtrlUqf+lKo0uWcocxeRc771KN8cPH3nHZ0rV0Hx4ZAZy6U4xxObe4rtSVY\n"
                + "07hNKXAb2odnVqgzcYiDkLV8ilvEmoNWMWrp8UBqkTcpEhYhCYp3cTkgJwMSuqv8\n"
                + "BqnGd87xQU3FVZI4tbtkB+KzjD9zz8QCDJAfDjZHR03KNQ5mxOgXwxwKw6lGMaiV\n"
                + "JTxpTKqym93whYk93l3ocEe55c0CAwEAAaN7MHkwCQYDVR0TBAIwADAsBglghkgB\n"
                + "hvhCAQ0EHxYdT3BlblNTTCBHZW5lcmF0ZWQgQ2VydGlmaWNhdGUwHQYDVR0OBBYE\n"
                + "FJ8Ud78/OrbKOIJCSBYs2tDLXofYMB8GA1UdIwQYMBaAFHua2o+QmU5S0qzbswNS\n"
                + "yoemDT4NMA0GCSqGSIb3DQEBBQUAA4IBAQA0sWglVlMx2zNGvUqFC73XtREwii53\n"
                + "CfMM6mtf2+f3k/d8KXhLNySrg8RRlN11zgmpPaLtbdTLrmG4UdAHHYr8O4y2BBmE\n"
                + "1cxNfGxxechgF8HX10QV4dkyzp6Z1cfwvCeMrT5G/V1pejago0ayXx+GPLbWlNeZ\n"
                + "S+Kl0m3p+QplXujtwG5fYcIpaGpiYraBLx3Tadih39QN65CnAh/zRDhLCUzKyt9l\n"
                + "UGPLEUDzRHMPHLnSqT1n5UU5UDRytbjJPXzF+l/+WZIsanefWLsxnkgAuZe/oMMF\n"
                + "EJMryEzOjg4Tfuc5qM0EXoPcQ/JlheaxZ40p2IyHqbsWV4MRYuFH4bkM\n"
                + "-----END CERTIFICATE-----\n");
        verifier.verify("foo.co.jp", cert);
        verifier.verify("\u82b1\u5b50.co.jp", cert);
    }

    @Test public void subjectAltUsesLocalDomainAndIp() throws Exception {
        // /C=CN/ST=GD/L=SZ/O=Acme, Inc./CN=*.mariadb.org, subjectAltName=DNS:localhost.localdomain,DNS:localhost,IP:127.0.0.1
        X509Certificate cert = getCertificate(""
                + "-----BEGIN CERTIFICATE-----\n"
                + "MIIDYTCCAkmgAwIBAgIJAPMG38xrY9DZMA0GCSqGSIb3DQEBCwUAMFMxCzAJBgNV\n"
                + "BAYTAkNOMQswCQYDVQQIDAJHRDELMAkGA1UEBwwCU1oxEzARBgNVBAoMCkFjbWUs\n"
                + "IEluYy4xFTATBgNVBAMMDEFjbWUgUm9vdCBDQTAgFw0xNzA2MjMxNTU0MjBaGA8y\n"
                + "MTE3MDUzMDE1NTQyMFowVDELMAkGA1UEBhMCQ04xCzAJBgNVBAgMAkdEMQswCQYD\n"
                + "VQQHDAJTWjETMBEGA1UECgwKQWNtZSwgSW5jLjEWMBQGA1UEAwwNKi5tYXJpYWRi\n"
                + "Lm9yZzCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBALuoVA18cp1Xe/HD\n"
                + "D+qxe4onCn6w2WJMVo5H7y5Lc9XmNpAbI2n1GaDuTPHJ0m8ykShvO5ACZTxQkgtD\n"
                + "8n6OLIi1TORBRIMTsIuhOKEMQc7WxAW0Z1ZeaiVTXHPfiG0cHFWPXPDIMgrvCGtM\n"
                + "Pt7UuxsP1XAgT0nKwzQk2eVM/JWgvvdcv3mYFGKLYXsW3NneqQsv1IcVbBEI9e44\n"
                + "joVPSo+Qgx9u/7dOKruDJPd6Pecsoa4OTzorrn3iwycdJAoopN0fb7Q1op1Fr64s\n"
                + "RwR9urGZpdvduinaM+wm5v/ENyML4r0oDDwPTkHy+eiKUeJ4NvMqj4vFtbrcWM34\n"
                + "fd4sBj0CAwEAAaM1MDMwMQYDVR0RBCowKIIVbG9jYWxob3N0LmxvY2FsZG9tYWlu\n"
                + "gglsb2NhbGhvc3SHBH8AAAEwDQYJKoZIhvcNAQELBQADggEBAKIhFtp3bgOfsvVp\n"
                + "RDY8DlJDmtrxKZi1aXGEjr0MqfQ8BZbh1Mb4ZXOrNQRskyP+6/BWPqIbHQ5qLEEW\n"
                + "V8B4XYlX18VEzOX/J1e9oztPNmJ4MvQGgmEr07bgbEBQNtb9Xexdr8UVBDNq7/qg\n"
                + "si1WCJjOyBU9UrnElxwjIAX2wIt1W7WoGtluDEiliTcsoYoIDNSe8pW1Y5Z+KxDQ\n"
                + "MxoqD23bvutZXm1nM/mCYMIYAj8g+RE4CSHwd7N2q05p1uM1BYdXP5GflR6Ols1t\n"
                + "RD6IzYEdpvx6RkLk5ZJYlPASH1WnOC0jg4CcPozfrFsATHOJbvQBjoHIsFeozTqA\n"
                + "Zs/akKg=\n"
                + "-----END CERTIFICATE-----\n");
        assertEquals(new X500Principal("CN=*.mariadb.org, O=\"Acme, Inc.\", L=SZ, ST=GD, C=CN"), cert.getSubjectX500Principal());


        verifier.verify("localhost", cert);
        verifier.verify("localhost.localdomain", cert);
        verifyExceptionEqual("local.host", cert, "DNS host \"local.host\" not found in certificate "
                + "alt-names certificate SubjectAltNames[{\"localhost.localdomain\"|DNS},{\"localhost\"|DNS},{\"127.0.0.1\"|IP}]");

        verifier.verify("127.0.0.1", cert);
        verifyExceptionEqual("127.0.0.2", cert, "No IPv4 corresponding to host \"127.0.0.2\" "
                + "in certificate alt-names certificate SubjectAltNames[{\"localhost.localdomain\"|DNS},{\"localhost\"|DNS},{\"127.0.0.1\"|IP}]");
    }

    @Test public void wildcardsCannotMatchIpAddresses() throws Exception {
        // openssl req -x509 -nodes -days 36500 -subj '/CN=*.0.0.1' -newkey rsa:512 -out cert.pem
        X509Certificate cert = getCertificate(""
                + "-----BEGIN CERTIFICATE-----\n"
                + "MIIDVzCCAj+gAwIBAgIJAN9L/Y9e1F7dMA0GCSqGSIb3DQEBBQUAMEExCzAJBgNV\n"
                + "BAYTAlVTMQswCQYDVQQIDAJDQTETMBEGA1UECgwKQWNtZSwgSW5jLjEQMA4GA1UE\n"
                + "AwwHKi4wLjAuMTAgFw0xNzA2MjMxNTU3MTdaGA8yMTE3MDUzMDE1NTcxN1owQTEL\n"
                + "MAkGA1UEBhMCVVMxCzAJBgNVBAgMAkNBMRMwEQYDVQQKDApBY21lLCBJbmMuMRAw\n"
                + "DgYDVQQDDAcqLjAuMC4xMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA\n"
                + "xAdLfWkv98yRxAGjnNlXYeLUl1zW8gi/rl35d4Wa/6f++/5flnk0otzuLTVIU+hM\n"
                + "SvUcbtHpe+XbtS0ab1np+aCwJsJEiZQIbXI8RHbR8CBY2EqSgaC84zw/FnMBLR1a\n"
                + "Bq6QGcb0QNVBCsu75se4eOLyrKwaJhAa9IHL9AtIxaN+RKPIW/wjalPSMuJ6CGBE\n"
                + "6XcimtFyyJ5sL8G/WInTqzwEPUL0SvOY/GodoKF3MYUBOb4biilycHHM+40NUT0J\n"
                + "otRLhDlKLEH1NFNDYvKvIS1Q5SecPKiS2tfcvOROavZ2FPwwiBEyw2QkPnAKkhO8\n"
                + "tyEFcJ2PSVyfk0xvRG38CwIDAQABo1AwTjAdBgNVHQ4EFgQUkygWj4eHIi1YDpBm\n"
                + "Z5143b+oFT8wHwYDVR0jBBgwFoAUkygWj4eHIi1YDpBmZ5143b+oFT8wDAYDVR0T\n"
                + "BAUwAwEB/zANBgkqhkiG9w0BAQUFAAOCAQEAAE4YYuS94g9EyIRvPeXcHlJsjG1n\n"
                + "moSZIVgSuKjLAb31SOyY+2c534SUELED7ECEb4yKM7WmWctpF0anFZUDCczuo+zl\n"
                + "uuTv1k/TE9GBWizxZgu/vX7+FAbxAgkA9Jxn2phJlks+PwnUClzVBzJ77zPNzIO8\n"
                + "6s8waZr9ttnASBHVaeSKkknI+gas5KpvY+B4eRxZx0G8Fyher29yIiE44Z6RHzjI\n"
                + "+EnURTvdjd2ZuY5QKvwlBQssqOHxDATg8pL6JmgnrvbYqh+FBpUN8sqwrXx6q8dz\n"
                + "aUH7ncQGgwZBAUIiQaKlb0QYpcyrMlGWNri+RFt+Goz5S3BxxobwfiaBoA==\n"
                + "-----END CERTIFICATE-----\n");
        verifyExceptionEqual("127.0.0.1", cert, "host \"127.0.0.1\" doesn't correspond to "
                + "certificate CN \"*.0.0.1\" : wildcards not possible for IPs");
    }

    @Test public void subjectAltNameWithWildcard() throws Exception {
        //subjectAltName=DNS:*.other.org,DNS:a*b.other2.com
        X509Certificate cert = getCertificate(""
                + "-----BEGIN CERTIFICATE-----\n"
                + "MIIDVjCCAj6gAwIBAgIJAPMG38xrY9DaMA0GCSqGSIb3DQEBCwUAMFMxCzAJBgNV\n"
                + "BAYTAkNOMQswCQYDVQQIDAJHRDELMAkGA1UEBwwCU1oxEzARBgNVBAoMCkFjbWUs\n"
                + "IEluYy4xFTATBgNVBAMMDEFjbWUgUm9vdCBDQTAgFw0xNzA2MjMxNjA1MTlaGA8y\n"
                + "MTE3MDUzMDE2MDUxOVowVDELMAkGA1UEBhMCQ04xCzAJBgNVBAgMAkdEMQswCQYD\n"
                + "VQQHDAJTWjETMBEGA1UECgwKQWNtZSwgSW5jLjEWMBQGA1UEAwwNKi5tYXJpYWRi\n"
                + "Lm9yZzCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAKqUYr7IHOuE1FD3\n"
                + "4AX/23DZ/VnK3U/sZNLfdP9322pO5HP4yv3HzHNtkl+3s/jKnRBwOpGjvmeoVgro\n"
                + "B8NK7Prs8tCMET9yfFXg/gSkoMAnR4g1jExB9bszTRN1+5dmLZK7xoKcRYtdKCLB\n"
                + "AeGYAx6lSKFkc5sAuU8e9k9iAdD+j2w4s5UbP2QTK6N97+EMGTYjQ91ynsxzUltX\n"
                + "5ueRLbg0M5WyRZpB4oH3J5T2L+NzpjU60Lhny/Kz5fCDizkwYjYBV+p9bhneklia\n"
                + "ByBxGm/y+yrDm5RDRICws5UhjWEe5ztOrfLwjZGmkYgcr2iuVqR3yhXoQIrxK1OV\n"
                + "8RnNJSECAwEAAaMqMCgwJgYDVR0RBB8wHYILKi5vdGhlci5vcmeCDmEqYi5vdGhl\n"
                + "cjIuY29tMA0GCSqGSIb3DQEBCwUAA4IBAQAAgqjYSzvgc+lUa/8gEpX9QJVvvDN9\n"
                + "nKqsJIB8G7uSGQgjq1eA8LrklTo1X3uER2+dLfoHIvJJxzuqRF6ugDnHMW+ocITY\n"
                + "yYkvb1Ok/aKo9e9sEKhndT47A9fjGoN94xhEEfVL8oc2g5gnNQ/+YHwO0vajdh2V\n"
                + "CMpkFvvSClvomb91u/leWwu1C07dJWHM2OzldEmlQK9sm847YofEfXe5FZXt+Py2\n"
                + "zpmwb3/djqBpSwdMgBB3us2wEiHN95EGRCT8BmTZ4gFtfdXt6uAZOd93NAoYlmpV\n"
                + "Flo8jrfEOHRCrdYqXobC/YVuxk+1h+Q2Nu5mKzbc3XfpG1LGGZB98+FP\n"
                + "-----END CERTIFICATE-----\n");

        verifyExceptionEqual("other.org", cert, "DNS host \"other.org\" not found in certificate "
                + "alt-names certificate SubjectAltNames[{\"*.other.org\"|DNS},{\"a*b.other2.com\"|DNS}]");
        verifier.verify("www.other.org", cert);
        verifyExceptionEqual("other2.org", cert, "DNS host \"other2.org\" not found in certificate "
                + "alt-names certificate SubjectAltNames[{\"*.other.org\"|DNS},{\"a*b.other2.com\"|DNS}]");
        verifyExceptionEqual("www.other2.org", cert, "DNS host \"www.other2.org\" not found in "
                + "certificate alt-names certificate SubjectAltNames[{\"*.other.org\"|DNS},{\"a*b.other2.com\"|DNS}]");
        verifier.verify("ab.other2.com", cert);
        verifier.verify("axxxxb.other2.com", cert);
        verifyExceptionEqual("axxxxbc.other2.org", cert, "DNS host \"axxxxbc.other2.org\" not found "
                + "in certificate alt-names certificate SubjectAltNames[{\"*.other.org\"|DNS},{\"a*b.other2.com\"|DNS}]");
        verifyExceptionEqual("caxxxxb.other2.org", cert, "DNS host \"caxxxxb.other2.org\" not found "
                + "in certificate alt-names certificate SubjectAltNames[{\"*.other.org\"|DNS},{\"a*b.other2.com\"|DNS}]");
        verifyExceptionEqual("a.axxxxb.other2.org", cert, "DNS host \"a.axxxxb.other2.org\" not found "
                + "in certificate alt-names certificate SubjectAltNames[{\"*.other.org\"|DNS},{\"a*b.other2.com\"|DNS}]");
    }

}
