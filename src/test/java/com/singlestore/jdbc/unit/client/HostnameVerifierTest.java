// SPDX-License-Identifier: LGPL-2.1-or-later
// Copyright (c) 2012-2014 Monty Program Ab
// Copyright (c) 2015-2021 MariaDB Corporation Ab
// Copyright (c) 2021 SingleStore, Inc.

package com.singlestore.jdbc.unit.client;

import static org.junit.jupiter.api.Assertions.*;

import com.singlestore.jdbc.client.tls.HostnameVerifier;
import java.io.ByteArrayInputStream;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import javax.net.ssl.SSLException;
import javax.security.auth.x500.X500Principal;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class HostnameVerifierTest {

  private static X509Certificate getCertificate(String certString) throws CertificateException {
    CertificateFactory cf = CertificateFactory.getInstance("X.509");
    return (X509Certificate)
        cf.generateCertificate(new ByteArrayInputStream(certString.getBytes()));
  }

  private void verifyExceptionEqual(String host, X509Certificate cert, String exceptionMessage) {
    Exception e =
        Assertions.assertThrows(SSLException.class, () -> HostnameVerifier.verify(host, cert, -1));
    Assertions.assertTrue(
        e.getMessage().contains(exceptionMessage), "real message:" + e.getMessage());
  }

  //   generating certificate example
  //        openssl genrsa -out "/home/osboxes/key" 2048
  //        openssl req -new -utf8 -sha1 \
  //              -key /home/osboxes/key \
  //              -subj "/C=US/ST=CA/O=Acme, Inc./CN=singlestore.org" \
  //              -reqexts SAN \
  //              -config <(cat /etc/ssl/openssl.cnf \
  //                      <(printf
  //   "\n[SAN]\nsubjectAltName=DNS:other.org,DNS:www.other.org")) \
  //              -out domain.csr

  @Test
  public void verifyCn() throws Exception {
    // CN=test.com
    X509Certificate cert =
        getCertificate(
            ""
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
    HostnameVerifier.verify("test.com", cert, -1);
    verifyExceptionEqual(
        "a.test.com",
        cert,
        "DNS host \"a.test.com\" doesn't correspond to certificate CN \"test.com\"");
    verifyExceptionEqual(
        "other.com",
        cert,
        "DNS host \"other.com\" doesn't correspond to certificate CN \"test.com\"");
  }

  @Test
  public void verifyNoSan() throws Exception {
    // CN=*.singlestore.com
    X509Certificate cert =
        getCertificate(
            ""
                + "-----BEGIN CERTIFICATE-----\n"
                + "MIIDYjCCAkqgAwIBAgIUa5fj2ylHRanZ8B1vxNLDoUn3YfgwDQYJKoZIhvcNAQEL\n"
                + "BQAwUzELMAkGA1UEBhMCQ04xCzAJBgNVBAgMAkdEMQswCQYDVQQHDAJTWjETMBEG\n"
                + "A1UECgwKQWNtZSwgSW5jLjEVMBMGA1UEAwwMQWNtZSBSb290IENBMCAXDTIxMTAw\n"
                + "NzE0MTAxOFoYDzIxMjEwOTEzMTQxMDE4WjBYMQswCQYDVQQGEwJDTjELMAkGA1UE\n"
                + "CAwCR0QxCzAJBgNVBAcMAlNaMRMwEQYDVQQKDApBY21lLCBJbmMuMRowGAYDVQQD\n"
                + "DBEqLnNpbmdsZXN0b3JlLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC\n"
                + "ggEBAMJDUVuIBo+8xTwIXdu/DKvO8GU/KAJM02ei7DUVvEg6W/CHoDOjyzkOL2t4\n"
                + "LOHwpAobj/0m533WYhilNeEycVZC6u69fcOfEKC8XpLkl4XhU4dh73eFzdqZkKvS\n"
                + "nngsJkbmRRg1gkRh5SGrPWXFh+9DBeA+avaexo9SuE2oNvcjusQmNWlVJQI3GPQp\n"
                + "ysMnfKTvsKstd4ibM8Ro5mRRrQHRlHA349QAbChNoij4WFM7h2CTqJ6EZaI2RiYv\n"
                + "oK5Kov+IqV4O2EhcWpTVNjwlPTGB3Q6+sEqi0KR45r+/9l08x/jwC9ImvCqXo+83\n"
                + "hz4OM+IVt3DAZpCxhwUrysaKcfMCAwEAAaMnMCUwIwYDVR0RBBwwGoIJb3RoZXIu\n"
                + "b3Jngg13d3cub3RoZXIub3JnMA0GCSqGSIb3DQEBCwUAA4IBAQBRS6ZdBLc2fWId\n"
                + "LwCg2ilIkMIr8yyq26KohD2dj+hzGjw8RLtMksPK6y90N8JQK8wJIfoBPX/ahCql\n"
                + "pQ29+glLB2WP0ol20yJ6FkHYm86tKC5CYZ/2TAeWQETNqeOv39428S+9Bl3q/aP/\n"
                + "XrGxF+7xUJgMdoIBW2ObZXYHZVkI4Uyp3sxeDJl1gjN1DSNv9wkHf0P0QR/w9iB5\n"
                + "SSbpJPn0AauO42iQOR/XEJoejT3fkedcB2wimS2NaT55/Pma/427Fp8+dEkaNt1H\n"
                + "XWRY+MZ1Dxw5nOwITIm3QLIaUx3pYaVSQ4K/Pe1GDH8GUTPhodAZGK4GMemfs80P\n"
                + "vyvl4ONr\n"
                + "-----END CERTIFICATE-----\n");
    HostnameVerifier.verify("test.singlestore.com", cert, -1);
    verifyExceptionEqual(
        "test.org",
        cert,
        "DNS host \"test.org\" doesn't correspond to certificate CN \"*.singlestore.com\"");
  }

  @Test
  public void verifyNonAsciiCn() throws Exception {
    // CN=😎.com = "\uD83D\uDE0E"
    X509Certificate cert =
        getCertificate(
            ""
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
    HostnameVerifier.verify("😎.com", cert, -1);
    verifyExceptionEqual(
        "a.😎.com", cert, "DNS host \"a.😎.com\" doesn't correspond to certificate CN \"😎.com\"");
  }

  @Test
  public void verifySubjectAlt() throws Exception {
    // CN=singlestore.org, subjectAlt=other.org,www.other.org
    //        openssl genrsa -out ca.key 2048
    //        openssl req -new -x509 -days 36500 -key ca.key -subj "/C=CN/ST=GD/L=SZ/O=Acme,
    // Inc./CN=Acme Root CA" -out ca.crt
    //
    //        openssl req -newkey rsa:2048 -nodes -keyout server.key -subj "/C=CN/ST=GD/L=SZ/O=Acme,
    // Inc./CN=*.singlestore.com" \
    //                  -out server.csr
    //        openssl x509 -req -extfile <(printf "subjectAltName=DNS:other.org,DNS:www.other.org")
    // -days 36500 \
    //                  -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -out server.crt
    X509Certificate cert =
        getCertificate(
            "-----BEGIN CERTIFICATE-----\n"
                + "MIIDYjCCAkqgAwIBAgIUa5fj2ylHRanZ8B1vxNLDoUn3YfcwDQYJKoZIhvcNAQEL\n"
                + "BQAwUzELMAkGA1UEBhMCQ04xCzAJBgNVBAgMAkdEMQswCQYDVQQHDAJTWjETMBEG\n"
                + "A1UECgwKQWNtZSwgSW5jLjEVMBMGA1UEAwwMQWNtZSBSb290IENBMCAXDTIxMTAw\n"
                + "NzE0MDgwMloYDzIxMjEwOTEzMTQwODAyWjBYMQswCQYDVQQGEwJDTjELMAkGA1UE\n"
                + "CAwCR0QxCzAJBgNVBAcMAlNaMRMwEQYDVQQKDApBY21lLCBJbmMuMRowGAYDVQQD\n"
                + "DBEqLnNpbmdsZXN0b3JlLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC\n"
                + "ggEBAO3tmtMuZQ1SMN+Oevrtpo9xjKVJbMzTDcpk2FXnjmNnsoSiub9VMfOcU8KY\n"
                + "MQ/LrZ+86kBNIh+aarNdeaLMFFfruPcK0pktK1wXoca+enYPz6KbscmtLx0F+9LZ\n"
                + "/ePlRrJASzv21dEgaIjnCVDviNCl36Ha0HywgkHr/6zw5u+wLRGUbA6/W12iLOFt\n"
                + "tYghkD3LIfwjP6jJgC193ZSvB3DIZ+sFCuDJipsG7D02ZiwAqy6hzYnHjs7Yq9M+\n"
                + "KyMoo1cdJ4t4uHwWQ9lLIDdA/XcShOiCK1aoPRWAFnf3XaQ0pLR6QWCF5tpKKWea\n"
                + "Z1O0fG8Hpb7jgs0w6ESs2gVVD48CAwEAAaMnMCUwIwYDVR0RBBwwGoIJb3RoZXIu\n"
                + "b3Jngg13d3cub3RoZXIub3JnMA0GCSqGSIb3DQEBCwUAA4IBAQCXBgAm4V4RYbq2\n"
                + "ceEtYofkBBdxT5oxQFwnXd7EKGTM5CeZEZNprPxqrwPw6ACh7l52odDwohikFW0h\n"
                + "RHJ9y5SMTZ/CFcxEUG1zJvGG/ZafAME+CPtL7HaQMIegy4VyjM4/O5R/ljcCDfwL\n"
                + "NmQif8GLrO96qoOKeAbATHwK3w/GHVk16NS0fuJpIRZ3TDJPauOvXZwlcDXEyS3i\n"
                + "4gV73DEJ6Q++lJ5wWIueRpA9+Mv2OuX1FSatQwA+L+/Jq6JeaQyEwU/xfj3VGF3b\n"
                + "m17W8skw9Ng8V712Xd+FgwOtZwVc08ymNryS9DhtfkVaJRX0VoF5N9QRuPtjAkgH\n"
                + "GlDZpnHV\n"
                + "-----END CERTIFICATE-----\n");

    verifyExceptionEqual(
        "singlestore.com",
        cert,
        "DNS host \"singlestore.com\" doesn't correspond to certificate "
            + "CN \"*.singlestore.com\" and SAN[{DNS:\"other.org\"},{DNS:\"www.other.org\"}]");
    HostnameVerifier.verify("a.singlestore.com", cert, -1);
    verifyExceptionEqual(
        "a.other2.org",
        cert,
        "DNS host \"a.other2.org\" doesn't correspond to certificate "
            + "CN \"*.singlestore.com\" and SAN[{DNS:\"other.org\"},{DNS:\"www.other.org\"}]");
    HostnameVerifier.verify("other.org", cert, -1);
    verifyExceptionEqual(
        "a.other.org",
        cert,
        "DNS host \"a.other.org\" doesn't correspond to certificate "
            + "CN \"*.singlestore.com\" and SAN[{DNS:\"other.org\"},{DNS:\"www.other.org\"}]");
    HostnameVerifier.verify("www.other.org", cert, -1);
  }

  @Test
  public void verifySubjectAltOnly() throws Exception {
    // subjectAlt=foo.com
    X509Certificate cert =
        getCertificate(
            ""
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
    HostnameVerifier.verify("foo.com", cert, -1);
    verifyExceptionEqual(
        "a.foo.com",
        cert,
        "CN not found in certificate principal "
            + "\"EMAILADDRESS=juliusdavies@gmail.com, OU=test certificates, O=httpcomponents, L=Forest Hill, "
            + "ST=Maryland, C=US\" and DNS host \"a.foo.com\" doesn't correspond to SAN[{DNS:\"foo.com\"}]");
  }

  @Test
  public void noCn() throws Exception {
    // subjectAlt=foo.com
    X509Certificate cert =
        getCertificate(
            "-----BEGIN CERTIFICATE-----\n"
                + "MIIDazCCAlOgAwIBAgIUFxeXaoK5VSZBV1UdBhcDIB0abUAwDQYJKoZIhvcNAQEF\n"
                + "BQAwRTELMAkGA1UEBhMCQVUxEzARBgNVBAgMClNvbWUtU3RhdGUxITAfBgNVBAoM\n"
                + "GEludGVybmV0IFdpZGdpdHMgUHR5IEx0ZDAeFw0yMTA0MTQxNjIxMTlaFw0zMTA0\n"
                + "MTIxNjIxMTlaMEUxCzAJBgNVBAYTAkFVMRMwEQYDVQQIDApTb21lLVN0YXRlMSEw\n"
                + "HwYDVQQKDBhJbnRlcm5ldCBXaWRnaXRzIFB0eSBMdGQwggEiMA0GCSqGSIb3DQEB\n"
                + "AQUAA4IBDwAwggEKAoIBAQDRmmo+05f5oqKFidWhk6+EbCNJzxJ7fwrGV13ZfqqP\n"
                + "HSyxZIcWI/bK6BCjb9BxJa/QaaThr6x7GxI6PaDqRWInpuHepqIBL2arJLq2H3ys\n"
                + "2zErfA5n0rFkryYW6zQUW/TJTxz5dbagemYA4TvS5Tshm0fimtNDTcv6Vb7U3OXc\n"
                + "pa42VeLgaaM+OeCQlFH4OEXzGqXwqU090D2aRp05uPJRCFhwvMI9QXG2R8zXogTx\n"
                + "TAlmxm4piKmg123TLd2N1TxJHxskg4OR5guO/XaG/Zji4KCKJ7dJFHjvNztG0Nme\n"
                + "dxOo/+I/AeWhLEq81fzGMg4/BYeU1cLv6wnqFi4pbyWLAgMBAAGjUzBRMB0GA1Ud\n"
                + "DgQWBBTLvLm0GjjbZOg5fJYgt80FvCgxNzAfBgNVHSMEGDAWgBTLvLm0GjjbZOg5\n"
                + "fJYgt80FvCgxNzAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3DQEBBQUAA4IBAQBp\n"
                + "3Z8UMM8y++04mKRZEP7y88dmmLOt64TRjJhTFoHvZsC/VZi5glt3/FnYxhlf8sfQ\n"
                + "sJB+TRlVcEWBff6yJd/uUScff56Zgy2PCCL4rjgBjxgq/kf28m6tC8nHlx88qz0Y\n"
                + "CZR9eEwTN8xgRvgUx5OFNewDYpkY0QAHkzAddCl5uaO6Mi0E34gSECIQ/cJ75xhQ\n"
                + "K3Qy/qj1Kl7r80WJtzmhpZbpKbVZXa3NpwTWUfaD6WhNW0H/BhAnQq3XkXVWK6sW\n"
                + "rLfHdWz9hQ79AiNaTc1I3YDyrnNEQBvHAFZ87Y8XIk4RaPzttLAfL/IKHuJdb85M\n"
                + "cMq0UjgrIuCJoKB8kcm/\n"
                + "-----END CERTIFICATE-----");
    verifyExceptionEqual(
        "a.foo.com",
        cert,
        "CN not found in certificate principal \"O=Internet Widgits Pty Ltd, ST=Some-State, C=AU\" and certificate doesn't contain SAN");
  }

  @Test
  public void verifyMultipleCn() throws Exception {
    // CN=test1.org, CN=test2.org
    X509Certificate cert =
        getCertificate(
            ""
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
    HostnameVerifier.verify("test1.org", cert, -1);
    verifyExceptionEqual(
        "test2.org",
        cert,
        "DNS host \"test2.org\" doesn't correspond to certificate CN \"test1.org\"");
  }

  @Test
  public void verifyWilcardCn() throws Exception {
    // CN=*.foo.com
    X509Certificate cert =
        getCertificate(
            ""
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
    verifyExceptionEqual(
        "foo.com", cert, "DNS host \"foo.com\" doesn't correspond to certificate CN \"*.foo.com\"");
    HostnameVerifier.verify("www.foo.com", cert, -1);
    HostnameVerifier.verify("花子.foo.com", cert, -1);
    verifyExceptionEqual(
        "a.b.foo.com",
        cert,
        "DNS host \"a.b.foo.com\" doesn't correspond to certificate CN \"*.foo.com\"");
  }

  @Test
  public void verifyWilcardCnOnTld() throws Exception {
    // It's the CA's responsibility to not issue broad-matching certificates!
    // CN=*.co.jp
    X509Certificate cert =
        getCertificate(
            ""
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
    HostnameVerifier.verify("foo.co.jp", cert, -1);
    HostnameVerifier.verify("花子.co.jp", cert, -1);
  }

  @Test
  public void subjectAltUsesLocalDomainAndIp() throws Exception {
    // /C=CN/ST=GD/L=SZ/O=Acme, Inc./CN=*.singlestore.org,
    // subjectAltName=DNS:localhost.localdomain,DNS:localhost,IP:127.0.0.1
    X509Certificate cert =
        getCertificate(
            ""
                + "-----BEGIN CERTIFICATE-----\n"
                + "MIIDgjCCAmqgAwIBAgIUa5fj2ylHRanZ8B1vxNLDoUn3YfowDQYJKoZIhvcNAQEL\n"
                + "BQAwUzELMAkGA1UEBhMCQ04xCzAJBgNVBAgMAkdEMQswCQYDVQQHDAJTWjETMBEG\n"
                + "A1UECgwKQWNtZSwgSW5jLjEVMBMGA1UEAwwMQWNtZSBSb290IENBMCAXDTIxMTAw\n"
                + "NzE0MTMzOFoYDzIxMjEwOTEzMTQxMzM4WjBYMQswCQYDVQQGEwJDTjELMAkGA1UE\n"
                + "CAwCR0QxCzAJBgNVBAcMAlNaMRMwEQYDVQQKDApBY21lLCBJbmMuMRowGAYDVQQD\n"
                + "DBEqLnNpbmdsZXN0b3JlLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC\n"
                + "ggEBAKaaPFH8kXwvL21ho+XFMLmJ2G9kPkUxp2YIteBeia3qy2Wsqb0A+DCJetZ/\n"
                + "RNX51/cFEUBRMxOUPDvBHAK7NfhIblbCNR9gXHoPTbCGhOlS+jKR/CoNTU7wQ5kS\n"
                + "VadNw1iP/E0DHqEM4q2nnN7+3Lo4GHe5lZbbr30mC6ySKAEVtNsi5UU0KjyNjd1n\n"
                + "snxkKN5AcD4THgftgutBDaMEz0+adJus5Mv5yD2wgZK+wG0kzfIu+L83ggV2VxAP\n"
                + "amuB4S/qIN7Lw0/uOCEouGerNhHzPyR77qPnxNyNtI58o341Vo2Gs+b0xuGRD01S\n"
                + "JGIsVOPpO7pXz4ym6te/PYXQpXsCAwEAAaNHMEUwQwYDVR0RBDwwOoIVbG9jYWxo\n"
                + "b3N0LmxvY2FsZG9tYWlugglsb2NhbGhvc3SHBH8AAAGHECABDbg5AjRoAAAAAAAA\n"
                + "BEMwDQYJKoZIhvcNAQELBQADggEBABQ4uWdYY/p+sUThi+opPYZP/XQFAeSEeedM\n"
                + "QrosklUENl8FBn/wypOg247wKKlzdFjmCIA0qzXrpzoFCnay9OkERoHfuOJ28cSI\n"
                + "mJbNhVhQ2BTauFENo2faK+36xF4wuYHV6j7b3V5fjKvSKlSxSwhHYf+ElIqSWmah\n"
                + "c5Zr/kSCGv8z7y9BYaMriZIOCb6bVTKSmP7HP35jdZhKY/1oo6KkmArCqcgB4Mdd\n"
                + "pnIu0dJhzxilJ1D60sZkv/VSGIh3j+XMcK79NtEkqvdAWfFmcn9gmj/X1I+pGzA5\n"
                + "pHOrtjFbvhViS+qYlgRD7djh463Etno8YrR6QxnE1cEDwbDzoxU=\n"
                + "-----END CERTIFICATE-----\n");
    assertEquals(
        new X500Principal("CN=*.singlestore.com, O=\"Acme, Inc.\", L=SZ, ST=GD, C=CN"),
        cert.getSubjectX500Principal());

    HostnameVerifier.verify("localhost", cert, -1);
    HostnameVerifier.verify("localhost.localdomain", cert, -1);
    verifyExceptionEqual(
        "local.host",
        cert,
        "DNS host \"local.host\" doesn't correspond to certificate "
            + "CN \"*.singlestore.com\" and SAN[{DNS:\"localhost.localdomain\"},{DNS:\"localhost\"},{IP:\"127.0.0.1\"},{IP:\"2001:db8:3902:3468:0:0:0:443\"}]");

    HostnameVerifier.verify("127.0.0.1", cert, -1);
    verifyExceptionEqual(
        "127.0.0.2",
        cert,
        "IPv4 host \"127.0.0.2\" doesn't correspond to certificate "
            + "CN \"*.singlestore.com\" and SAN[{DNS:\"localhost.localdomain\"},{DNS:\"localhost\"},{IP:\"127.0.0.1\"},{IP:\"2001:db8:3902:3468:0:0:0:443\"}]");

    HostnameVerifier.verify("2001:db8:3902:3468:0:0:0:443", cert, -1);
    verifyExceptionEqual(
        "2001:db8:1::",
        cert,
        "IPv6 host \"2001:db8:1::\" doesn't correspond to certificate "
            + "CN \"*.singlestore.com\" and SAN[{DNS:\"localhost.localdomain\"},{DNS:\"localhost\"},{IP:\"127.0.0.1\"},{IP:\"2001:db8:3902:3468:0:0:0:443\"}]");
  }

  @Test
  public void wildcardsCannotMatchIpAddresses() throws Exception {
    // openssl req -x509 -nodes -days 36500 -subj '/CN=*.0.0.1' -newkey rsa:512 -out cert.pem
    X509Certificate cert =
        getCertificate(
            ""
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
    verifyExceptionEqual(
        "127.0.0.1",
        cert,
        "IPv4 host \"127.0.0.1\" doesn't correspond to "
            + "certificate CN \"*.0.0.1\" : wildcards not possible for IPs");
  }

  @Test
  public void subjectAltNameWithWildcard() throws Exception {
    // subjectAltName=DNS:*.other.org,DNS:a*b.other2.com
    X509Certificate cert =
        getCertificate(
            ""
                + "-----BEGIN CERTIFICATE-----\n"
                + "MIIDZTCCAk2gAwIBAgIUa5fj2ylHRanZ8B1vxNLDoUn3YfswDQYJKoZIhvcNAQEL\n"
                + "BQAwUzELMAkGA1UEBhMCQ04xCzAJBgNVBAgMAkdEMQswCQYDVQQHDAJTWjETMBEG\n"
                + "A1UECgwKQWNtZSwgSW5jLjEVMBMGA1UEAwwMQWNtZSBSb290IENBMCAXDTIxMTAw\n"
                + "NzE0MTQzNloYDzIxMjEwOTEzMTQxNDM2WjBYMQswCQYDVQQGEwJDTjELMAkGA1UE\n"
                + "CAwCR0QxCzAJBgNVBAcMAlNaMRMwEQYDVQQKDApBY21lLCBJbmMuMRowGAYDVQQD\n"
                + "DBEqLnNpbmdsZXN0b3JlLmNvbTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoC\n"
                + "ggEBAMbODs2R+lM3UNvHOyoYZUS4JJjlAocKGHXRxUlnbOTTT1KtVN1ZdnG3XPC7\n"
                + "zhk+aHzuonhNcgeXDav6x0pDiAZsnEvMhOji5B+IciUFiFW2eso4dn8sb90BpIwQ\n"
                + "QnNWZUOG77jSPvt/GGbSBTyDyxVBGaP6TPuA7munj9mW0qz4P1oC6N7/olOV7XTG\n"
                + "qFfTJZhCt3V0IL8d7p0XDR9renDFb0qzbsdytwGDuF7GwInksnGQsojr7VG6Zo2O\n"
                + "IEiv6zyLAWoKKnu3fCmJZiKJCWQ0zpL5woUCrJc2jK54FUmYDcSIGItM9JCo1mzG\n"
                + "ydF0QCHl93cazcdWVfUQdpZZ6N0CAwEAAaMqMCgwJgYDVR0RBB8wHYILKi5vdGhl\n"
                + "ci5vcmeCDmEqYi5vdGhlcjIuY29tMA0GCSqGSIb3DQEBCwUAA4IBAQANP0783M7E\n"
                + "cjHE+XOp1j8W5GZG7vgkfY6KJDuCRN3p95WC/okOIH55E3lHnvq6uEVOfCgJAmsq\n"
                + "ksFTHn+s3/OtqBbkt5dUxtFsW79+in32Kg7iOqdohMgZqpVDQsm/tK7YVvr8hxbB\n"
                + "CpFtuNXE+GXSjdNyLA7d1lzTt/g1O44IbHFcigbx/u0j8cyT8+cE0oUt9ScKYk8N\n"
                + "5Ymq7quKCIQ5aOVLr8WF2NfpnKXDlW11V7hJRDnJyEY16bb2aLmEFXK0kOYoEMgi\n"
                + "F9fxHYd9Kv5hdAVLjpkgApuK8/me6cEnv+UcAF2D5KkokpSpkmRYPUOr1kUySnbZ\n"
                + "4OoJsTcGkICd\n"
                + "-----END CERTIFICATE-----\n");

    verifyExceptionEqual(
        "other.org",
        cert,
        "DNS host \"other.org\" doesn't correspond "
            + "to certificate CN \"*.singlestore.com\" and SAN[{DNS:\"*.other.org\"},{DNS:\"a*b.other2.com\"}]");
    HostnameVerifier.verify("www.other.org", cert, -1);
    verifyExceptionEqual(
        "other2.org",
        cert,
        "DNS host \"other2.org\" doesn't correspond "
            + "to certificate CN \"*.singlestore.com\" and SAN[{DNS:\"*.other.org\"},{DNS:\"a*b.other2.com\"}]");
    verifyExceptionEqual(
        "www.other2.org",
        cert,
        "DNS host \"www.other2.org\" doesn't correspond "
            + "to certificate CN \"*.singlestore.com\" and SAN[{DNS:\"*.other.org\"},{DNS:\"a*b.other2.com\"}]");
    HostnameVerifier.verify("ab.other2.com", cert, -1);
    HostnameVerifier.verify("axxxxb.other2.com", cert, -1);
    verifyExceptionEqual(
        "axxxxbc.other2.org",
        cert,
        "DNS host \"axxxxbc.other2.org\" doesn't "
            + "correspond to certificate CN \"*.singlestore.com\" and SAN[{DNS:\"*.other.org\"},{DNS:\"a*b.other2.com\"}]");
    verifyExceptionEqual(
        "caxxxxb.other2.org",
        cert,
        "DNS host \"caxxxxb.other2.org\" doesn't "
            + "correspond to certificate CN \"*.singlestore.com\" and SAN[{DNS:\"*.other.org\"},{DNS:\"a*b.other2.com\"}]");
    verifyExceptionEqual(
        "a.axxxxb.other2.org",
        cert,
        "DNS host \"a.axxxxb.other2.org\" doesn't "
            + "correspond to certificate CN \"*.singlestore.com\" and SAN[{DNS:\"*.other.org\"},{DNS:\"a*b.other2.com\"}]");
  }
}
