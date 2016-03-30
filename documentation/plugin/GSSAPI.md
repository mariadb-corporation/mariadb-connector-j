
# GSSAPI Authentication

The following subsections show how to implement GSSAPI Authentication with the java connector. 
Server configuration can be found on https://github.com/MariaDB/server/blob/55d61ec878f94cfbdafee43599809dda98803a9f/plugin/auth_gssapi/README.md.

As part of the security context establishment, the driver will initiate a context that will be authenticated by database. 
Database also be authenticated back to the driver ("mutual authentication").

## General configuration

Database configuration must have been set. 
To use GSSAPI authentication, a user must be set to use GSSAPI :
CREATE USER one IDENTIFIED VIA gssapi AS 'userOne@EXAMPLE.COM';

The driver doesn't use SSO information. a user must be set.
for example using jdbc url : 
DriverManager.getConnection("jdbc:mariadb://db.example.com:3306/db?user=one");

When connecting to database, these user is send to database and GSSAPI authentication will be use. 
The principal (userOne@EXAMPLE.COM in example) must be the one defined on the user definition.
 
Database server will wait for a ticket associated for the principal defined in user ('userOne@EXAMPLE').
That mean on client, user must have obtained a TGT beforehand. 


### GSSAPI configuration
#### Java system properties

Realm information are generally defined by DNS, but this can be forced using system properties.
"java.security.krb5.kdc" defined the Key Distribution Center (KDC), realm by "java.security.krb5.realm".
Example : 

        System.setProperty("java.security.krb5.kdc", "kdc1.example.com");
        System.setProperty("java.security.krb5.realm", "EXAMPLE.COM");

Logging can be set using additional properties:

        System.setProperty("sun.security.krb5.debug", "true");
        System.setProperty("sun.security.jgss.debug", "true");
#### Jaas

The driver will use the native ticket cache to get the TGT available in it using JAAS.
If the System property "java.security.auth.login.config" is empty, driver will use the following configuration :

    Krb5ConnectorContext {
        com.sun.security.auth.module.Krb5LoginModule required useTicketCache=true renewTGT=true doNotPrompt=true; 
    };

This permit to use current user TGT cache. 

#### Java JCE

Depending on the kerberos ticket encryption, you may have to install the [Java Cryptography Extension (JCE)](http://www.oracle.com/technetwork/java/javase/downloads/jce8-download-2133166.html) Unlimited Strength Jurisdiction Policy File.
(CentOS/Red Hat Enterprise Linux 5.6 or later, Ubuntu are using AES-256 encryption by default for tickets).

On unix, you can execute the "klist -e" command to view the encryption type in use:
If AES is being used, output like the following is displayed after you type the klist command (note that AES-256 is included in the output):

Ticket cache: FILE:/tmp/krb5cc_0
Default principal: userOne@EXAMPLE
Valid starting     Expires            Service principal
03/30/15 13:25:04  03/31/15 13:25:04  krbtgt/EXAMPLE@EXAMPLE
    Etype (skey, tkt): AES-256 CTS mode with 96-bit SHA-1 HMAC, AES-256 CTS mode with 96-bit SHA-1 HMAC


#### Windows specific
To permit java to retrieve TGT (Ticket-Granting-Ticket), windows host need to have a registry entry set.

HKEY_LOCAL_MACHINE\System\CurrentControlSet\Control\Lsa\Kerberos\Parameters
Value Name: AllowTGTSessionKey
Value Type: REG_DWORD
Value: 1


##Possible errors

* "GSSException: Failure unspecified at GSS-API level (Mechanism level: No Kerberos credentials available)"
  There is no active credential. Check with klist that there is an existing credential. If not create it with the "kinit" command
* "java.sql.SQLInvalidAuthorizationSpecException: Could not connect: GSSAPI name mismatch, requested 'userOne@EXAMPLE.COM', actual name 'userTwo@EXEMPLE.COM'"
  There is an existing credential, but doesn't correspond to the connection user. 
  example :
    if user is created with a command like 
    ```script
    CREATE USER userOne@'%' IDENTIFIED WITH gssapi AS 'userTwo@EXAMPLE.COM';
    ```
    klist must show the same principal (userTwo@EXAMPLE.COM in this example)
* "GSSException: No valid credentials provided (Mechanism level: Clock skew too great (37))". The Kerberos protocol requires the time of the client 
  and server to match: if the system clocks of the client does not match that of the KDC server, authentication will fail with this kind of error. 
  The simplest way to synchronize the system clocks is to use a Network Time Protocol (NTP) server. 
