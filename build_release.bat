REM Assume zip install layout. and MYSQL_INSTALL_ROOT environment variable points to the root directory of the installation

IF "%MYSQL_INSTALL_ROOT%"=="" set MYSQL_INSTALL_ROOT=C:\mysql\mariadb-5.5.28a-winx64\mariadb-5.5.28a-winx64\

set BINDIR=%MYSQL_INSTALL_ROOT%\bin
set CERTDIR=%MYSQL_INSTALL_ROOT%\mysqltest\std_data
start %BINDIR%\mysqld.exe --console --max_allowed_packet=1G --enable-named-pipe --socket=JDBC-test-socket --ssl-ca=%CERTDIR%\cacert.pem --ssl-cert=%CERTDIR%\server-cert.pem --ssl-key=%CERTDIR%\server-key.pem
timeout 20
call mvn exec:exec package -Dpackage-source
call %BINDIR%\mysqladmin -uroot shutdown

#deploy package
#mvn clean deploy -Dmaven.test.skip=true -Dpackage-source

