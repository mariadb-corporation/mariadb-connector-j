start C:\mysql\mariadb-5.5.28a-winx64\mariadb-5.5.28a-winx64\bin\mysqld --console --max_allowed_packet=1G --enable-named-pipe --socket=JDBC-test-socket
timeout 20
call mvn exec:exec package -Dpackage-source
call C:\mysql\mariadb-5.5.28a-winx64\mariadb-5.5.28a-winx64\bin\mysqladmin -uroot shutdown