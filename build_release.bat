start C:\mysql\mariadb-5.5.28a-winx64\mariadb-5.5.28a-winx64\bin\mysqld --console --max_allowed_packet=1G
timeout 20
call mvn exec:exec package -Dpackage-source
call C:\mysql\mariadb-5.5.28a-winx64\mariadb-5.5.28a-winx64\bin\mysqladmin -uroot shutdown