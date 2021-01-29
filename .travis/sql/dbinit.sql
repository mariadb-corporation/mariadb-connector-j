CREATE USER 'bob'@'localhost';
GRANT ALL ON *.* TO 'bob'@'localhost' with grant option;

CREATE USER 'bob'@'%';
GRANT ALL ON *.* TO 'bob'@'%' with grant option;

CREATE USER 'boby'@'%' identified by 'hey';
GRANT ALL ON *.* TO 'boby'@'%' /*M!100401 identified by 'hey' */ with grant option;

CREATE USER 'boby'@'localhost' identified by 'hey';
GRANT ALL ON *.* TO 'boby'@'localhost' /*M!100401 identified by 'hey' */ with grant option;

FLUSH PRIVILEGES;

CREATE DATABASE test2;