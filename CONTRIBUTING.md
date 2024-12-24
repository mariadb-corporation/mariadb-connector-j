# Contributing

Each pull request should address a single issue, and contain both the fix and a description of how the pull request and
tests that validate that the PR fixes the issue in question.

For significant feature additions, we like to have an open issue
in [MariaDB JIRA](https://mariadb.atlassian.net/secure/RapidBoard.jspa?projectKey=CONJ). It is expected that discussion
will have taken place in the attached issue.

# Install Prerequisites

These are the set of tools which are required in order to complete any build. Follow the links to download and install
them on your own before continuing.

* At least one GPG Key see https://help.github.com/en/articles/generating-a-new-gpg-key
* [Oracle JDK 8](http://www.oracle.com/technetwork/java/javase/downloads/index.html) (
	with [JCE policies](http://www.oracle.com/technetwork/java/javase/downloads/jce8-download-2133166.html) if using
	TLS/SSL)
* IDE (eclipse / netbean / intelliJ) with maven and GIT plugins

# Fork source

Before downloading source, fork the project to your own repository, and use your repository as source.

## Branch signification

* master : correspond to the last released version
* develop : Develop new features for upcoming releases (compatible with java 8)
* develop-jre6 : maintenance branch compatible with java 6 / 7

# Run local test

Before any submission :
Run the test locally : by default, you need to have a MySQL/MariaDB server on localhost:3306 with a database named "
testj" and a user root without password.
so you can run

```script
		mvn test
```

You can change this parameter by adding -DdbUrl parameter. like :

```script
		mvn test -DdbUrl=jdbc:mariadb://127.0.0.1:3306/testj?user=root&password=*****
```

You can launch a specific test by adding -Dtest

```script
		mvn test -Dtest=org.mariadb.jdbc.JdbcParserTest
```

When all test are passing, you can package project.
Additional tests , like javadoc formatting, code style validation will be done :

```script
		mvn package -Dmaven.test.skip=true
```

If operation succeed, a new mariadb-java-client jar will be on the target folder.

# Ensure code style

```script
		mvn spotless:apply
```

# Run travis test

You can activate travis to validate your repository.
The advantage of travis compare to running test locally is that it will launch tests for a combination of servers

For that, you have to go on [travis website](https://travis-ci.org), connect with your github account, and activate your
mariadb-connector-j repository.
After this step, every push to your repository will launch a travis test.

## Submitting a request

When your repository has the correction/change done, you can submit a pull request by clicking the "Pull request" button
on github.
Please detail the operation done in your request.

## License

Distributed under the terms of the GNU Library or "Lesser" General Public License (LGPL).
