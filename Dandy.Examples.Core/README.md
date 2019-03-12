Dandy.Examples.Core
===========================================

It is possible to run this project on NetCore and host the DB2 database in a Docker container.

Install and run DB2 Docker container
https://hub.docker.com/_/db2-developer-c-edition

Update the `ConnectionString` in the code with the parameters provided while configuring the DB2 instance

`Server=127.0.0.1:50000; Database=TESTDB; UID=DB2INST1; PWD=password`

