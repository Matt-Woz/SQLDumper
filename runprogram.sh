#!/bin/bash
read -p "Enter name of database to dump with .db: " database
read -p "Enter name of sql file to store with .sql: " statement
javac *.java
java -cp "sqlite-jdbc-3.7.2.jar:." Coursework $database $statement>$statement
echo "finished"