#!/bin/bash	
echo "Welcome - Compiling code"	
rm ./*.class	
javac -cp "../sqlite-jdbc-3.7.2.jar:." ./Coursework.java DatabaseDumper.java DatabaseDumper201.java	
echo "Running.."	
ls	
java -cp "../sqlite-jdbc-3.7.2.jar:." Coursework "$1" > "$2" 