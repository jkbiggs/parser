# SQL SCHEMA and TESTS

#SCHEMA
CREATE DATABASE logs;
CREATE TABLE logs (log_date DATETIME(3), ip VARCHAR (20), request VARCHAR(30), status VARCHAR(3), user_agent VARCHAR (256));


#TESTING

#TEST CASE 1:	java -cp "parser.jar" com.ef.Parser --startDate=2017-01-01.15:00:00 --duration=hourly --threshold=200
SELECT ip  FROM logs WHERE log_date between '2017-01-01 15:00:00' and '2017-01-01 16:00:00'  
GROUP BY ip HAVING COUNT(ip) > 200;

#TEST CASE 2: 	java -cp "parser.jar" com.ef.Parser --startDate=2017-01-01.00:00:00 --duration=daily --threshold=500
SELECT ip  FROM logs WHERE log_date between '2017-01-01.00:00:00' and '2017-01-02.00:00:00'  
GROUP BY ip HAVING COUNT(ip) > 500;