package com.ef;

import java.io.File;
import java.io.IOException;
import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Scanner;

class Log {
    // hey you what's good?  this is highlighted as a change?

    private static final String DELIMETER = "\\|";
    private static final String TABLE_LOG = "logs";
    private static final String TABLE_RESULTS = "results";
    private static final String DROP_TABLE_LOGS = "DROP TABLE logs;";
    private static final String DROP_TABLE_RESULTS = "DROP TABLE results;";
    private static final String CREATE_TABLE_LOGS =
            "CREATE TABLE logs (log_date DATETIME(3), ip VARCHAR (20), request VARCHAR(30), " +
                    "status VARCHAR(3), user_agent VARCHAR (256));";
    private static final String CREATE_TABLE_RESULTS =
            "CREATE TABLE results (ip VARCHAR (20), comments VARCHAR (150));";

    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    private Connection connection;

    // read data from the log file, and save it to the database
    void readDataFromLogFile(String fileName) {
        try (Scanner input = new Scanner(new File(fileName));
             Statement statement = connection.createStatement()) {

            // drop old logs table and create a new one
            if (tableExists(TABLE_LOG)) {
                statement.execute(DROP_TABLE_LOGS);
            }

            statement.execute(CREATE_TABLE_LOGS);

            System.out.println("Loading log to database...");

            while (input.hasNextLine()) {
                String newLine = input.nextLine();
                Scanner s = new Scanner(newLine).useDelimiter(DELIMETER);

                String log_date = s.next();
                String ip = s.next();
                String request = s.next();
                String status = s.next();
                String user_agent = s.next();

                saveDataToLogsTable(log_date, ip, request, status, user_agent);
            }
        } catch (IOException | SQLException e) {
            System.out.println(e);
            System.exit(1);
        }

        System.out.println("File loaded successfully.");
    }

    // save data into the database
    private void saveDataToLogsTable(String log_date, String ip, String request, String status, String user_agent) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO logs VALUES(?, ?, ?, ?, ?)");
            preparedStatement.setTimestamp(1, Timestamp.valueOf(log_date));
            preparedStatement.setString(2, ip);
            preparedStatement.setString(3, request);
            preparedStatement.setString(4, status);
            preparedStatement.setString(5, user_agent);

            preparedStatement.executeUpdate();

        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    // save and print records that match the arguments
    void queryAndSaveResultSet(Map<String, String> argsMap) {

        if (connection != null) {

            try (Statement statement = connection.createStatement()){

                String duration = argsMap.get("duration");
                String startDate = argsMap.get("startDate").replace(".", " "); // remove the period from the input
                String threshold = argsMap.get("threshold");

                // 1. get endDate by adding duration to the start date
                String endDate = getEndDate(duration, startDate);

                System.out.println(String.format("Querying for IPs that made more than %s requests between %s and %s.", threshold, startDate, endDate));

                // 2. drop old results table and create a new one
                if (tableExists(TABLE_RESULTS)) {
                    statement.execute(DROP_TABLE_RESULTS);
                }

                statement.execute(CREATE_TABLE_RESULTS);

                // 3. create sql select query
                String query = String.format("SELECT ip FROM logs WHERE log_date between '%s' and '%s' GROUP BY ip HAVING COUNT(ip) > %s;",
                    startDate, endDate, threshold);

                ResultSet resultSet = statement.executeQuery(query);
                String comments = String.format("IP has been blocked for making more than %s requests between %s and %s", threshold, startDate, endDate);

                // 4. print and save results to the results table
                while (resultSet.next()) {
                    String ip = resultSet.getString("ip");

                    saveDataToResultsTable(ip, comments);
                    System.out.println(ip + "\t" + comments);
                }

            } catch (SQLException e) {
                System.out.println(e);
            }
        }
    }

    private String getEndDate(String duration, String startDate) {

        Timestamp temp = Timestamp.valueOf(startDate);

        LocalDateTime startDateTime = temp.toLocalDateTime();
        LocalDateTime endDateTime = startDateTime;

        if (duration.equalsIgnoreCase("hourly")) {
            endDateTime = startDateTime.plusHours(1);
        } else  if (duration.equalsIgnoreCase("daily")) {
            endDateTime = startDateTime.plusHours(24);
        } else {
            System.out.println("The duration entered was invalid.  Please try again.");
            System.exit(0);
        }

        String endDate = formatter.format(endDateTime);

        return endDate;
    }

    // save data into the database
    private void saveDataToResultsTable(String ip, String comments) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement("INSERT INTO results VALUES(?, ?)");
            preparedStatement.setString(1, ip);
            preparedStatement.setString(2, comments);

            preparedStatement.executeUpdate();

        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    // create a connection to the database if one doesn't exist
    void connectDB() {
        if (connection == null) {
            try {
                Class.forName("com.mysql.jdbc.Driver");
                connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/logs?useSSL=false", "root", "password");
            } catch (SQLException | ClassNotFoundException e) {

                System.out.println(e);
                System.exit(1);
            }
        }
    }

    // close db connection
    void closeDB() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            System.out.println(e);
        }

    }

    // returns true iF the table name exists in the connected db
    private boolean tableExists(String tableName) throws SQLException {
        boolean tableExists = false;

        try (ResultSet resultSet = connection.getMetaData().getTables(null, null, tableName, null)) {

            while (resultSet.next()) {

                String tName = resultSet.getString("TABLE_NAME");

                if (tName != null && tName.equals(tableName)) {
                    tableExists = true;
                    break;
                }
            }
        }
        return tableExists;
    }
}
 // possible performance enhancements:
    // 1. share prepared statement and batch saves to db // statement.addBatch()
    // 1b. and/or we could turn off auto-commit and only commit the transaction when all batches are finished
