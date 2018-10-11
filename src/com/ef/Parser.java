package com.ef;

import java.util.HashMap;
import java.util.Map;

/*
    Parser is a command line tool for returning how many and which ip addresses are blocked
    for submitting too many requests in a certain about of time.
    The program will parse the given log file,
    save it into a local mySQL database,
    query the result set, then print and save the results of the query
    to another table called "results" in the db
 */
public class Parser {

    public static void main(String[] args) {

        // 1. parse and validate args
        Map<String, String> argsMap = parseArgs(args);
        validateArgs(argsMap);

        // 2. read, parse and save log to the db
        String fileName = argsMap.get("accesslog"); //src/com/resources/access.log"
        String debugMode = argsMap.get("debug");

        if (fileName != null && (debugMode == null || debugMode.equalsIgnoreCase("false"))) {
            readLogAndSaveToDB(fileName);
        }

        // 3. query the db, then save and print the result set
        queryDB(argsMap);

    }
    // store all our args in a map with key:value pair for easy retrieval
    /*
        Expected arguments:
        startDate : the first date in the timeframe window to include in the query
        duration  : the window to query - hourly or daily
        threshold : the number of requests made by a specific ip
        debug     : if true, we will go straight to querying the database
     */
    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> argsMap = new HashMap<>();
        for (String arg: args) {
            String[] parts = arg.replaceAll("--", "").split("="); // remove the '--'
            argsMap.put(parts[0], parts[1]); // save left side as key, right side as value
        }
        return argsMap;
    }

    // confirm that required arguments were given
    // if this was a production or supported app, we'd add format validation to each field, respectively
    private static void validateArgs(Map<String, String> argsMap) {
        if (!argsMap.containsKey("accesslog")) {
            System.out.println("Warning - failure to provide an accesslog path could result in stale data");
        }
        if (!argsMap.containsKey("startDate")) {
            System.out.println("Please provide a start date. (Date Format: \"yyyy-MM-dd HH:mm:ss.SSS\")");
            System.exit(0);
        }
        if (!argsMap.containsKey("duration")) {
            System.out.println("Please provide a duration. (hourly or daily)");
            System.exit(0);
        }
        if (!argsMap.containsKey("threshold")) {
            System.out.println("Please provide a threshold. (limit of requests)");
            System.exit(0);
        }
    }

    private static void readLogAndSaveToDB(String fileName) {
        Log log = new Log();

        log.connectDB();
        log.readDataFromLogFile(fileName);
        log.closeDB();
    }

    private static void queryDB(Map<String, String> argsMap) {
        Log log = new Log();

        log.connectDB();
        log.queryAndSaveResultSet(argsMap);
        log.closeDB();
    }
}
