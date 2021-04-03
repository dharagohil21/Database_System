package com.group21.client;

import java.util.Scanner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.server.authentication.Authentication;
import com.group21.server.processor.QueryProcessor;
import com.group21.server.sqlDump.CreateSqlDump;
import com.group21.server.queries.erd.ERDGenerator;
import com.group21.utils.RemoteDatabaseConnection;
import com.group21.utils.RemoteDatabaseWriter;

public class DDBMSClient {
    private static final Logger LOGGER = LoggerFactory.getLogger(DDBMSClient.class);

    public static void main(String[] args) {
        LOGGER.info("   .-----------------------------.                  ");
        LOGGER.info("   |  Welcome to Group 21 DDBMS  |                  ");
        LOGGER.info("   '-----------------------------'                  ");
        LOGGER.info("                  |                                 ");
        LOGGER.info("                  |                                 ");
        LOGGER.info("                  |                 (\\_/)          ");
        LOGGER.info("                  '---------------- (O.O)           ");
        LOGGER.info("                                    (> <)           ");
        LOGGER.info("");

        DDBMSSetup.perform();

        Scanner scanner = new Scanner(System.in);

        LOGGER.info("Enter Username : ");
        String username = scanner.nextLine().trim();

        LOGGER.info("Enter Password : ");
        String password = scanner.nextLine().trim();

        Authentication authentication = new Authentication();
        boolean isValidUser = authentication.login(username, password);

        if (!isValidUser) {
            LOGGER.error("Invalid username or password.");
            return;
        }

        LOGGER.info("");

        while (true) {
            try {
                LOGGER.info("DDBMS>>");

                String userInput = scanner.nextLine();
                String command;

                if (userInput.matches("^export sqldump")) {
                    command = "sqldump";
                } else if (userInput.matches("^export erd;?$")) {
                    command = "erd";
                } else {
                    command = userInput.trim();
                }

                switch (command) {
                    case "":
                        break;
                    case "help":
                        LOGGER.info("Below are some available options:");
                        LOGGER.info("\texport sqldump - To get table structure DDLs");
                        LOGGER.info("\terd <DATABASE_NAME>      - To get Textual ER Diagram");
                        LOGGER.info("\tValid SQL Query          - To execute valid SQL queries");
                        LOGGER.info("\texit                     - To exit DDBMS client");
                        break;
                    case "sqldump":
                    	CreateSqlDump.process(command);
                        //LOGGER.info("SQL Dump is exported successfully.");
                        break;
                    case "erd":
                        ERDGenerator.generate();
                        break;
                    case "exit":
                        RemoteDatabaseConnection.closeSession();
                        System.exit(0);
                        return;
                    default:
                        QueryProcessor.process(command);
                        RemoteDatabaseWriter.syncDistributedDataDictionary();
                        break;
                }
                LOGGER.info("");
            } catch (Exception e) {
                LOGGER.error("Error occurred while execution : ", e);
                return;
            }
        }
    }
}
