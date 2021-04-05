package com.group21.server.queries.insert;

import com.group21.server.models.DatabaseSite;

import java.util.List;


public class InsertQueryExecutor {

    private final InsertParser insertParser;

    public InsertQueryExecutor() {
        this.insertParser = new InsertParser();
    }

    public void execute(String query) {
        boolean isQueryValid = insertParser.isValid(query);

        if (isQueryValid) {
            String tableName = insertParser.getTableName(query);
            DatabaseSite databaseSite = insertParser.getDatabaseSite(tableName);
            List<String> columnValues = insertParser.getColumnValues(query, tableName);
            databaseSite.writeData(tableName, columnValues);
            databaseSite.incrementRowCountInLocalDataDictionary(tableName);
        }
    }
}