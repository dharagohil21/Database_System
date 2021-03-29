package com.group21.server.queries.insert;

import java.util.List;

import com.group21.utils.FileWriter;

public class InsertQueryExecutor {

    private final InsertParser insertParser;

    public InsertQueryExecutor() {
        this.insertParser = new InsertParser();
    }

    public void execute(String query) {
        boolean isQueryValid = insertParser.isValid(query);

        if (isQueryValid) {
            String tableName = insertParser.getTableName(query);
            List<String> columnValues = insertParser.getColumnValues(query, tableName);
            FileWriter.writeData(tableName, columnValues);
        }
    }
}
