package com.group21.server.queries.insert;

import com.group21.utils.FileWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


public class InsertQueryExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(InsertQueryExecutor.class);

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
