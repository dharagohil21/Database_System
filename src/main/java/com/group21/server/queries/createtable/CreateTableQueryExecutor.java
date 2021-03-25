package com.group21.server.queries.createtable;

import java.util.List;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.server.models.Column;
import com.group21.server.models.TableInfo;
import com.group21.utils.FileReader;
import com.group21.utils.FileWriter;

public class CreateTableQueryExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableQueryExecutor.class);

    private final CreateTableParser createTableParser;

    public CreateTableQueryExecutor() {
        this.createTableParser = new CreateTableParser();
    }

    public void execute(String query) {
        boolean isQueryValid = createTableParser.isValid(query);

        if (isQueryValid) {
            String tableName = createTableParser.getTableName(query);
            List<Column> columns = createTableParser.getColumns(query);

            List<TableInfo> tableInfoList = FileReader.readLocalDataDictionary();
            List<String> tableNameList = tableInfoList.stream().map(TableInfo::getTableName).collect(Collectors.toList());

            if (tableNameList.contains(tableName)) {
                LOGGER.error("Table '{}' already exists.", tableName);
                return;
            }

            FileWriter.writeMetadata(tableName, columns);

            List<String> tableData = columns.stream().map(Column::getColumnName).collect(Collectors.toList());
            FileWriter.writeData(tableName, tableData);

            TableInfo tableInfo = new TableInfo();
            tableInfo.setTableName(tableName);
            tableInfo.setNumberOfRows(0);
            tableInfo.setCreatedOn(System.currentTimeMillis());

            FileWriter.writeLocalDataDictionary(tableInfo);

            LOGGER.info("Table '{}' created Successfully.", tableName);
        }
    }
}
