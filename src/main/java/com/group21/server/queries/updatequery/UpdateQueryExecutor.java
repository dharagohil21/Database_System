package com.group21.server.queries.updatequery;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.server.models.DatabaseSite;
import com.group21.server.models.TableInfo;
import com.group21.utils.FileReader;

public class UpdateQueryExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateQueryExecutor.class);

    private final UpdateParser updateQueryParser;

    public UpdateQueryExecutor() {
        this.updateQueryParser = new UpdateParser();
    }

    public void execute(String query, boolean isAutoCommit) {
        boolean isValid = updateQueryParser.isValid(query);
        if (isValid) {
            String tableName = updateQueryParser.getTableName(query);

            Map<String, DatabaseSite> gddMap = FileReader.readDistributedDataDictionary();

            if (gddMap.containsKey(tableName)) {
                DatabaseSite databaseSite = gddMap.get(tableName);

                List<TableInfo> tableInfoList = databaseSite.readLocalDataDictionary();

                for (TableInfo tableInfo : tableInfoList) {
                    if (tableInfo.getTableName().equals(tableName)) {
                        if (updateQueryParser.isWhereConditionExists(query)) {
                            updateQueryParser.updateTableWhere(tableInfo, query, databaseSite, isAutoCommit);
                        } else {
                            updateQueryParser.updateTable(tableInfo, query, databaseSite, isAutoCommit);
                        }
                    }
                }
            } else {
                LOGGER.info("Table '{}' does not exist in database!", tableName);
            }
        }
    }
}

