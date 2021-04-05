package com.group21.server.queries.deletequery;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.server.models.DatabaseSite;
import com.group21.server.models.TableInfo;
import com.group21.utils.FileReader;

public class DeleteQueryExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteQueryExecutor.class);

    private final DeleteParser deleteQueryParser;

    public DeleteQueryExecutor() {
        this.deleteQueryParser = new DeleteParser();
    }

    public void execute(String query) {
        boolean isValid = deleteQueryParser.isValid(query);
        if (isValid) {
            String tableName = deleteQueryParser.getTableName(query);

            Map<String, DatabaseSite> gddMap = FileReader.readDistributedDataDictionary();

            if (gddMap.containsKey(tableName)) {
                DatabaseSite databaseSite = gddMap.get(tableName);

                List<TableInfo> tableInfoList = databaseSite.readLocalDataDictionary();

                for (TableInfo tableInfo : tableInfoList) {
                    if (tableInfo.getTableName().equals(tableName)) {
                        if (deleteQueryParser.isWhereConditionExists(query)) {
                            deleteQueryParser.deleteTableWhere(tableInfo, query, databaseSite);
                        } else {
                            deleteQueryParser.deleteTable(tableInfo, databaseSite);
                        }
                    }
                }
            } else {
                LOGGER.info("Table '{}' does not exist in database!", tableName);
            }
        }
    }
}
