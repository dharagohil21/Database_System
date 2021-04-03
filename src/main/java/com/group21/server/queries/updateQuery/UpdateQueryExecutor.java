package com.group21.server.queries.updateQuery;

import com.group21.server.models.DatabaseSite;
import com.group21.server.models.TableInfo;
import com.group21.utils.FileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class UpdateQueryExecutor {
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateQueryExecutor.class);

    private UpdateParser updateQueryParser;

    public UpdateQueryExecutor() {
        this.updateQueryParser = new UpdateParser();
    }

    public void execute(String query) {
        boolean isValid = updateQueryParser.isValid(query);
        if (isValid) {
            boolean invalidTableName = true;

            String tableName = updateQueryParser.getTableName(query);
            DatabaseSite databaseSite = DatabaseSite.from(updateQueryParser.getDatabaseSite(query));

            List<TableInfo> tableInfoList = databaseSite.readLocalDataDictionary();

            for (TableInfo tableInfo : tableInfoList) {
                if (tableInfo.getTableName().equals(tableName)) {
                    invalidTableName = false;
                    if (updateQueryParser.isWhereConditionExists(query)) {
                        updateQueryParser.updateTableWhere(tableInfo, query,databaseSite);
                        break;
                    }
                    updateQueryParser.updateTable(tableInfo, query,databaseSite);
                    break;
                }
            }
            if (invalidTableName) {
                LOGGER.error("Table Name '{}' Does not exist ", tableName);
            }
        }
    }
}

