package com.group21.server.queries.updateQuery;

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

            List<TableInfo> tableInfoList = FileReader.readLocalDataDictionary();

            for (TableInfo tableInfo : tableInfoList) {
                if (tableInfo.getTableName().equals(tableName)) {
                    invalidTableName = false;
                    if (updateQueryParser.isWhereConditionExists(query)) {
                        updateQueryParser.updateTableWhere(tableInfo, query);
                        break;
                    }
                    updateQueryParser.updateTable(tableInfo, query);
                    break;
                }
            }
            if (invalidTableName) {
                LOGGER.error("Table Name '{}' Does not exist ", tableName);
            }
        }
    }
}
