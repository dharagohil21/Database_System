package com.group21.server.queries.deleteQuery;

import com.group21.server.models.DatabaseSite;
import com.group21.server.models.TableInfo;
import com.group21.utils.FileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class DeleteQueryExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteQueryExecutor.class);

    private DeleteParser deleteQueryParser;

    public DeleteQueryExecutor(){
        this.deleteQueryParser = new DeleteParser();
    }

    public void execute(String query)
    {
        boolean isValid = deleteQueryParser.isValid(query);
        if(isValid) {
            boolean invalidTableName = true;

            String tableName = deleteQueryParser.getTableName(query);
            DatabaseSite databaseSite = DatabaseSite.from(deleteQueryParser.getDatabaseSite(query));

            List<TableInfo> tableInfoList = databaseSite.readLocalDataDictionary();

            for (TableInfo tableInfo : tableInfoList) {
                if (tableInfo.getTableName().equals(tableName)) {
                    invalidTableName = false;
                    if (deleteQueryParser.isWhereConditionExists(query)) {
                        deleteQueryParser.deleteTableWhere(tableInfo, query,databaseSite);
                        break;
                    }
                    deleteQueryParser.deleteTable(tableInfo,databaseSite);
                    break;
                }
            }

            if (invalidTableName) {
                LOGGER.error("Table Name '{}' Does not exist ", tableName);
            }
        }
    }
}
