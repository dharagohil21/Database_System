package com.group21.server.queries.updateQuery;

import com.group21.server.models.TableInfo;
import com.group21.utils.FileReader;
import com.group21.utils.RegexUtil;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class UpdateQueryParser
{
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateQueryParser.class);
    private static final String UPDATE_TABLE_REGEX = "UPDATE [a-zA-Z_]+ SET [a-zA-Z_]+ ([<>=]|[<>]=);?";
    private static final String UPDATE_TABLE_WHERE_REGEX = "UPDATE [a-zA-Z_]+ SET [a-zA-Z_]+ ([<>=]|[<>]=) WHERE [a-zA-Z_]+ ([<>=]|[<>]=) [a-zA-Z0-9_]+;?";


    public boolean isValid(String query){
        String matchedQuery = RegexUtil.getMatch(query, UPDATE_TABLE_REGEX);
        String matchedWhereQuery = RegexUtil.getMatch(query, UPDATE_TABLE_WHERE_REGEX);
        if (Strings.isBlank(matchedQuery) && Strings.isBlank(matchedWhereQuery)) {
            LOGGER.error("Syntax error in provided delete query.");
            return false;
        }
        boolean invalidTableName = true;

        String tableName = getTableName(query);

        List<TableInfo> tableInfoList = FileReader.readLocalDataDictionary();

        for (TableInfo tableInfo : tableInfoList) {
            if (tableInfo.getTableName().equals(tableName)) {
                invalidTableName = false;
                if(query.matches(UPDATE_TABLE_REGEX)){
                    updateTable(tableInfo);
                    break;
                }
                updateTableWhere(tableInfo,query);
                break;
            }
        }
        if (invalidTableName) {
            LOGGER.error("Table Name '{}' Does not exist ", tableName);
            return false;
        }

        return true;

    }

    private void updateTableWhere(TableInfo tableInfo, String query) {
    }

    private void updateTable(TableInfo tableInfo) {
    }

    private String getTableName(String query) {
        return " ";
    }
}
