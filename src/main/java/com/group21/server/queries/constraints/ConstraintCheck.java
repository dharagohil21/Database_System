package com.group21.server.queries.constraints;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.constants.CommonRegex;
import com.group21.server.models.*;
import com.group21.utils.RegexUtil;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ConstraintCheck
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ConstraintCheck.class);

    public static boolean checkQueryConstraints(String tableName, String columnName, String columnValue, DatabaseSite databaseSite) {

        List<Column> columnList = databaseSite.readMetadata(tableName);
        List<String> columnNameList = new ArrayList<>();
        Map<String, DataType> columnTypeList = new HashMap<>();
        Map<String, Constraint> columnConstraintList = new HashMap<>();
        Map<String, String> columnForeignKeyTableList = new HashMap<>();
        Map<String, String> columnForeignKeyColumnNameList = new HashMap<>();

        for (Column c : columnList) {
            columnNameList.add(c.getColumnName());
            columnTypeList.put(c.getColumnName(), c.getColumnType());
            columnConstraintList.put(c.getColumnName(), c.getConstraint());
            columnForeignKeyTableList.put(c.getColumnName(), c.getForeignKeyTable());
            columnForeignKeyColumnNameList.put(c.getColumnName(), c.getForeignKeyColumnName());
        }

        if (!columnNameList.contains(columnName)) {
            LOGGER.error("Column Name '{}' does not exist in table", columnName);
            return false;
        }

        DataType columnValueDatatype = columnTypeList.get(columnName);
        if (columnValueDatatype.equals(DataType.INT)) {
            columnValue = RegexUtil.getMatch(columnValue, CommonRegex.INTEGER_REGEX);
        } else if (columnValueDatatype.equals(DataType.DOUBLE)) {
            columnValue = RegexUtil.getMatch(columnValue, CommonRegex.DOUBLE_REGEX);
        } else if (columnValueDatatype.equals(DataType.TEXT)) {
            columnValue = RegexUtil.getMatch(columnValue, CommonRegex.TEXT_REGEX);

            if (columnValue != null) {
                columnValue = columnValue.substring(1, columnValue.length() - 1);
            }
        }
        if (Strings.isBlank(columnValue)) {
            LOGGER.error("Column '{}' requires value of type '{}'", columnName, columnValueDatatype.name());
            return false;
        }

        return true;
    }

    public static boolean checkForeignKeyConstraints(String tableName, List<String> uniqueIds,DatabaseSite databaseSite) {
        List<TableInfo> tableInfoList = databaseSite.readLocalDataDictionary();
        List<String> tableNameList = tableInfoList.stream().map(TableInfo::getTableName).collect(Collectors.toList());
        List<Column> columnNameList;
        tableNameList.remove(tableName);
        //Dict for foreign key check w.r.t tables
        HashMap<String, List<String>> checkTableExists = new HashMap<>();
        for (String table : tableNameList) {
            columnNameList = databaseSite.readMetadata(table);
            for (Column column : columnNameList) {
                if (column.getForeignKeyTable().equals(tableName)) {
                    List<String> violatedIds = checkForeignKeyUniqueIds(uniqueIds, table, column,databaseSite);
                    if (!violatedIds.isEmpty()) {
                        checkTableExists.put(table, violatedIds);
                    }
                }
            }
        }
        if (!checkTableExists.keySet().isEmpty()) {
            for (Map.Entry<String, List<String>> tableDetail : checkTableExists.entrySet()) {
                if (!tableDetail.getValue().isEmpty()) {
                    LOGGER.error("Foreign Key constraint for {} violated for keys {}", tableName, String.join(",", tableDetail.getValue()));
                }
            }
            return true;
        }

        return false;
    }

    private static List<String> checkForeignKeyUniqueIds(List<String> uniqueIds, String tableName, Column column,DatabaseSite databaseSite) {
        List<String> violatedIds = new ArrayList<>();
        List<String> fileLines = databaseSite.readData(tableName);
        fileLines.remove(0);
        for (String line : fileLines) {
            String[] columnList = line.split(ApplicationConfiguration.DELIMITER_REGEX);
            if (uniqueIds.contains(columnList[column.getColumnPosition()])) {
                violatedIds.add(columnList[column.getColumnPosition()]);
            }
        }
        return violatedIds;
    }

    public static boolean checkPrimaryKeyConstraints(String tableName, String uniqueId, DatabaseSite databaseSite) {
        List<Column> columns = databaseSite.readMetadata(tableName);

        List<Column> getPrimaryKeyColumns =
                columns.stream().filter(
                        t -> t.getConstraint().getKeyword().equals("PRIMARY KEY")
                ).collect(Collectors.toList());

        List<String> uniqueIds = new ArrayList<>();
        uniqueIds.add(uniqueId);

        List<String> idsPresent = checkForeignKeyUniqueIds(uniqueIds, tableName, getPrimaryKeyColumns.get(0),databaseSite);
        return idsPresent.isEmpty();

    }
}
