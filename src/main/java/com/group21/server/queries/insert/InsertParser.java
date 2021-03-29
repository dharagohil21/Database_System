package com.group21.server.queries.insert;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.server.models.Column;
import com.group21.server.models.Constraint;
import com.group21.server.models.DataType;
import com.group21.server.models.TableInfo;
import com.group21.server.queries.createtable.CreateTableParser;
import com.group21.utils.FileReader;
import com.group21.utils.RegexUtil;

public class InsertParser {

    private static final Logger LOGGER = LoggerFactory.getLogger(InsertParser.class);

    private static final String INSERT_REGEX_TYPE1 = "^INSERT INTO [a-zA-Z_]* \\(.*\\) VALUES \\(.*\\);?$";
    private static final String INSERT_REGEX_TYPE2 = "^INSERT INTO [a-zA-Z_]* VALUES \\(.*\\);?$";
    private static final String INTEGER_REGEX = "^[-]?[0-9]+$";
    private static final String DOUBLE_REGEX = "^[-]?[0-9]+(\\.[0-9]+)?$";
    private static final String TEXT_REGEX = "^['\"].*['\"]$";


    public boolean isValid(String query) {
        String matchedQueryType1 = RegexUtil.getMatch(query, INSERT_REGEX_TYPE1);
        String matchedQueryType2 = RegexUtil.getMatch(query, INSERT_REGEX_TYPE2);

        if (Strings.isBlank(matchedQueryType1) && Strings.isBlank(matchedQueryType2)) {
            LOGGER.error("Syntax error in provided insert into table query.");
            return false;
        }

        boolean invalidTableName = true;

        String tableName = getTableName(query);

        List<TableInfo> tableInfoList = FileReader.readLocalDataDictionary();

        for (TableInfo tableInfo : tableInfoList) {
            if (tableInfo.getTableName().equals(tableName)) {
                invalidTableName = false;
                break;
            }
        }

        if (invalidTableName) {
            LOGGER.error("Table Name '{}' Does not exist ", tableName);
            return false;
        }

        int firstColumnValueBracketIndex = query.indexOf("VALUES") + 7;
        int lastColumnValueBracketIndex = query.indexOf(')', firstColumnValueBracketIndex);

        String columnValueString = query.substring(firstColumnValueBracketIndex + 1, lastColumnValueBracketIndex);

        if (Strings.isBlank(columnValueString)) {
            LOGGER.error("Column Values - 'Missing' in provided insert into table query.");
            return false;
        }

        String[] columnValueArray = columnValueString.split(",");
        String[] columnNameArray;

        List<Column> columnList = FileReader.readMetadata(tableName);

        if (Strings.isBlank(matchedQueryType2)) {
            int firstColumnNameBracketIndex = query.indexOf('(');
            int lastColumnNameBracketIndex = query.indexOf(')');

            String columnNameString = query.substring(firstColumnNameBracketIndex + 1, lastColumnNameBracketIndex);

            if (Strings.isBlank(columnNameString)) {
                LOGGER.error("Column Names - 'Missing' in provided insert into table query.");
                return false;
            }

            columnNameArray = columnNameString.split(",");

            if (columnNameArray.length != columnValueArray.length) {
                LOGGER.error("Number of columns and values mismatch");
                return false;
            }

            List<String> primaryKeyColumnNames = new ArrayList<>();
            for (Column c : columnList) {
                if (c.getConstraint().equals(Constraint.PRIMARY_KEY)) {
                    primaryKeyColumnNames.add(c.getColumnName());
                }
            }

            for (String p : primaryKeyColumnNames) {
                boolean doesNotContainKey = true;
                for (String columnName : columnNameArray) {
                    if (columnName.trim().equals(p)) {
                        doesNotContainKey = false;
                        break;
                    }
                }
                if (doesNotContainKey) {
                    LOGGER.error("Primary Key Value is Missing");
                    return false;
                }
            }

        } else {

            columnNameArray = new String[columnList.size()];

            for (int i = 0; i < columnList.size(); i++) {
                columnNameArray[i] = columnList.get(i).getColumnName();
            }
        }
        return checkConstraints(tableName, columnNameArray, columnValueArray);
    }

    private boolean checkConstraints(String tableName, String[] columnNameArray, String[] columnValueArray) {

        int columnLength = columnNameArray.length;

        List<Column> columnList = FileReader.readMetadata(tableName);
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

        for (int i = 0; i < columnLength; i++) {
            String columnName = columnNameArray[i].trim();
            String columnValue = columnValueArray[i].trim();

            if (!columnNameList.contains(columnName)) {
                LOGGER.error("Column Name '{}' does not exist in table", columnName);
                return false;
            }

            DataType columnValueDatatype = columnTypeList.get(columnName);
            if (columnValueDatatype.equals(DataType.INT)) {
                columnValue = RegexUtil.getMatch(columnValue, INTEGER_REGEX);
            } else if (columnValueDatatype.equals(DataType.DOUBLE)) {
                columnValue = RegexUtil.getMatch(columnValue, DOUBLE_REGEX);
            } else if (columnValueDatatype.equals(DataType.TEXT)) {
                columnValue = RegexUtil.getMatch(columnValue, TEXT_REGEX);

                if (columnValue != null) {
                    columnValue = columnValue.substring(1, columnValue.length() - 1);
                }
            }

            if (Strings.isBlank(columnValue)) {
                LOGGER.error("Column '{}' requires value of type '{}'", columnName, columnValueDatatype.name());
                return false;
            }

            Constraint columnValueConstraint = columnConstraintList.get(columnName);
            if (columnValueConstraint.equals(Constraint.PRIMARY_KEY)) {
                List<String> primaryKeyValueList = FileReader.readColumnData(tableName, columnName);

                if (primaryKeyValueList.contains(columnValue)) {
                    LOGGER.error("Primary Key Constraint Violated");
                    return false;
                }
            } else if (columnValueConstraint.equals(Constraint.FOREIGN_KEY)) {
                String foreignKeyTable = columnForeignKeyTableList.get(columnName);
                String foreignKeyColumnName = columnForeignKeyColumnNameList.get(columnName);
                List<String> foreignKeyValueList = FileReader.readColumnData(foreignKeyTable, foreignKeyColumnName);

                if (!foreignKeyValueList.contains(columnValue)) {
                    LOGGER.error("Foreign Key Constraint Violated");
                    return false;
                }
            }
        }
        return true;
    }

    public String getTableName(String query) {
        int tableNameStartIndex = query.indexOf("INTO") + 5;
        int tableNameEndIndex = query.indexOf(' ', tableNameStartIndex);
        return query.substring(tableNameStartIndex, tableNameEndIndex);
    }

    public List<String> getColumnValues(String query, String tableName) {
        String matchedQueryType2 = RegexUtil.getMatch(query, INSERT_REGEX_TYPE2);

        List<String> columnValues = new ArrayList<>();

        int firstColumnValueBracketIndex = query.indexOf("VALUES") + 7;
        int lastColumnValueBracketIndex = query.indexOf(')', firstColumnValueBracketIndex);

        String columnValueString = query.substring(firstColumnValueBracketIndex + 1, lastColumnValueBracketIndex);
        String[] columnValueArray = columnValueString.split(",");

        List<Column> columnList = FileReader.readMetadata(tableName);
        Map<String, DataType> columnNameType = new LinkedHashMap<>();
        for (Column c : columnList) {
            columnNameType.put(c.getColumnName(), c.getColumnType());
        }

        if (Strings.isBlank(matchedQueryType2)) {
            int firstColumnNameBracketIndex = query.indexOf('(');
            int lastColumnNameBracketIndex = query.indexOf(')');
            String columnNameString = query.substring(firstColumnNameBracketIndex + 1, lastColumnNameBracketIndex);
            String[] columnNameArray = columnNameString.split(",");

            Map<String, String> columnNameValue = new LinkedHashMap<>();
            for (int i = 0; i < columnNameArray.length; i++) {
                columnNameValue.put(columnNameArray[i].trim(), columnValueArray[i].trim());
            }
            for (String name : columnNameType.keySet()) {
                if (columnNameValue.containsKey(name)) {
                    String value = columnNameValue.get(name).trim();
                    if (columnNameType.get(name).equals(DataType.TEXT)) {
                        value = value.substring(1, value.length() - 1);
                    }
                    columnValues.add(value);
                } else {
                    columnValues.add("null");
                }
            }
        } else {
            int i = 0;
            for (String name : columnNameType.keySet()) {
                String value = columnValueArray[i++].trim();
                if (columnNameType.get(name).equals(DataType.TEXT)) {
                    value = value.substring(1, value.length() - 1);
                }
                columnValues.add(value);
            }
        }
        return columnValues;
    }
}
