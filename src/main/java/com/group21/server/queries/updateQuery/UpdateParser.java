package com.group21.server.queries.updateQuery;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.server.models.Column;
import com.group21.server.models.DatabaseSite;
import com.group21.server.models.TableInfo;
import com.group21.server.queries.constraints.ConstraintCheck;
import com.group21.utils.RegexUtil;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class UpdateParser
{
    private static final Logger LOGGER = LoggerFactory.getLogger(UpdateParser.class);
    private static final String UPDATE_TABLE_REGEX = "UPDATE [a-zA-Z_]+ NODE (LOCAL|REMOTE)? SET [a-zA-Z_]+ (=) [a-zA-Z0-9_]+;?";
    private static final String UPDATE_TABLE_WHERE_REGEX = "UPDATE [a-zA-Z_]+ NODE (LOCAL|REMOTE)? SET [a-zA-Z_]+ (=) [a-zA-Z0-9_]+ WHERE [a-zA-Z_]+ (=) [a-zA-Z0-9_]+;?";

    public boolean isValid(String query)
    {
        String matchedQuery = RegexUtil.getMatch(query, UPDATE_TABLE_REGEX);
        String matchedWhereQuery = RegexUtil.getMatch(query, UPDATE_TABLE_WHERE_REGEX);
        if (Strings.isBlank(matchedQuery) && Strings.isBlank(matchedWhereQuery)) {
            LOGGER.error("Syntax error in provided update query!");
            return false;
        }
        return true;
    }

    public boolean isWhereConditionExists(String query){
        return query.matches(UPDATE_TABLE_WHERE_REGEX);
    }

    public String getTableName(String query) {
        String tableName = " ";
        int tableNameStartIndex = query.indexOf("UPDATE") + 7;
        int tableNameEndIndex = query.indexOf("NODE") - 1;
        tableName = query.substring(tableNameStartIndex, tableNameEndIndex);
        return tableName;
    }

    public String getDatabaseSite(String query) {
        int indexOfNode = query.indexOf("NODE");
        if (indexOfNode == -1) {
            return "LOCAL";
        }
        int nodeStartIndex = query.indexOf("NODE")+ 5;
        int nodeEndIndex = query.indexOf("SET") - 1;
        return query.substring(nodeStartIndex, nodeEndIndex).trim();
    }


    private String[] getSetParameters(String query)
    {

        if(query.matches(UPDATE_TABLE_WHERE_REGEX)) {
            String setQuery = "";
            int setStartIndex = query.indexOf("SET") + 4;
            int setEndIndex = query.indexOf("WHERE") - 1;
            setQuery = query.substring(setStartIndex, setEndIndex);
            String[] setParameters = setQuery.split(" ");
            return setParameters;
        }
        String [] splitBySet= query.split("SET");
        String [] setParameters = splitBySet[1].trim().split(" ");
        return setParameters;
    }

    private String[] getWhereParameters(String query) {
        String [] splitByWhere = query.split("WHERE");
        String [] whereParameters = splitByWhere[1].trim().split(" ");
        return whereParameters;
    }

    public void updateTableWhere(TableInfo tableInfo, String query, DatabaseSite databaseSite)
    {
        String[] whereParameters = getWhereParameters(query);
        String[] setParameters = getSetParameters(query);
        try {
            List<String> fileLines = databaseSite.readData(tableInfo.getTableName());
            List<String> headers = Arrays.asList(fileLines.get(0).split(ApplicationConfiguration.DELIMITER_REGEX));
            int setHeaderIndex = headers.indexOf(setParameters[0]);
            int whereHeaderIndex = headers.indexOf(whereParameters[0]);
            List<String> conditionalFileLines = new ArrayList<>();
            List<String> nonConditionalLines=new ArrayList<>();
            List<String> primaryIds=new ArrayList<>();
            fileLines.remove(0);

            if (ConstraintCheck.checkQueryConstraints(tableInfo.getTableName(), setParameters[0], setParameters[2].replace(";", ""),databaseSite) &&
                    ConstraintCheck.checkQueryConstraints(tableInfo.getTableName(), whereParameters[0], whereParameters[2].replace(";", ""),databaseSite))
            {
                List<Column> columns = databaseSite.readMetadata(tableInfo.getTableName());

                List<Column> filteredSetColumns =
                        columns.stream().filter(
                                t -> t.getColumnName().equals(setParameters[0])
                        ).collect(Collectors.toList());

                if (filteredSetColumns.get(0).getConstraint().getKeyword().equals("PRIMARY KEY")) {
                    for (String line : fileLines) {
                        primaryIds.add(line.split(ApplicationConfiguration.DELIMITER_REGEX)[setHeaderIndex]);
                    }

                    if (primaryIds.contains(setParameters[2].replace(";", ""))) {
                        LOGGER.error("Primary Key constraint violated! Duplicate primary key");
                        return;
                    }
                }
                if (filteredSetColumns.get(0).getConstraint().getKeyword().equals("FOREIGN KEY") || !ConstraintCheck.checkForeignKeyConstraints(tableInfo.getTableName(), primaryIds, databaseSite)) {
                    if (ConstraintCheck.checkPrimaryKeyConstraints(filteredSetColumns.get(0).getForeignKeyTable(), setParameters[2].replace(";", ""), databaseSite)) {
                        LOGGER.error("Foreign Key constraint violated! Foreign Key " + setParameters[2].replace(";", "") + " Does not exist in " + filteredSetColumns.get(0).getForeignKeyTable());
                        return;
                    }
                }

                for (String line : fileLines) {
                    String[] columnList = line.split(ApplicationConfiguration.DELIMITER_REGEX);
                    if (columnList[whereHeaderIndex].equalsIgnoreCase(whereParameters[2].replace(";", ""))) {
                        //For replacement
                        columnList[setHeaderIndex] = setParameters[2].replace(";", "");
                        conditionalFileLines.add(String.join(ApplicationConfiguration.DELIMITER, columnList));
                    } else {
                        nonConditionalLines.add(String.join(ApplicationConfiguration.DELIMITER, columnList));
                    }
                }

                conditionalFileLines.addAll(nonConditionalLines);
                List<String> columnNames = columns.stream().map(Column::getColumnName).collect(Collectors.toList());
                databaseSite.deleteOnlyTable(tableInfo.getTableName());
                databaseSite.writeData(tableInfo.getTableName(), columnNames);

                for (String line : conditionalFileLines) {
                    databaseSite.writeData(tableInfo.getTableName(), Arrays.asList(line.split(ApplicationConfiguration.DELIMITER_REGEX)));
                }
                LOGGER.info("Updated executed successfully!");
            }
        }
        catch (Exception exception) {
            LOGGER.info("Error occurred while updating the table!");
        }

    }

    public void updateTable(TableInfo tableInfo,String query,DatabaseSite databaseSite) {
        String[] setParameters = getSetParameters(query);
        try {
            List<String> fileLines = databaseSite.readData(tableInfo.getTableName());
            List<String> headers = Arrays.asList(fileLines.get(0).split(ApplicationConfiguration.DELIMITER_REGEX));
            int headerIndex = headers.indexOf(setParameters[0]);
            fileLines.remove(0);
            List<String> writeFileLines = new ArrayList<>();

            if (ConstraintCheck.checkQueryConstraints(tableInfo.getTableName(), setParameters[0], setParameters[2].replace(";", ""),databaseSite))
            {
                List<Column> columns = databaseSite.readMetadata(tableInfo.getTableName());
                List<Column> filteredColumns =
                        columns.stream().filter(
                                t -> t.getColumnName().equals(setParameters[0])
                        ).collect(Collectors.toList());

                if(filteredColumns.get(0).getConstraint().getKeyword().equals("PRIMARY KEY"))
                {
                    LOGGER.error("Primary Key constraint violated! Duplicate primary key");
                    return;
                }
                if(filteredColumns.get(0).getConstraint().getKeyword().equals("FOREIGN KEY")) {
                    if(ConstraintCheck.checkPrimaryKeyConstraints(filteredColumns.get(0).getForeignKeyTable(),setParameters[2].replace(";", ""),databaseSite)){
                        LOGGER.error("Foreign Key constraint violated! Foreign Key "+ setParameters[2].replace(";", "") + " Does not exist in "+ filteredColumns.get(0).getForeignKeyTable());
                        return;
                    }

                }

                for (String line : fileLines)
                {
                    String[] columnList = line.split(ApplicationConfiguration.DELIMITER_REGEX);
                    //For replacement
                    columnList[headerIndex] = setParameters[2].replace(";", "");
                    writeFileLines.add(String.join(ApplicationConfiguration.DELIMITER,columnList));
                }

                List<String> columnNames = columns.stream().map(Column::getColumnName).collect(Collectors.toList());
                databaseSite.deleteOnlyTable(tableInfo.getTableName());
                databaseSite.writeData(tableInfo.getTableName(),columnNames);

                for (String line:writeFileLines){
                    databaseSite.writeData(tableInfo.getTableName(), Arrays.asList(line.split(ApplicationConfiguration.DELIMITER_REGEX)));
                }

                LOGGER.info("Updated executed successfully!");
            }
        } catch (Exception exception) {
            LOGGER.info("Error occurred while updating the table!");
        }

    }
}

