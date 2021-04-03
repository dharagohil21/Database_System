package com.group21.server.queries.deleteQuery;

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

public class DeleteParser
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteParser.class);
    private static final String DELETE_TABLE_REGEX = "DELETE FROM [a-zA-Z_]+ NODE (LOCAL|REMOTE)?;?";
    private static final String DELETE_TABLE_WHERE_REGEX = "DELETE FROM [a-zA-Z_]+ NODE (LOCAL|REMOTE)? WHERE [a-zA-Z_]+ (=) [a-zA-Z0-9_]+;?";

    public boolean isValid(String query)
    {
        String matchedQuery = RegexUtil.getMatch(query, DELETE_TABLE_REGEX);
        String matchedWhereQuery = RegexUtil.getMatch(query, DELETE_TABLE_WHERE_REGEX);
        if (Strings.isBlank(matchedQuery) && Strings.isBlank(matchedWhereQuery)) {
            LOGGER.error("Syntax error in provided delete query!");
            return false;
        }
        return true;
    }

    public boolean isWhereConditionExists(String query){
        return query.matches(DELETE_TABLE_WHERE_REGEX);
    }

    public String getDatabaseSite(String query) {
        int indexOfNode = query.indexOf("NODE");
        if (indexOfNode == -1) {
            return "LOCAL";
        }
        if(query.matches(DELETE_TABLE_REGEX)) {
            int nodeStartIndex = query.indexOf("NODE") + 5;
            int nodeEndIndex = query.indexOf(";") - 1;
            return query.substring(nodeStartIndex, nodeEndIndex).trim();
        }
        else{
            int nodeStartIndex = query.indexOf("NODE") + 5;
            int nodeEndIndex = query.indexOf("WHERE") - 1;
            return query.substring(nodeStartIndex, nodeEndIndex).trim();
        }

    }

    public String getTableName(String query) {
        String tableName = " ";

        int tableNameStartIndex = query.indexOf("FROM") + 5;
        int tableNameEndIndex = query.indexOf("NODE") - 1;
        tableName = query.substring(tableNameStartIndex, tableNameEndIndex);
        return tableName;
    }

    public boolean deleteTable(TableInfo tableInfo, DatabaseSite databaseSite)  {
         try {
            List<String> fileLines = databaseSite.readData(tableInfo.getTableName());
            List<String> uniqueIds = new ArrayList<>();
            fileLines.remove(0);
            for (String line : fileLines) {
                String[] columnList = line.split(ApplicationConfiguration.DELIMITER_REGEX);
                uniqueIds.add(columnList[0]);
            }
            if(!ConstraintCheck.checkForeignKeyConstraints(tableInfo.getTableName(),uniqueIds,databaseSite))
            {
                databaseSite.deleteOnlyTable(tableInfo.getTableName());
                //No Constraints
                List<Column> columns = databaseSite.readMetadata(tableInfo.getTableName());
                List<String> columnNames = columns.stream().map(Column::getColumnName).collect(Collectors.toList());
                databaseSite.writeData(tableInfo.getTableName(),columnNames);
                LOGGER.info("Delete executed successfully!");
            }

        } catch (Exception e) {
            LOGGER.error("Error occurred while deleting the table");
        }
        return true;
    }


    public String[] getWhereParameters(String query){
        String [] splitByWhere = query.split("WHERE");
        String [] whereParameters = splitByWhere[1].trim().split(" ");
        return whereParameters;
    }

    public boolean deleteTableWhere(TableInfo tableInfo,String query,DatabaseSite databaseSite){
        String[] whereParameters = getWhereParameters(query);
        List<String> fileLines;
        List<String> writeFileLines = new ArrayList<>();
        List<String> uniqueIds = new ArrayList<>();
        List<String> lineIds = new ArrayList<>();

        try {
            fileLines = databaseSite.readData(tableInfo.getTableName());
            List<String> headers = Arrays.asList(fileLines.get(0).split(ApplicationConfiguration.DELIMITER_REGEX));
            Integer headerIndex = headers.indexOf(whereParameters[0]);
            fileLines.remove(0);
            if (ConstraintCheck.checkQueryConstraints(tableInfo.getTableName(), whereParameters[0], whereParameters[2].replace(";", ""),databaseSite))
            {
                for (String line : fileLines) {
                    String[] columnList = line.split(ApplicationConfiguration.DELIMITER_REGEX);
                    if (columnList[headerIndex].equalsIgnoreCase(whereParameters[2].replace(";", ""))) {
                        uniqueIds.add(columnList[0]);
                    }
                    else{
                        lineIds.add(line);
                    }
                }

                if(!ConstraintCheck.checkForeignKeyConstraints(tableInfo.getTableName(),uniqueIds,databaseSite))
                {
                    databaseSite.deleteOnlyTable(tableInfo.getTableName());
                    databaseSite.writeData(tableInfo.getTableName(),databaseSite.readColumnMetadata(tableInfo.getTableName()));
                    //No Constraints
                    for(String line:lineIds) {
                        writeFileLines.add(line);
                        databaseSite.writeData(tableInfo.getTableName(), Arrays.asList(line.split(ApplicationConfiguration.DELIMITER_REGEX)));
                    }
                    LOGGER.info("Delete executed successfully!");
                }
            }
            }catch (Exception e) {
            LOGGER.error("Error occurred while deleting the table");
        }
        return true;
    }
}