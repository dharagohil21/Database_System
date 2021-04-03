package com.group21.server.queries.deleteQuery;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.server.models.Column;
import com.group21.server.models.TableInfo;
import com.group21.utils.FileReader;
import com.group21.utils.FileWriter;
import com.group21.utils.RegexUtil;
import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class DeleteParser
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteParser.class);
    private static final String DELETE_TABLE_REGEX = "DELETE FROM [a-zA-Z_]+;?";
    private static final String DELETE_TABLE_WHERE_REGEX = "DELETE FROM [a-zA-Z_]+ WHERE [a-zA-Z_]+ ([<>=]|[<>]=) [a-zA-Z0-9_]+;?";

    public boolean isValid(String query) {
        String matchedQuery = RegexUtil.getMatch(query, DELETE_TABLE_REGEX);
        String matchedWhereQuery = RegexUtil.getMatch(query, DELETE_TABLE_WHERE_REGEX);
        if (Strings.isBlank(matchedQuery) && Strings.isBlank(matchedWhereQuery)) {
            LOGGER.error("Syntax error in provided delete query!");
            return false;
        }

        return true;
    }

    public boolean isWhereConditionExists(String query){
        return !query.matches(DELETE_TABLE_REGEX);
    }

    public boolean deleteTable(TableInfo tableInfo)  {
        String dataFileName = tableInfo.getTableName() + ApplicationConfiguration.DATA_FILE_FORMAT;
        Path localDDFilePath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + dataFileName);
        try {
            List<String> fileLines = Files.readAllLines(localDDFilePath);
            List<String> uniqueIds = new ArrayList<>();
            fileLines.remove(0);
            for (String line : fileLines) {
                String[] columnList = line.split(ApplicationConfiguration.DELIMITER_REGEX);
                uniqueIds.add(columnList[0]);
            }
            if(!FileReader.checkForeignKeyConstraints(tableInfo.getTableName(),uniqueIds))
            {
                Files.delete(localDDFilePath);
                //No Constraints
                List<Column> columns = FileReader.readMetadata(tableInfo.getTableName());
                List<String> columnNames = columns.stream().map(Column::getColumnName).collect(Collectors.toList());
                FileWriter.writeData(tableInfo.getTableName(),columnNames);
                LOGGER.info("Delete executed successfully!");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    public String getTableName(String query) {
        String tableName = " ";

        if (query.matches(DELETE_TABLE_REGEX)) {
            int tableNameStartIndex = query.indexOf("FROM") + 5;
            int tableNameEndIndex = query.indexOf(";");
            tableName = query.substring(tableNameStartIndex, tableNameEndIndex);
        }
        else{
            try{
                int tableNameStartIndex = query.indexOf("FROM") + 5;
                int tableNameEndIndex = query.indexOf("WHERE") - 1;
                tableName = query.substring(tableNameStartIndex, tableNameEndIndex);
            }
            catch (Exception exception){
                LOGGER.error("Missing where condition!");
            }
        }
        return tableName;
    }

    public String[] getWhereParameters(String query){
        String [] splitByWhere = query.split("WHERE");
        String [] whereParameters = splitByWhere[1].trim().split(" ");
        return whereParameters;
    }

    public boolean deleteTableWhere(TableInfo tableInfo,String query){
        String[] whereParameters = getWhereParameters(query);
        String dataFileName = tableInfo.getTableName() + ApplicationConfiguration.DATA_FILE_FORMAT;
        Path localDDFilePath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + dataFileName);
        List<String> fileLines;
        List<String> writeFileLines=new ArrayList<>();
        List<String> uniqueIds = new ArrayList<>();
        List<String> lineIds = new ArrayList<>();

        try {
            fileLines = Files.readAllLines(localDDFilePath);
            List<String> headers = Arrays.asList(fileLines.get(0).split(ApplicationConfiguration.DELIMITER_REGEX));
            Integer headerIndex = headers.indexOf(whereParameters[0]);
            fileLines.remove(0);
            if (FileReader.checkQueryConstraints(tableInfo.getTableName(), whereParameters[0], whereParameters[2].replace(";", "")))
            {
                for (String line : fileLines) {
                    String[] columnList = line.split(ApplicationConfiguration.DELIMITER_REGEX);
                    if (columnList[headerIndex].equalsIgnoreCase(whereParameters[2].replace(";", ""))) {
                        uniqueIds.add(columnList[0]);//0:headerIndex
                    }
                    else{
                        lineIds.add(line);
                    }
                }

                if(!FileReader.checkForeignKeyConstraints(tableInfo.getTableName(),uniqueIds))
                {
                    Files.delete(localDDFilePath);
                    FileWriter.writeData(tableInfo.getTableName(),FileReader.readColumnMetadata(tableInfo.getTableName()));
                    //No Constraints
                    for(String line:lineIds) {
                        writeFileLines.add(line);
                        FileWriter.writeData(tableInfo.getTableName(), Arrays.asList(line.split(ApplicationConfiguration.DELIMITER_REGEX)));
                    }
                    LOGGER.info("Delete executed successfully!");
                }
            }
            }catch (IOException e) {
            LOGGER.error("Error occurred while deleting the table");
        }
        return true;
    }
}