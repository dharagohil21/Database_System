package com.group21.server.queries.deleteQuery;

import com.group21.configurations.ApplicationConfiguration;
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
import java.util.Arrays;
import java.util.List;

public class DeleteParser
{
    private static final Logger LOGGER = LoggerFactory.getLogger(DeleteParser.class);
    private static final String DELETE_TABLE_REGEX = "DELETE FROM [a-zA-Z_]+;?";
    private static final String DELETE_TABLE_WHERE_REGEX = "DELETE FROM [a-zA-Z_]+ WHERE [a-zA-Z_]+ ([<>=]|[<>]=) [a-zA-Z0-9_]+;?";

    public boolean isValid(String query) {
        String matchedQuery = RegexUtil.getMatch(query, DELETE_TABLE_REGEX);
        String matchedWhereQuery = RegexUtil.getMatch(query, DELETE_TABLE_WHERE_REGEX);
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
                if(query.matches(DELETE_TABLE_REGEX)){
                    deleteTable(tableInfo);
                    break;
                }
                deleteTableWhere(tableInfo,query);
                break;
            }
        }

        if (invalidTableName) {
            LOGGER.error("Table Name '{}' Does not exist ", tableName);
            return false;
        }

        return true;
    }

    public boolean deleteTable(TableInfo tableInfo)  {
        String dataFileName = tableInfo.getTableName() + ApplicationConfiguration.DATA_FILE_FORMAT;
        Path localDDFilePath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + dataFileName);
        try {
            Files.delete(localDDFilePath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        FileReader.readMetadata(tableInfo.getTableName());
        FileWriter.writeData(tableInfo.getTableName(),FileReader.readMetadata(tableInfo.getTableName()));
        LOGGER.info("Delete query executed successfully!");
        return true;
    }

    public String getTableName(String query) {
        if(query.matches(DELETE_TABLE_REGEX)) {
            int tableNameStartIndex = query.indexOf("FROM") + 5;
            int tableNameEndIndex = query.indexOf(";");
            return query.substring(tableNameStartIndex, tableNameEndIndex);
        }
        int tableNameStartIndex = query.indexOf("FROM") + 5;
        int tableNameEndIndex = query.indexOf("WHERE") - 1;
        return query.substring(tableNameStartIndex, tableNameEndIndex);
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
        List<String> fileLines=null;
        try {
            fileLines = Files.readAllLines(localDDFilePath);
            List<String> headers = Arrays.asList(fileLines.get(0).split(ApplicationConfiguration.DELIMITER_REGEX));
            Integer headerIndex = headers.indexOf(whereParameters[0]);
            fileLines.remove(0);
            for (String line : fileLines) {
                String[] columnList = line.split(ApplicationConfiguration.DELIMITER_REGEX);
                if(columnList[headerIndex].equalsIgnoreCase(whereParameters[2].replace(";",""))){
                    fileLines.remove(line);
                    //System.out.println(fileLines);
                    deleteTable(tableInfo);
                    FileWriter.writeData(tableInfo.getTableName(),fileLines);
                }
            }
            } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }
}