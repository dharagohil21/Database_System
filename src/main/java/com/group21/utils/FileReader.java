package com.group21.utils;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.server.models.Column;
import com.group21.server.models.Constraint;
import com.group21.server.models.DataType;
import com.group21.server.models.TableInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class FileReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileReader.class);

    private FileReader() {
    }

    public static Map<String, String> readAuthenticationFile() {
        String authenticationFileName = ApplicationConfiguration.AUTHENTICATION_FILE_NAME;

        Map<String, String> authenticationMap = new HashMap<>();
        try {
            URL authenticationFileUrl = FileReader.class.getClassLoader().getResource(authenticationFileName);

            assert authenticationFileUrl != null;

            Path authenticationFilePath = Paths.get(authenticationFileUrl.toURI());

            List<String> fileLines = Files.readAllLines(authenticationFilePath);

            for (String line : fileLines) {
                String[] columnList = line.split(ApplicationConfiguration.DELIMITER_REGEX);
                String username = columnList[0];
                String password = columnList[1];

                authenticationMap.put(username, password);
            }
        } catch (URISyntaxException | IOException exception) {
            LOGGER.error("Error occurred while reading authentication file.");
        }
        return authenticationMap;
    }

    public static List<TableInfo> readLocalDataDictionary() {
        List<TableInfo> tableInfoList = new ArrayList<>();
        try {
            Path localDDFilePath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + ApplicationConfiguration.LOCAL_DATA_DICTIONARY_NAME);
            List<String> fileLines = Files.readAllLines(localDDFilePath);
            fileLines.remove(0);

            for (String line : fileLines) {
                String[] columnList = line.split(ApplicationConfiguration.DELIMITER_REGEX);

                TableInfo tableInfo = new TableInfo();
                tableInfo.setTableName(columnList[0]);
                tableInfo.setNumberOfRows(Integer.parseInt(columnList[1]));
                tableInfo.setCreatedOn(Long.parseLong(columnList[2]));

                tableInfoList.add(tableInfo);
            }
        } catch (IOException exception) {
            LOGGER.error("Error occurred while reading local data dictionary.");
        }
        return tableInfoList;
    }

    public static List<String>  readColumnMetadata(String tableName) {
        String metadataFileName = tableName + ApplicationConfiguration.METADATA_FILE_FORMAT;
        Path localDDFilePath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + metadataFileName);
        List<String> fileLines = null;
        List<String> columnNames = new ArrayList<>();
        try {
            fileLines = Files.readAllLines(localDDFilePath);
            fileLines.remove(0);

            for (String line : fileLines) {
                String[] columnList = line.split(ApplicationConfiguration.DELIMITER_REGEX);
                columnNames.add(columnList[0]);
            }
            }catch (IOException e) {
                LOGGER.error("Error occurred while reading column name meta data");
            }
        return columnNames;
    }

    public static List<Column> readMetadata(String tableName) {
        List<Column> columnInfoList = new ArrayList<>();
        try {
            Path localDDFilePath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + tableName + ApplicationConfiguration.METADATA_FILE_FORMAT);
            List<String> fileLines = Files.readAllLines(localDDFilePath);
            fileLines.remove(0);

            for (String line : fileLines) {
                String[] columnInfo = line.split(ApplicationConfiguration.DELIMITER_REGEX);
                Column column = new Column();
                column.setColumnName(columnInfo[0]);
                column.setColumnType(DataType.valueOf(columnInfo[1]));
                column.setConstraint(Constraint.valueOf(columnInfo[2]));
                column.setForeignKeyTable(columnInfo[3]);
                column.setForeignKeyColumnName(columnInfo[4]);

                columnInfoList.add(column);
            }
        } catch (IOException exception) {
            LOGGER.error("Error occurred while reading metadata.");
        }
        return columnInfoList;
    }

    public static List<String> readColumnData(String tableName, String columnName) {
        List<String> columnDataList = new ArrayList<>();
        try {
            Path localDDFilePath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + tableName + ApplicationConfiguration.DATA_FILE_FORMAT);
            List<String> fileLines = Files.readAllLines(localDDFilePath);

            int index = 0;
            String firstLine = fileLines.get(0);
            String[] firstLineArray = firstLine.split(ApplicationConfiguration.DELIMITER_REGEX);

            for (int i = 0; i < firstLineArray.length; i++) {
                if (columnName.equals(firstLineArray[i])) {
                    index = i;
                    break;
                }
            }

            fileLines.remove(0);

            for (String line : fileLines) {
                String[] columnData = line.split(ApplicationConfiguration.DELIMITER_REGEX);
                columnDataList.add(columnData[index]);
            }

        } catch (IOException exception) {
            LOGGER.error("Error occurred while reading column data.");
        }
        return columnDataList;
    }
}
