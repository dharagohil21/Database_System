package com.group21.utils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.StringJoiner;

import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.server.models.Column;
import com.group21.server.models.TableInfo;

public class FileWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(FileWriter.class);

    private FileWriter() {
    }

    public static void writeMetadata(String tableName, List<Column> columnDetails) {
        StringBuilder tableMetadata = new StringBuilder(Strings.EMPTY);

        String headerRow = "ColumnName|ColumnType|Constraint|ForeignKeyTable|ForeignKeyColumn";
        tableMetadata.append(headerRow).append(ApplicationConfiguration.NEW_LINE);

        for (Column column : columnDetails) {
            StringJoiner columnEntry = new StringJoiner(ApplicationConfiguration.DELIMITER);

            columnEntry.add(column.getColumnName());
            columnEntry.add(column.getColumnType().name());
            columnEntry.add(column.getConstraint().name());
            columnEntry.add(column.getForeignKeyTable());
            columnEntry.add(column.getForeignKeyColumnName());

            tableMetadata.append(columnEntry.toString()).append(ApplicationConfiguration.NEW_LINE);
        }

        String metadataFileName = tableName + ApplicationConfiguration.METADATA_FILE_FORMAT;

        Path metadataFilePath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + metadataFileName);

        try {
            if (Files.notExists(metadataFilePath)) {
                Files.createFile(metadataFilePath);
            }

            Files.write(metadataFilePath, tableMetadata.toString().getBytes());
        } catch (IOException exception) {
            LOGGER.error("Error occurred while storing table {} metadata.", tableName);
        }
    }

    public static void writeData(String tableName, List<String> columnData) {
        StringJoiner tableDataJoiner = new StringJoiner(ApplicationConfiguration.DELIMITER);

        for (String data : columnData) {
            tableDataJoiner.add(data);
        }

        String tableData = tableDataJoiner.toString() + ApplicationConfiguration.NEW_LINE;

        String dataFileName = tableName + ApplicationConfiguration.DATA_FILE_FORMAT;

        Path metadataFilePath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + dataFileName);

        try {
            if (Files.notExists(metadataFilePath)) {
                Files.createFile(metadataFilePath);
            }

            Files.write(metadataFilePath, tableData.getBytes(), StandardOpenOption.APPEND);
        } catch (IOException exception) {
            LOGGER.error("Error occurred while storing data in table {}.", tableName);
        }
    }

    public static void writeLocalDataDictionary(TableInfo tableInfo) {
        try {
            Path localDDFilePath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + ApplicationConfiguration.LOCAL_DATA_DICTIONARY_NAME);

            StringJoiner tableInfoJoiner = new StringJoiner(ApplicationConfiguration.DELIMITER);
            tableInfoJoiner.add(tableInfo.getTableName());
            tableInfoJoiner.add(String.valueOf(tableInfo.getNumberOfRows()));
            tableInfoJoiner.add(String.valueOf(tableInfo.getCreatedOn()));

            Files.write(localDDFilePath, tableInfoJoiner.toString().getBytes(), StandardOpenOption.APPEND);
        } catch (IOException exception) {
            LOGGER.error("Error occurred while writing to local data dictionary.");
        }
    }
}
