package com.group21.utils;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.StringJoiner;
import java.util.UUID;

import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.server.models.Column;
import com.group21.server.models.TableInfo;
import com.jcraft.jsch.ChannelSftp;

public class RemoteDatabaseWriter {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteDatabaseWriter.class);

    private RemoteDatabaseWriter() {
    }

    public static void writeFile(String fileName, String fileContent) {
        try {
            String filePath = ApplicationConfiguration.REMOTE_DB_DATA_DIRECTORY + File.separator + fileName;

            String tempFilePath = ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + UUID.randomUUID().toString() + ".tmp";
            Path tempFile = Paths.get(tempFilePath);
            Files.createFile(tempFile);
            Files.write(tempFile, fileContent.getBytes(), StandardOpenOption.TRUNCATE_EXISTING);

            ChannelSftp sftpChannel = RemoteDatabaseConnection.getSftpChannel();
            sftpChannel.put(tempFilePath, filePath);

            Files.deleteIfExists(tempFile);
        } catch (Exception exception) {
            LOGGER.error("Error occurred while writing file {} to remote server.", fileName);
        }
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

        writeFile(metadataFileName, tableMetadata.toString());
    }

    public static void writeLocalDataDictionary(TableInfo tableInfo) {
        List<TableInfo> tableInfoList = RemoteDatabaseReader.readLocalDataDictionary();
        tableInfoList.add(tableInfo);

        String headerRow = "TableName|NumberOfRows|CreatedOn" + ApplicationConfiguration.NEW_LINE;
        StringBuilder tableInfoDetails = new StringBuilder(headerRow);
        for (TableInfo tableInfoInstance : tableInfoList) {
            StringJoiner tableInfoJoiner = new StringJoiner(ApplicationConfiguration.DELIMITER);
            tableInfoJoiner.add(tableInfoInstance.getTableName());
            tableInfoJoiner.add(String.valueOf(tableInfoInstance.getNumberOfRows()));
            tableInfoJoiner.add(String.valueOf(tableInfoInstance.getCreatedOn()));

            tableInfoDetails.append(tableInfoJoiner.toString()).append(ApplicationConfiguration.NEW_LINE);
        }

        writeFile(ApplicationConfiguration.LOCAL_DATA_DICTIONARY_NAME, tableInfoDetails.toString());
    }

    public static void writeData(String tableName, List<String> columnData) {
        StringJoiner tableDataJoiner = new StringJoiner(ApplicationConfiguration.DELIMITER);

        for (String data : columnData) {
            tableDataJoiner.add(data);
        }

        String tableData = tableDataJoiner.toString() + ApplicationConfiguration.NEW_LINE;

        String dataFileName = tableName + ApplicationConfiguration.DATA_FILE_FORMAT;

        List<String> existingData = RemoteDatabaseReader.readFile(dataFileName);
        existingData.add(tableData);

        writeFile(dataFileName, String.join(ApplicationConfiguration.NEW_LINE, existingData));
    }

    public static void syncDistributedDataDictionary() {
        try {
            Path gddFilePath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + ApplicationConfiguration.DISTRIBUTED_DATA_DICTIONARY_NAME);
            List<String> gddLines = Files.readAllLines(gddFilePath);

            String gddContent = String.join(ApplicationConfiguration.NEW_LINE, gddLines) + ApplicationConfiguration.NEW_LINE;

            writeFile(ApplicationConfiguration.DISTRIBUTED_DATA_DICTIONARY_NAME, gddContent);
        } catch (Exception exception) {
            LOGGER.error("Error occurred while updating distributed data dictionary to remote.");
        }
    }
}
