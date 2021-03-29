package com.group21.utils;

import java.io.File;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.server.models.TableInfo;
import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.SftpException;

public class RemoteDatabaseReader {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoteDatabaseReader.class);

    private RemoteDatabaseReader() {
    }

    public static List<String> readFile(String fileName) {
        List<String> fileLines = new ArrayList<>();

        try {
            String filePath = ApplicationConfiguration.REMOTE_DB_DATA_DIRECTORY + File.separator + fileName;

            ChannelSftp sftpChannel = RemoteDatabaseConnection.getSftpChannel();
            InputStream stream = sftpChannel.get(filePath);

            Path tempFile = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + UUID.randomUUID().toString() + ".tmp");
            Files.copy(stream, tempFile, StandardCopyOption.REPLACE_EXISTING);
            fileLines = Files.readAllLines(tempFile);

            Files.deleteIfExists(tempFile);
        } catch (SftpException exception) {
            if (exception.id != ChannelSftp.SSH_FX_NO_SUCH_FILE) {
                LOGGER.error("Error occurred while reading file {} from remote server.", fileName);
            }
        } catch (Exception exception) {
            LOGGER.error("Error occurred while reading file {} from remote server.", fileName);
        }
        return fileLines;
    }

    public static List<TableInfo> readLocalDataDictionary() {
        List<TableInfo> tableInfoList = new ArrayList<>();

        try {
            String filePath = ApplicationConfiguration.REMOTE_DB_DATA_DIRECTORY + File.separator + ApplicationConfiguration.LOCAL_DATA_DICTIONARY_NAME;

            ChannelSftp sftpChannel = RemoteDatabaseConnection.getSftpChannel();
            InputStream stream = sftpChannel.get(filePath);

            Path tempFile = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + UUID.randomUUID().toString() + ".tmp");
            Files.copy(stream, tempFile, StandardCopyOption.REPLACE_EXISTING);
            List<String> fileLines = Files.readAllLines(tempFile);
            fileLines.remove(0);

            for (String line : fileLines) {
                String[] columnList = line.split(ApplicationConfiguration.DELIMITER_REGEX);

                TableInfo tableInfo = new TableInfo();
                tableInfo.setTableName(columnList[0]);
                tableInfo.setNumberOfRows(Integer.parseInt(columnList[1]));
                tableInfo.setCreatedOn(Long.parseLong(columnList[2]));

                tableInfoList.add(tableInfo);
            }

            Files.deleteIfExists(tempFile);
        } catch (Exception exception) {
            LOGGER.error("Error occurred while reading local data dictionary from remote server");
        }

        return tableInfoList;
    }

    public static void syncDistributedDataDictionary() {
        try {
            String filePath = ApplicationConfiguration.REMOTE_DB_DATA_DIRECTORY + File.separator + ApplicationConfiguration.DISTRIBUTED_DATA_DICTIONARY_NAME;

            ChannelSftp sftpChannel = RemoteDatabaseConnection.getSftpChannel();
            InputStream stream = sftpChannel.get(filePath);

            Path gddFile = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + ApplicationConfiguration.DISTRIBUTED_DATA_DICTIONARY_NAME);
            Files.copy(stream, gddFile, StandardCopyOption.REPLACE_EXISTING);
        } catch (SftpException exception) {
            if (exception.id != ChannelSftp.SSH_FX_NO_SUCH_FILE) {
                LOGGER.error("Error occurred while reading distributed data dictionary from remote server");
            }
        } catch (Exception exception) {
            LOGGER.error("Error occurred while reading distributed data dictionary from remote server");
        }
    }
}
