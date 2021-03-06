package com.group21.client;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.utils.RemoteDatabaseReader;

public class DDBMSSetup {

    private static final Logger LOGGER = LoggerFactory.getLogger(DDBMSSetup.class);

    private DDBMSSetup() {
    }

    public static void perform() {
        Path dataDirectoryPath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY);
        Path localDDPath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + ApplicationConfiguration.LOCAL_DATA_DICTIONARY_NAME);
        Path distributedDDPath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + ApplicationConfiguration.DISTRIBUTED_DATA_DICTIONARY_NAME);
        try {
            if (Files.notExists(dataDirectoryPath)) {
                Files.createDirectory(dataDirectoryPath);
            }

            if (Files.notExists(localDDPath)) {
                Files.createFile(localDDPath);

                String headerRow = "TableName|NumberOfRows|CreatedOn" + ApplicationConfiguration.NEW_LINE;
                Files.write(localDDPath, headerRow.getBytes());
            }

            if (Files.notExists(distributedDDPath)) {
                Files.createFile(distributedDDPath);

                String headerRow = "TableName|DatabaseSite" + ApplicationConfiguration.NEW_LINE;
                Files.write(distributedDDPath, headerRow.getBytes());
            }

            RemoteDatabaseReader.syncDistributedDataDictionary();
        } catch (IOException exception) {
            LOGGER.error("Error occurred while creating data directory.");
        }
    }
}
