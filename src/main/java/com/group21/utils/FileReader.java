package com.group21.utils;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.configurations.ApplicationConfiguration;
import com.group21.server.models.DatabaseSite;
import com.group21.server.models.TableInfo;

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

    public static Map<String, DatabaseSite> readDistributedDataDictionary() {
        Map<String, DatabaseSite> tableInfoMap = new HashMap<>();
        try {
            RemoteDatabaseReader.syncDistributedDataDictionary();
            Path localDDFilePath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + ApplicationConfiguration.DISTRIBUTED_DATA_DICTIONARY_NAME);
            List<String> fileLines = Files.readAllLines(localDDFilePath);
            fileLines.remove(0);

            for (String line : fileLines) {
                String[] columnList = line.split(ApplicationConfiguration.DELIMITER_REGEX);

                tableInfoMap.put(columnList[0], DatabaseSite.from(columnList[1]));
            }
        } catch (IOException exception) {
            LOGGER.error("Error occurred while reading distributed data dictionary.");
        }
        return tableInfoMap;
    }
}
