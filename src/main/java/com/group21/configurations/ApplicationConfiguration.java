package com.group21.configurations;

public class ApplicationConfiguration {
    private ApplicationConfiguration() {
    }

    public static final String DATA_FILE_FORMAT = ".dat";
    public static final String METADATA_FILE_FORMAT = ".metadata";
    public static final String DELIMITER = "|";
    public static final String DELIMITER_REGEX = "\\|";
    public static final String DATA_DIRECTORY = "DDBMS_21_Data";
    public static final String AUTHENTICATION_FILE_NAME = "authentication.dat";
    public static final String LOCAL_DATA_DICTIONARY_NAME = "local_data_dictionary.dat";
    public static final String FILE_SEPARATOR = System.getProperty("file.separator");
    public static final String NEW_LINE = "\n";
}
