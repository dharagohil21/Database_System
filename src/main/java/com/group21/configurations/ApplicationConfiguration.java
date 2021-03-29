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
    public static final String DISTRIBUTED_DATA_DICTIONARY_NAME = "distributed_data_dictionary.dat";
    public static final String FILE_SEPARATOR = System.getProperty("file.separator");
    public static final String NEW_LINE = "\n";
    public static final String INTEGER_REGEX = "[0-9]+";
    public static final String DOUBLE_REGEX = "[0-9]+(\\.[0-9]+)";
    public static final String TEXT_REGEX = "[a-zA-Z_]+";

    // Remote Database configuration
    public static final String REMOTE_DB_DATA_DIRECTORY = "/home/kartik_gevariya0003/csci-5408-group-21/DDBMS_21_Data";
    public static final String REMOTE_DB_USER = "kartik_gevariya0003";
    public static final String REMOTE_DB_HOST = "34.67.239.126";
    public static final String PRIVATE_KEY_FILE_PATH = "/Users/kartikgevariya/.ssh/id_rsa_gcp";

}
