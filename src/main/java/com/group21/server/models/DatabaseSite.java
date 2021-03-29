package com.group21.server.models;

import java.util.List;

import com.group21.utils.FileReader;
import com.group21.utils.FileWriter;
import com.group21.utils.RemoteDatabaseReader;
import com.group21.utils.RemoteDatabaseWriter;

public enum DatabaseSite {
    LOCAL {
        @Override
        public List<TableInfo> readLocalDataDictionary() {
            return FileReader.readLocalDataDictionary();
        }

        @Override
        public void writeLocalDataDictionary(TableInfo tableInfo) {
            FileWriter.writeLocalDataDictionary(tableInfo);
        }

        @Override
        public void writeMetadata(String tableName, List<Column> columnDetails) {
            FileWriter.writeMetadata(tableName, columnDetails);
        }

        @Override
        public void writeData(String tableName, List<String> columnData) {
            FileWriter.writeData(tableName, columnData);
        }
    },
    REMOTE {
        @Override
        public List<TableInfo> readLocalDataDictionary() {
            return RemoteDatabaseReader.readLocalDataDictionary();
        }

        @Override
        public void writeLocalDataDictionary(TableInfo tableInfo) {
            RemoteDatabaseWriter.writeLocalDataDictionary(tableInfo);
        }

        @Override
        public void writeMetadata(String tableName, List<Column> columnDetails) {
            RemoteDatabaseWriter.writeMetadata(tableName, columnDetails);
        }

        @Override
        public void writeData(String tableName, List<String> columnData) {
            RemoteDatabaseWriter.writeData(tableName, columnData);
        }
    };

    public abstract List<TableInfo> readLocalDataDictionary();

    public abstract void writeLocalDataDictionary(TableInfo tableInfo);

    public abstract void writeMetadata(String tableName, List<Column> columnDetails);

    public abstract void writeData(String tableName, List<String> columnData);

    public static DatabaseSite from(String siteName) {
        for (DatabaseSite databaseSite : values()) {
            if (databaseSite.name().equalsIgnoreCase(siteName)) {
                return databaseSite;
            }
        }
        return LOCAL;
    }
}
