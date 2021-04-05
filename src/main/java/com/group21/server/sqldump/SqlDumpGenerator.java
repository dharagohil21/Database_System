package com.group21.server.sqldump;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.group21.configurations.ApplicationConfiguration;
import com.group21.server.models.Column;
import com.group21.server.models.Constraint;
import com.group21.server.models.DataType;
import com.group21.server.models.DatabaseSite;
import com.group21.server.queries.createtable.CreateTableParser;
import com.group21.utils.FileReader;
import com.group21.utils.RemoteDatabaseReader;

public class SqlDumpGenerator {
	
	public static final String START_BRACKET = "(";
    public static final String CLOSE_BRACKET = ")";
	
	private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableParser.class);

	public static void generate() {
		Path sqlDumpFilePath = Paths.get(ApplicationConfiguration.DATA_DIRECTORY + ApplicationConfiguration.FILE_SEPARATOR + ApplicationConfiguration.SQL_DUMP_FILE_NAME);

        try {
            if (Files.notExists(sqlDumpFilePath)) {
                Files.createFile(sqlDumpFilePath);
            } else {
                Files.write(sqlDumpFilePath, "".getBytes(), StandardOpenOption.TRUNCATE_EXISTING);
            }
		
			 Map<String, DatabaseSite> tableInfoMap = FileReader.readDistributedDataDictionary();
			 Set<String> tableNameList = tableInfoMap.keySet();
			 for(String list : tableNameList) {
				 DatabaseSite site = tableInfoMap.get(list);
				 
				 StringBuilder queryString = new StringBuilder("CREATE TABLE " + list.toLowerCase() + " " + START_BRACKET);
				 
				 List<Column> localColumns = new ArrayList<Column>();
				 if(site.equals(DatabaseSite.LOCAL)) {
					 localColumns = FileReader.readMetadata(list);
				 }
				 else if(site.equals(DatabaseSite.REMOTE)) {
					 localColumns = RemoteDatabaseReader.readMetadata(list);
				 }
					 
				List<String> columnNameList = localColumns.stream().map(Column::getColumnName).collect(Collectors.toList());
				List<DataType> dataTypeList = localColumns.stream().map(Column::getColumnType).collect(Collectors.toList());
				List<Constraint> ConstraintList = localColumns.stream().map(Column::getConstraint).collect(Collectors.toList());
				List<String> foreignKeyTableList = localColumns.stream().map(Column::getForeignKeyTable).collect(Collectors.toList());
				List<String> foreignKeyColumnNameList = localColumns.stream().map(Column::getForeignKeyColumnName).collect(Collectors.toList());
				List<Integer> columnPositionList = localColumns.stream().map(Column::getColumnPosition).collect(Collectors.toList());
					 
				for(int i=0; i<columnPositionList.size(); i++) {
						 
					queryString.append(columnNameList.get(i).toLowerCase()+ " ");
					if(!dataTypeList.get(i).equals(DataType.UNKNOWN)) {
						queryString.append(dataTypeList.get(i));
					}
						 
					if(!ConstraintList.get(i).equals(Constraint.UNKNOWN)) {
						if(ConstraintList.get(i).equals(Constraint.PRIMARY_KEY)) {
							queryString.append(" primary key");
						}
							 
						if(ConstraintList.get(i).equals(Constraint.FOREIGN_KEY)) {
							queryString.append(" foreign key references ");
						}
					}
						 
					if(!foreignKeyTableList.get(i).equals("null")) {
						queryString.append(foreignKeyTableList.get(i).toLowerCase());
						queryString.append(START_BRACKET + foreignKeyColumnNameList.get(i).toLowerCase() + CLOSE_BRACKET);
					}
						 
					if(i == columnPositionList.size()-1) {
						queryString.append(CLOSE_BRACKET);		 
					}
					else {
						queryString.append(", ");
					}
				}
				
				queryString.append(ApplicationConfiguration.NEW_LINE);
				Files.write(sqlDumpFilePath, queryString.toString().getBytes(), StandardOpenOption.APPEND);
			 }
			 
			 LOGGER.info("SqlDump file '{}' generated successfully.", ApplicationConfiguration.SQL_DUMP_FILE_NAME);
        } catch (IOException exception) {
            LOGGER.info("Error occurred while generating Sql Ddump.");
        } 
	}
}
