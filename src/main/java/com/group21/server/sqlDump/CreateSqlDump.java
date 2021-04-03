package com.group21.server.sqlDump;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.group21.server.models.Column;
import com.group21.server.models.Constraint;
import com.group21.server.models.DataType;
import com.group21.server.models.DatabaseSite;
import com.group21.server.models.TableInfo;
import com.group21.server.queries.createtable.CreateTableParser;
import com.group21.utils.FileReader;
import com.group21.utils.RegexUtil;
import com.group21.utils.RemoteDatabaseReader;

public class CreateSqlDump {
	
	private static final Logger LOGGER = LoggerFactory.getLogger(CreateTableParser.class);
	
	 private static final String VALID_SQL_DUMP_REGEX = "^export sqldump";
	 
	 public static boolean isValid(String query) {
	        String matchedQueryType1 = RegexUtil.getMatch(query, VALID_SQL_DUMP_REGEX);
	        
		 if (Strings.isBlank(matchedQueryType1)) {
	         LOGGER.error("Syntax error in exporting sql dump file.");
	         return false;
		 }
		return true;
	 
	// List<TableInfo> tableList = FileReader.readLocalDataDictionary();
}

	public static void process(String command) {
		System.out.println("in process for dump");
		
			 Map<String, DatabaseSite> tableInfoMap = FileReader.readDistributedDataDictionary();
			 Set<String> tableNameList = tableInfoMap.keySet();
			 
			 for(String list : tableNameList) {
				 System.out.println("list:"+list);
				 DatabaseSite site = tableInfoMap.get(list);
				 System.out.println("site:"+site);
				 
				 StringBuilder queryString = new StringBuilder("CREATE TABLE "+ list.toLowerCase() +" (");
				 
				 if(site.equals(DatabaseSite.LOCAL)) {
					 System.out.println("in local");
					 List<Column> localColumns = FileReader.readMetadata(list);
					 
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
							 //queryString.append(ConstraintList.get(i));
							 if(ConstraintList.get(i).equals(Constraint.PRIMARY_KEY)) {
								 queryString.append(" primary key");
							 }
							 
							 if(ConstraintList.get(i).equals(Constraint.FOREIGN_KEY)) {
								 queryString.append(" foreign key references ");
							 }
						 }
						 
						 if(!foreignKeyTableList.get(i).equals("null")) {
							 queryString.append(foreignKeyTableList.get(i).toLowerCase());
							 queryString.append("("+foreignKeyColumnNameList.get(i).toLowerCase()+")");
						 }
						 
						 if(i == columnPositionList.size()-1) {
							 queryString.append(")");
						 }
						 else {
							 queryString.append(", ");
						 }
					 }
					 System.out.println("String: "+ queryString);
					 System.out.println(); 
				 }
				 else if(site.equals(DatabaseSite.REMOTE)) {
					 System.out.println("in remote");
				 }
			 }

	}
}
