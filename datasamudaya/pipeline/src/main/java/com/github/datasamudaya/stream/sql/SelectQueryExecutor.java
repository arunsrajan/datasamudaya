package com.github.datasamudaya.stream.sql;

import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import com.github.datasamudaya.common.ColumnMetadata;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.stream.PipelineException;
import com.github.datasamudaya.stream.sql.build.StreamPipelineSql;
import com.github.datasamudaya.stream.sql.build.StreamPipelineSqlBuilder;
import com.github.datasamudaya.stream.utils.SQLUtils;

import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.validation.Validation;
import net.sf.jsqlparser.util.validation.ValidationError;
import net.sf.jsqlparser.util.validation.feature.DatabaseType;

/**
 * This class executes the sql queries from sql client terminal.
 * @author arun
 *
 */
public class SelectQueryExecutor {

	/**
	 * This method executes the query and returns the result in list
	 * @param selectquery
	 * @param user
	 * @param jobid
	 * @param tejobid
	 * @return query results in list format
	 * @throws Exception
	 */
	public static List executeSelectQuery(String dbdefault, String selectquery, String user, String jobid,
			String tejobid, boolean isjgroups, boolean isyarn) throws Exception {
		try {
			PipelineConfig pc = new PipelineConfig();
			pc.setLocal("false");
			if(isjgroups) {
				pc.setJgroups("true");
			} else {
				pc.setJgroups("false");
			}
			if(isyarn) {
				pc.setYarn("true");
			} else {
				pc.setYarn("false");
			}
			pc.setMesos("false");
			pc.setMode(DataSamudayaConstants.MODE_NORMAL);
			pc.setContaineralloc(DataSamudayaConstants.CONTAINER_ALLOC_USERSHARE);
			pc.setUseglobaltaskexecutors(true);		
			pc.setUser(user);
			pc.setJobid(jobid);
			pc.setTejobid(tejobid);
			CCJSqlParserManager parserManager = new CCJSqlParserManager();
			Validation validation = new Validation(Arrays.asList(DatabaseType.SQLSERVER, DatabaseType.MARIADB,
					DatabaseType.POSTGRESQL, DatabaseType.H2), selectquery);
			List<ValidationError> errors = validation.validate();
			if (!CollectionUtils.isEmpty(errors)) {
				throw new Exception("Syntax error in SQL");
			}
			Statement statement = parserManager.parse(new StringReader(selectquery));
			var tables = new ArrayList<Table>();
			var tmpset = new HashSet<String>();			
			SQLUtils.getAllTables(statement, tables, tmpset);
			var builder = StreamPipelineSqlBuilder.newBuilder()
					.setHdfs(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL,
							DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT))
					.setPipelineConfig(pc).setSql(selectquery).setDb(dbdefault);
			for (Table table : tables) {
				var columnMetadatas = new ArrayList<ColumnMetadata>();
				TableCreator.getColumnMetadataFromTable(dbdefault, table.getName(), columnMetadatas);
				String hdfslocation = null;
				List<String> tablecolumn = new ArrayList<>();
				List<SqlTypeName> tablecolumnDataType = new ArrayList<>();
				for (ColumnMetadata columnMetadata : columnMetadatas) {
					if (columnMetadata.getColumnName().toLowerCase().equals("hdfslocation")) {
						hdfslocation = columnMetadata.getColumnDefault().replace("'", "").trim();
					} else {
						tablecolumn.add(columnMetadata.getColumnName().toLowerCase());
						tablecolumnDataType.add(SQLUtils.getSQLTypeName(columnMetadata.getDataType()));
					}
				}
				builder = builder.add(hdfslocation, table.getName().toLowerCase(), tablecolumn, tablecolumnDataType);
			}
			StreamPipelineSql mdpsql = builder.build();
			return (List) mdpsql.collect(true, null);
		} catch (PipelineException ex) {
			List errors = new ArrayList<>();
			List error = new ArrayList<>();
			error.add(ExceptionUtils.getRootCauseMessage(ex));
			errors.add(error);
			return errors;
		} catch (Exception ex) {
			List errors = new ArrayList<>();
			List error = new ArrayList<>();
			try (StringWriter stackTrace = new StringWriter(); 
					PrintWriter writer = new PrintWriter(stackTrace);) {
				ex.printStackTrace(writer);
				writer.flush();
				error.add(stackTrace.toString());
				errors.add(error);
			}
			return errors;
		}
	}
	
	
	public static List executeSelectQueryIgnite(String dbdefault, String selectquery, String user, String jobid,
			String tejobid) throws Exception {
		try {
			PipelineConfig pc = new PipelineConfig();
			pc.setLocal("false");
			pc.setJgroups("false");
			pc.setYarn("false");
			pc.setMesos("false");
			pc.setMode(DataSamudayaConstants.MODE_DEFAULT);
			pc.setContaineralloc(DataSamudayaConstants.CONTAINER_ALLOC_USERSHARE);
			pc.setUseglobaltaskexecutors(true);		
			pc.setUser(user);
			pc.setJobid(jobid);
			pc.setTejobid(tejobid);
			CCJSqlParserManager parserManager = new CCJSqlParserManager();
			Validation validation = new Validation(Arrays.asList(DatabaseType.SQLSERVER, DatabaseType.MARIADB,
					DatabaseType.POSTGRESQL, DatabaseType.H2), selectquery);
			List<ValidationError> errors = validation.validate();
			if (!CollectionUtils.isEmpty(errors)) {
				throw new Exception("Syntax error in SQL");
			}
			Statement statement = parserManager.parse(new StringReader(selectquery));
			Select select = (Select) statement;
			var tables = new ArrayList<Table>();
			var tmpset = new HashSet<String>();			
			SQLUtils.getAllTables(statement, tables, tmpset);
			var builder = StreamPipelineSqlBuilder.newBuilder()
					.setHdfs(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL,
							DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT))
					.setPipelineConfig(pc).setSql(selectquery).setDb(dbdefault);
			for (Table table : tables) {
				var columnMetadatas = new ArrayList<ColumnMetadata>();
				TableCreator.getColumnMetadataFromTable(dbdefault, table.getName(), columnMetadatas);
				String hdfslocation = null;
				List<String> tablecolumn = new ArrayList<>();
				List<SqlTypeName> tablecolumnDataType = new ArrayList<>();
				for (ColumnMetadata columnMetadata : columnMetadatas) {
					if (columnMetadata.getColumnName().toLowerCase().equals("hdfslocation")) {
						hdfslocation = columnMetadata.getColumnDefault().replace("'", "").trim();
					} else {
						tablecolumn.add(columnMetadata.getColumnName().toLowerCase());
						tablecolumnDataType.add(SQLUtils.getSQLTypeName(columnMetadata.getDataType()));
					}
				}
				builder = builder.add(hdfslocation, table.getName().toLowerCase(), tablecolumn, tablecolumnDataType);
			}
			StreamPipelineSql ipsql = builder.build();
			return (List) ipsql.collect(true, null);
		} catch (Exception ex) {
			List errors = new ArrayList<>();
			List error = new ArrayList<>();
			try (StringWriter stackTrace = new StringWriter(); 
					PrintWriter writer = new PrintWriter(stackTrace);) {
				ex.printStackTrace(writer);
				writer.flush();
				error.add(stackTrace.toString());
				errors.add(error);
			}
			return errors;
		}
	}

}
