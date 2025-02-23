package com.github.datasamudaya.stream.sql;

import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.runtime.CalciteContextException;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidatorException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;

import com.github.datasamudaya.common.ColumnMetadata;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.PipelineConstants;
import com.github.datasamudaya.stream.PipelineException;
import com.github.datasamudaya.stream.sql.build.StreamPipelineSqlBuilder;
import com.github.datasamudaya.stream.sql.build.StreamPipelineSql;
import com.github.datasamudaya.stream.utils.SQLUtils;

import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.util.validation.Validation;
import net.sf.jsqlparser.util.validation.ValidationError;
import net.sf.jsqlparser.util.validation.ValidationException;
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
			String tejobid, boolean isjgroups, boolean isyarn, PrintWriter writerresult, boolean isschedulerremote) throws Exception {
		try {
			PipelineConfig pc = new PipelineConfig();
			pc.setWriter(writerresult);
			pc.setLocal("false");
			if (isjgroups) {
				pc.setJgroups("true");
			} else {
				pc.setJgroups("false");
			}
			if (isyarn) {
				pc.setYarn("true");
			} else {
				pc.setYarn("false");
			}
			pc.setJobname(DataSamudayaConstants.SQL);
			pc.setMesos("false");
			pc.setMode(DataSamudayaConstants.MODE_NORMAL);
			pc.setContaineralloc(DataSamudayaConstants.CONTAINER_ALLOC_USERSHARE);
			pc.setUseglobaltaskexecutors(true);
			pc.setUser(user);
			pc.setJobid(jobid);
			pc.setTejobid(tejobid);
			pc.setIsremotescheduler(isschedulerremote);
			CCJSqlParserManager parserManager = new CCJSqlParserManager();
			Validation validation = new Validation(Arrays.asList(DatabaseType.SQLSERVER, DatabaseType.MARIADB,
					DatabaseType.POSTGRESQL, DatabaseType.H2), selectquery);
			List<ValidationError> errors = validation.validate();
			if (!CollectionUtils.isEmpty(errors)) {
				StringBuilder builder = new StringBuilder();
				for(ValidationError validerror:errors) {
					Set<ValidationException> validexception = validerror.getErrors();
					for(ValidationException validex:validexception) {
						builder.append(validex.getMessage());
						builder.append(DataSamudayaConstants.NEWLINE);
					}
				}
				throw new SQLException("Syntax error in SQL: "+builder.toString());
			}
			Statement statement = parserManager.parse(new StringReader(selectquery));
			var tables = new ArrayList<String>();
			SQLUtils.getAllTables(statement, tables);
			var builder = StreamPipelineSqlBuilder.newBuilder()
					.setHdfs(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL,
							DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT))
					.setPipelineConfig(pc).setSql(selectquery).setDb(dbdefault);
			for (String tablename : tables) {
				var columnMetadatas = new ArrayList<ColumnMetadata>();
				String hdfslocation = TableCreator.getColumnMetadataFromTable(user, dbdefault, tablename, columnMetadatas);
				String fileformat = DataSamudayaConstants.CSV;
				List<String> tablecolumn = new ArrayList<>();
				List<SqlTypeName> tablecolumnDataType = new ArrayList<>();
				for (ColumnMetadata columnMetadata : columnMetadatas) {
					tablecolumn.add(columnMetadata.getColumnName().toLowerCase());
					tablecolumnDataType.add(SQLUtils.getHiveSQLTypeName(columnMetadata.getDataType()));
				}
				builder = builder.add(hdfslocation, tablename.toLowerCase(), tablecolumn, tablecolumnDataType);
				builder.setFileformat(fileformat);
			}
			StreamPipelineSql mdpsql = builder.build();
			return (List) mdpsql.collect(true, null);
		} catch(IllegalStateException ise) {
			List errors = new ArrayList<>();
			List error = new ArrayList<>();
			if(ise.getMessage().equals(PipelineConstants.TABLEMUSHHAVEFIELDS)) {
				error.add(PipelineConstants.TABLEMUSHHAVEFIELDS + DataSamudayaConstants.OR + DataSamudayaConstants.NEWLINE +PipelineConstants.EITHERTABLEISNOTAVAILABLEINSCHEMAORNOFIELDS.formatted(dbdefault));
			}
			errors.add(error);
			return errors;
		} catch(CalciteContextException cce) {
			List errors = new ArrayList<>();
			List error = new ArrayList<>();		
			error.add(cce.getMessage());
			if(cce.getCause() instanceof SqlValidatorException sve) {
				error.add(sve.getMessage());
			}
			errors.add(error);
			return errors;
		} catch (PipelineException pex) {
			List errors = new ArrayList<>();
			List error = new ArrayList<>();
			error.add(pex.getMessage());
			errors.add(error);
			return errors;
		} catch(SQLException sqlex) {
			List errors = new ArrayList<>();
			List error = new ArrayList<>();
			error.add(sqlex.getMessage());
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

	/**
	 * The function explains the optimized plan for the given select query
	 * @param dbdefault
	 * @param selectquery
	 * @throws Exception
	 */
	public static void explain(String user, String dbdefault, String selectquery, PrintWriter out) throws Exception {
		try {
			CCJSqlParserManager parserManager = new CCJSqlParserManager();
			Validation validation = new Validation(Arrays.asList(DatabaseType.SQLSERVER, DatabaseType.MARIADB,
					DatabaseType.POSTGRESQL, DatabaseType.H2), selectquery);
			List<ValidationError> errors = validation.validate();
			if (!CollectionUtils.isEmpty(errors)) {
				throw new Exception("Syntax error in SQL");
			}
			Statement statement = parserManager.parse(new StringReader(selectquery));
			var tables = new ArrayList<String>();
			SQLUtils.getAllTables(statement, tables);
			ConcurrentMap<String, List<String>> tablecolumnsmap = new ConcurrentHashMap<>();
			ConcurrentMap<String, List<SqlTypeName>> tablecolumntypesmap = new ConcurrentHashMap<>();
			for (String tablename : tables) {
				var columnMetadatas = new ArrayList<ColumnMetadata>();
				TableCreator.getColumnMetadataFromTable(user, dbdefault, tablename, columnMetadatas);
				List<String> tablecolumn = new ArrayList<>();
				List<SqlTypeName> tablecolumnDataType = new ArrayList<>();
				for (ColumnMetadata columnMetadata : columnMetadatas) {
					tablecolumn.add(columnMetadata.getColumnName().toLowerCase());
					tablecolumnDataType.add(SQLUtils.getHiveSQLTypeName(columnMetadata.getDataType()));
				}
				tablecolumnsmap.put(tablename, tablecolumn);
				tablecolumntypesmap.put(tablename, tablecolumnDataType);
			}
			AtomicBoolean isdistinct = new AtomicBoolean(false);
			RelNode relnode = SQLUtils.validateSql(tablecolumnsmap, tablecolumntypesmap, selectquery, dbdefault,
					isdistinct);
			SQLUtils.traverseRelNode(relnode, 0, out);
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
			out.println("Error in Query: " + selectquery);
		}
	}

	/**
	 * Executes select sql query ignite mode.
	 * @param dbdefault
	 * @param selectquery
	 * @param user
	 * @param jobid
	 * @param tejobid
	 * @return sql query results
	 * @throws Exception
	 */
	public static List executeSelectQueryIgnite(String dbdefault, String selectquery, String user, String jobid,
			String tejobid, long maxmem) throws Exception {
		try {
			PipelineConfig pc = new PipelineConfig();
			pc.setJobname(DataSamudayaConstants.SQL);
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
			String mem = String.valueOf(maxmem);
			pc.setMinmem(mem);
			pc.setMaxmem(mem);
			CCJSqlParserManager parserManager = new CCJSqlParserManager();
			Validation validation = new Validation(Arrays.asList(DatabaseType.SQLSERVER, DatabaseType.MARIADB,
					DatabaseType.POSTGRESQL, DatabaseType.H2), selectquery);
			List<ValidationError> errors = validation.validate();
			if (!CollectionUtils.isEmpty(errors)) {
				StringBuilder builder = new StringBuilder();
				for(ValidationError validerror:errors) {
					Set<ValidationException> validexception = validerror.getErrors();
					for(ValidationException validex:validexception) {
						builder.append(validex.getMessage());
						builder.append(DataSamudayaConstants.NEWLINE);
					}
				}
				throw new SQLException("Syntax error in SQL: "+builder.toString());
			}
			Statement statement = parserManager.parse(new StringReader(selectquery));
			var tables = new ArrayList<String>();
			SQLUtils.getAllTables(statement, tables);
			var builder = StreamPipelineSqlBuilder.newBuilder()
					.setHdfs(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL,
							DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT))
					.setPipelineConfig(pc).setSql(selectquery).setDb(dbdefault);
			for (String tablename : tables) {
				var columnMetadatas = new ArrayList<ColumnMetadata>();
				String hdfslocation = TableCreator.getColumnMetadataFromTable(user, dbdefault, tablename, columnMetadatas);
				String fileformat = DataSamudayaConstants.CSV;
				List<String> tablecolumn = new ArrayList<>();
				List<SqlTypeName> tablecolumnDataType = new ArrayList<>();
				for (ColumnMetadata columnMetadata : columnMetadatas) {
					tablecolumn.add(columnMetadata.getColumnName().toLowerCase());
					tablecolumnDataType.add(SQLUtils.getHiveSQLTypeName(columnMetadata.getDataType()));
				}
				builder = builder.add(hdfslocation, tablename.toLowerCase(), tablecolumn, tablecolumnDataType);
				builder.setFileformat(fileformat);
			}
			StreamPipelineSql ipsql = builder.build();
			return (List) ipsql.collect(true, null);
		} catch(IllegalStateException ise) {
			List errors = new ArrayList<>();
			List error = new ArrayList<>();
			if(ise.getMessage().equals(PipelineConstants.TABLEMUSHHAVEFIELDS)) {
				error.add(PipelineConstants.TABLEMUSHHAVEFIELDS + DataSamudayaConstants.OR + DataSamudayaConstants.NEWLINE +PipelineConstants.EITHERTABLEISNOTAVAILABLEINSCHEMAORNOFIELDS.formatted(dbdefault));
			}
			errors.add(error);
			return errors;
		} catch(CalciteContextException cce) {
			List errors = new ArrayList<>();
			List error = new ArrayList<>();		
			error.add(cce.getMessage());
			if(cce.getCause() instanceof SqlValidatorException sve) {
				error.add(sve.getMessage());
			}
			errors.add(error);
			return errors;
		} catch (PipelineException pex) {
			List errors = new ArrayList<>();
			List error = new ArrayList<>();
			error.add(pex.getMessage());
			errors.add(error);
			return errors;
		} catch(SQLException sqlex) {
			List errors = new ArrayList<>();
			List error = new ArrayList<>();
			error.add(sqlex.getMessage());
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

	private SelectQueryExecutor() {
	}

}
