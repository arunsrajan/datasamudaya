package com.github.datasamudaya.tasks.scheduler.sql;

import static java.util.Objects.nonNull;

import java.io.PrintWriter;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.collections.CollectionUtils;

import com.github.datasamudaya.common.ColumnMetadata;
import com.github.datasamudaya.common.JobConfiguration;
import com.github.datasamudaya.common.JobConfigurationBuilder;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.stream.sql.TableCreator;
import com.github.datasamudaya.stream.utils.SQLUtils;
import com.github.datasamudaya.tasks.scheduler.MapReduceApplication;
import com.github.datasamudaya.tasks.scheduler.MapReduceApplicationIgnite;
import com.github.datasamudaya.tasks.scheduler.MapReduceApplicationYarn;

import net.sf.jsqlparser.parser.CCJSqlParserManager;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.util.validation.Validation;
import net.sf.jsqlparser.util.validation.ValidationError;
import net.sf.jsqlparser.util.validation.feature.DatabaseType;

/**
 * This class executes the sql queries from sql client terminal.
 * @author arun
 *
 */
public class SelectQueryExecutorMR {

	/**
	 * This method executes the query and returns the result in list
	 * @param selectquery
	 * @param user
	 * @param appid
	 * @param teappid
	 * @return query results in list format
	 * @throws Exception
	 */
	public static List executeSelectQuery(String defaultdb, String selectquery, String user, String appid,
			String teappid, boolean isyarn) throws Exception {
		try {
			JobConfiguration jc = JobConfigurationBuilder.newBuilder()
					.setIsuseglobalte(!isyarn)
					.setUser(user)
					.setTeappid(teappid)
					.setExecmode(isyarn ? DataSamudayaConstants.EXECMODE_YARN : DataSamudayaConstants.EXECMODE_DEFAULT)
					.build();
			CCJSqlParserManager parserManager = new CCJSqlParserManager();
			Validation validation = new Validation(Arrays.asList(DatabaseType.SQLSERVER, DatabaseType.MARIADB,
					DatabaseType.POSTGRESQL, DatabaseType.H2), selectquery);
			List<ValidationError> errors = validation.validate();
			if (!CollectionUtils.isEmpty(errors)) {
				throw new Exception("Syntax error in SQL");
			}
			Statement statement = parserManager.parse(new StringReader(selectquery));
			Select select = (Select) statement;
			if (select.getSelectBody() instanceof PlainSelect) {
				var plainSelect = (PlainSelect) select.getSelectBody();
				Table tablePrimary = (Table) plainSelect.getFromItem();
				var tables = new ArrayList<Table>();
				tables.add(tablePrimary);

				if (nonNull(plainSelect.getJoins())) {
					for (Join join : plainSelect.getJoins()) {
						tables.add(((Table) join.getRightItem()));
					}
				}

				var builder = MapReduceApplicationSqlBuilder.newBuilder().setHdfs(DataSamudayaProperties.get()
						.getProperty(DataSamudayaConstants.HDFSNAMENODEURL, DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT))
						.setJobConfiguration(jc).setSql(selectquery);
				for (Table table : tables) {
					var columnMetadatas = new ArrayList<ColumnMetadata>();
					TableCreator.getColumnMetadataFromTable(defaultdb, table.getName(), columnMetadatas);
					String hdfslocation = null;
					List<String> tablecolumn = new ArrayList<>();
					List<SqlTypeName> tablecolumnDataType = new ArrayList<>();
					for (ColumnMetadata columnMetadata : columnMetadatas) {
						if ("hdfslocation".equals(columnMetadata.getColumnName().toLowerCase())) {
							hdfslocation = columnMetadata.getColumnDefault().replace("'", "").trim();
						} else {
							tablecolumn.add(columnMetadata.getColumnName().toLowerCase());
							tablecolumnDataType.add(SQLUtils.getSQLTypeNameMR(columnMetadata.getDataType()));
						}
					}
					builder = builder.add(hdfslocation, table.getName().toLowerCase(),
							tablecolumn,
							tablecolumnDataType);
				}
				Object mraobj = builder.build();
				if(!isyarn && mraobj instanceof MapReduceApplication mra) {					
					return (List) mra.call();
				} else if(isyarn && mraobj instanceof MapReduceApplicationYarn mray) {
					return (List) mray.call();
				}
			}
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
		return new ArrayList<>();
	}
	
	
	public static List executeSelectQueryIgnite(String defaultdb, String selectquery, String user, String appid,
			String teappid) throws Exception {
		try {
			JobConfiguration jc = JobConfigurationBuilder.newBuilder()
					.setIsuseglobalte(true)
					.setUser(user)
					.setTeappid(teappid)
					.setExecmode(DataSamudayaConstants.EXECMODE_IGNITE)
					.build();
			CCJSqlParserManager parserManager = new CCJSqlParserManager();
			Validation validation = new Validation(Arrays.asList(DatabaseType.SQLSERVER, DatabaseType.MARIADB,
					DatabaseType.POSTGRESQL, DatabaseType.H2), selectquery);
			List<ValidationError> errors = validation.validate();
			if (!CollectionUtils.isEmpty(errors)) {
				throw new Exception("Syntax error in SQL");
			}
			Statement statement = parserManager.parse(new StringReader(selectquery));
			Select select = (Select) statement;
			if (select.getSelectBody() instanceof PlainSelect) {
				var plainSelect = (PlainSelect) select.getSelectBody();
				Table tablePrimary = (Table) plainSelect.getFromItem();
				var tables = new ArrayList<Table>();
				tables.add(tablePrimary);

				if (nonNull(plainSelect.getJoins())) {
					for (Join join : plainSelect.getJoins()) {
						tables.add(((Table) join.getRightItem()));
					}
				}

				var builder = MapReduceApplicationSqlBuilder.newBuilder().setHdfs(DataSamudayaProperties.get()
						.getProperty(DataSamudayaConstants.HDFSNAMENODEURL, DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT))
						.setJobConfiguration(jc).setSql(selectquery);
				for (Table table : tables) {
					var columnMetadatas = new ArrayList<ColumnMetadata>();
					TableCreator.getColumnMetadataFromTable(defaultdb, table.getName(), columnMetadatas);
					String hdfslocation = null;
					List<String> tablecolumn = new ArrayList<>();
					List<SqlTypeName> tablecolumnDataType = new ArrayList<>();
					for (ColumnMetadata columnMetadata : columnMetadatas) {
						if ("hdfslocation".equals(columnMetadata.getColumnName().toLowerCase())) {
							hdfslocation = columnMetadata.getColumnDefault().replace("'", "").trim();
						} else {
							tablecolumn.add(columnMetadata.getColumnName().toLowerCase());
							tablecolumnDataType.add(SQLUtils.getSQLTypeNameMR(columnMetadata.getDataType()));
						}
					}
					builder = builder.add(hdfslocation, table.getName().toLowerCase(),
							tablecolumn,
							tablecolumnDataType);
				}
				MapReduceApplicationIgnite mra = (MapReduceApplicationIgnite) builder.build();
				return (List) mra.call();
			}
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
		return new ArrayList<>();
	}

	private SelectQueryExecutorMR() {
	}

}
