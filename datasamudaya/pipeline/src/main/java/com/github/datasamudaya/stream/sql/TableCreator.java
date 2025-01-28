package com.github.datasamudaya.stream.sql;

import static java.util.Objects.nonNull;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.ConnectException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorResponse;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatTable;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;

import com.github.datasamudaya.common.ColumnMetadata;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.PipelineConstants;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.PipelineException;

/**
 * The SQL class to create table and executes sql queries.
 * @author arun
 *
 */
public class TableCreator {

	/**
	 * Exceutes create table command. 
	 * @param createCommand
	 * @return Table Created message
	 * @throws Exception
	 */
	public static String createAlterTable(String user, String db, String createCommand) throws Exception {
		Configuration conf = Utils.getHiveConf(user);
		try (Driver driver = new Driver((HiveConf) conf);) {
			CommandProcessorResponse response = driver.run(createCommand);			
			return response.getMessage();
		} catch(CommandProcessorException cpex) {			
			if(cpex.getCause() instanceof HiveException hex) {
				if(hex.getCause() instanceof MetaException mex) {
					return mex.getMessage();
				}
				if(hex.getCause() instanceof AlreadyExistsException aex) {
					return aex.getMessage();
				}
				return hex.getMessage();
			}
			return cpex.getMessage();
		} catch (Exception ex) {			
			try (StringWriter stackTrace = new StringWriter();
					PrintWriter writer = new PrintWriter(stackTrace);) {
				ex.printStackTrace(writer);
				writer.flush();
				return stackTrace.toString();
			}
		}
	}

	/**
	 * Executes the Drop table command.
	 * @param dropCommand
	 * @return "Table Dropped" message
	 * @throws Exception
	 */
	public static String dropTable(String user, String db, String dropCommand) throws Exception {
		Configuration conf = Utils.getHiveConf(user);
		try (Driver driver = new Driver((HiveConf) conf);) {
			CommandProcessorResponse response = driver.run(dropCommand);			
			return response.getMessage();
		} catch (Exception ex) {
			if(ex.getCause() instanceof SemanticException semanticex) {
				return semanticex.getMessage();
			}
			try (StringWriter stackTrace = new StringWriter();
					PrintWriter writer = new PrintWriter(stackTrace);) {
				ex.printStackTrace(writer);
				writer.flush();
				return stackTrace.toString();
			}
		}
	}

	/**
	 * The function returns all tables for the given schema
	 * @param user
	 * @param db
	 * @param showcommand
	 * @return tables
	 * @throws Exception
	 */
	public static List<String> showTables(String user, String db, String showcommand) throws Exception {
		Configuration conf = Utils.getHiveConf(user);
		HCatClient client = null;
		try {
			client = HCatClient.create(conf);
			return client.listTableNamesByPattern(db, DataSamudayaConstants.ASTERIX);
		} catch (Exception ex) {
			try (StringWriter stackTrace = new StringWriter();
				PrintWriter writer = new PrintWriter(stackTrace);) {
				ex.printStackTrace(writer);
				writer.flush();
				return Arrays.asList(stackTrace.toString());
			}
		} finally {
			if (nonNull(client)) {
				client.close();
			}
		}
	}

	/**
	 * The function returns all databases from metastore
	 * @param user
	 * @param db
	 * @param showcommand
	 * @return databases
	 * @throws Exception
	 */
	public static List<String> showDatabases(String user) throws Exception {
		Configuration conf = Utils.getHiveConf(user);
		HCatClient client = null;
		try {
			client = HCatClient.create(conf);
			return client.listDatabaseNamesByPattern(DataSamudayaConstants.ASTERIX);
		} catch (Exception ex) {
			try (StringWriter stackTrace = new StringWriter();
				PrintWriter writer = new PrintWriter(stackTrace);) {
				ex.printStackTrace(writer);
				writer.flush();
				return Arrays.asList(stackTrace.toString());
			}
		} finally {
			if (nonNull(client)) {
				client.close();
			}
		}
	}
	
	/**
	 * Gets Column metadata from table.
	 * @param tablename
	 * @param columnMetadatas
	 * @return "Metadata Retrieved for table" message
	 * @throws Exception
	 */
	public static String getColumnMetadataFromTable(String user, String db, String tablename, List<ColumnMetadata> columnMetadatas) throws Exception {
		Configuration conf = Utils.getHiveConf(user);
		HCatClient client = null;
		try {
			client = HCatClient.create(conf);
			HCatTable table = client.getTable(db, tablename);
			for (HCatFieldSchema col :table.getCols()) {
				var columnMetadata = new ColumnMetadata(col.getName(),
						col.getTypeString(), 10, DataSamudayaConstants.EMPTY);
				columnMetadatas.add(columnMetadata);
			}
			URI uri = URI.create(table.getLocation());
			return uri.getRawPath();
		} catch (Exception ex) {
			try (StringWriter stackTrace = new StringWriter();
				PrintWriter writer = new PrintWriter(stackTrace);) {
				ex.printStackTrace(writer);
				writer.flush();
				return stackTrace.toString();
			}
		} finally {
			if (nonNull(client)) {
				client.close();
			}
		}
	}

	private TableCreator() {
	}
}
