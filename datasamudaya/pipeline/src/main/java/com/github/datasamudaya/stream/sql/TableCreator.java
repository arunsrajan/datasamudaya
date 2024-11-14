package com.github.datasamudaya.stream.sql;

import static java.util.Objects.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hive.hcatalog.api.HCatClient;
import org.apache.hive.hcatalog.api.HCatTable;
import org.apache.hive.hcatalog.data.schema.HCatFieldSchema;

import com.github.datasamudaya.common.ColumnMetadata;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.utils.Utils;

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
	public static String createAlterTable(String db, String createCommand) throws Exception {
		Configuration conf = Utils.getHiveConf();	      
		try (Driver driver = new Driver((HiveConf) conf);){	        
	        driver.run(createCommand);
			return "Table created/altered";
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
	public static String dropTable(String db, String dropCommand) throws Exception {
		Configuration conf = Utils.getHiveConf();	      
		try (Driver driver = new Driver((HiveConf) conf);){	        
	        driver.run(dropCommand);
			return "Table dropped";
		} catch (Exception ex) {
			try (StringWriter stackTrace = new StringWriter();
					PrintWriter writer = new PrintWriter(stackTrace);) {
				ex.printStackTrace(writer);
				writer.flush();
				return stackTrace.toString();
			}
		}
	}

	public static List<String> showTables(String db, String showcommand) throws Exception {
		Configuration conf = Utils.getHiveConf();	 
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
			if(nonNull(client)) {
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
	public static String getColumnMetadataFromTable(String db, String tablename, List<ColumnMetadata> columnMetadatas) throws Exception {
		Configuration conf = Utils.getHiveConf();	 
		HCatClient client = null;
		try {
			client = HCatClient.create(conf);
			HCatTable table = client.getTable(db, tablename);
			for (HCatFieldSchema col:table.getCols()) {
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
			if(nonNull(client)) {
				client.close();
			}
		}
	}

	private TableCreator() {
	}
}
