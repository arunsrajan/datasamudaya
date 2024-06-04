package com.github.datasamudaya.stream.sql;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import com.github.datasamudaya.common.ColumnMetadata;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;

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
		try (Connection conn = DriverManager.getConnection(DataSamudayaConstants.SQLDB_URL + db,
				DataSamudayaProperties.get()
						.getProperty(DataSamudayaConstants.SQLDBUSERNAME, DataSamudayaConstants.SQLDBUSERNAME_DEFAULT),
				DataSamudayaProperties.get()
						.getProperty(DataSamudayaConstants.SQLDBPASSWORD, DataSamudayaConstants.SQLDBPASSWORD_DEFAULT));
					Statement stmt = conn.createStatement();) {
			stmt.execute(createCommand);
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
	 * @param createCommand
	 * @return "Table Dropped" message
	 * @throws Exception
	 */
	public static String dropTable(String db, String createCommand) throws Exception {
		try (Connection conn = DriverManager.getConnection(DataSamudayaConstants.SQLDB_URL + db,
				DataSamudayaProperties.get()
						.getProperty(DataSamudayaConstants.SQLDBUSERNAME, DataSamudayaConstants.SQLDBUSERNAME_DEFAULT),
				DataSamudayaProperties.get()
						.getProperty(DataSamudayaConstants.SQLDBPASSWORD, DataSamudayaConstants.SQLDBPASSWORD_DEFAULT));
				Statement stmt = conn.createStatement();) {
			stmt.execute(createCommand);
			return "Table Dropped";
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
		try (Connection conn = DriverManager.getConnection(DataSamudayaConstants.SQLDB_URL + db,
				DataSamudayaProperties.get()
						.getProperty(DataSamudayaConstants.SQLDBUSERNAME, DataSamudayaConstants.SQLDBUSERNAME_DEFAULT),
				DataSamudayaProperties.get()
						.getProperty(DataSamudayaConstants.SQLDBPASSWORD, DataSamudayaConstants.SQLDBPASSWORD_DEFAULT));
				Statement stmt = conn.createStatement();
				ResultSet tablesresultset = stmt.executeQuery(showcommand);) {
			var tables = new ArrayList<String>();
			while (tablesresultset.next()) {
				tables.add(tablesresultset.getString(1));
			}
			return tables;
		} catch (Exception ex) {
			try (StringWriter stackTrace = new StringWriter();
				PrintWriter writer = new PrintWriter(stackTrace);) {
				ex.printStackTrace(writer);
				writer.flush();
				return Arrays.asList(stackTrace.toString());
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
		try (Connection conn = DriverManager.getConnection(DataSamudayaConstants.SQLDB_URL + db,
				DataSamudayaProperties.get()
						.getProperty(DataSamudayaConstants.SQLDBUSERNAME, DataSamudayaConstants.SQLDBUSERNAME_DEFAULT),
				DataSamudayaProperties.get()
						.getProperty(DataSamudayaConstants.SQLDBPASSWORD, DataSamudayaConstants.SQLDBPASSWORD_DEFAULT));
				var stmt = conn.prepareStatement("SELECT COLUMN_NAME, DATA_TYPE, COLUMN_DEFAULT, CHARACTER_MAXIMUM_LENGTH"
						+ " FROM INFORMATION_SCHEMA.COLUMNS "
						+ " WHERE TABLE_NAME = '" + tablename.toUpperCase() + "'");
				ResultSet result = stmt.executeQuery();) {
			while (result.next()) {
				var columnMetadata = new ColumnMetadata(result.getString("COLUMN_NAME"),
						result.getString("DATA_TYPE"), result.getInt("CHARACTER_MAXIMUM_LENGTH"), result.getString("COLUMN_DEFAULT"));
				columnMetadatas.add(columnMetadata);
			}
			return "Metadata Retrieved for table " + tablename;
		} catch (Exception ex) {
			try (StringWriter stackTrace = new StringWriter();
				PrintWriter writer = new PrintWriter(stackTrace);) {
				ex.printStackTrace(writer);
				writer.flush();
				return stackTrace.toString();
			}
		}
	}

	private TableCreator() {
	}
}
