package com.github.datasamudaya.common.utils.sql;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;

/**
 * The Class Functions List contains all the supported sql functions.
 * @author arun
 */
public class SQLFunctionsList {
	
	/**
	 * The Function returns all the supported sql functions
	 * @return List of supported sql functions
	 */
	public static List<String> getSupportedSQLFunctions(){
		SqlStdOperatorTable sqlStdOperatorTable = SqlStdOperatorTable.instance();
		List<SqlFunction> sqlFunctions = Functions.getAllSqlFunctions();

		SqlOperatorTable customSqlOperatorTable = SqlOperatorTables.of(sqlFunctions.toArray(new SqlFunction[0]));
		SqlOperatorTable operatorTable = new SqlFunctionsChainedOperatorTable(Arrays.asList(sqlStdOperatorTable, customSqlOperatorTable));
		List<SqlOperator> supportedFunctions = operatorTable.getOperatorList();

		return supportedFunctions.stream().filter(function->function instanceof SqlFunction).map(SqlOperator::getName).sorted().toList();
	}
	
	/**
	 * The function describes SQL function
	 * @param functionname
	 * @return description of sql function.
	 */
	public static String describeFunction(String functionname){
		SqlStdOperatorTable sqlStdOperatorTable = SqlStdOperatorTable.instance();
		List<SqlFunction> sqlFunctions = Functions.getAllSqlFunctions();

		SqlOperatorTable customSqlOperatorTable = SqlOperatorTables.of(sqlFunctions.toArray(new SqlFunction[0]));
		SqlOperatorTable operatorTable = new SqlFunctionsChainedOperatorTable(Arrays.asList(sqlStdOperatorTable, customSqlOperatorTable));
		List<SqlOperator> supportedFunctions = operatorTable.getOperatorList();

		Optional<SqlOperator> functionopt = supportedFunctions.stream().filter(operator->operator.getName().equalsIgnoreCase(functionname)).findFirst();
		
		if(functionopt.isEmpty()) {
			return "Function '"+functionname+"' not available";
		}
		SqlOperator functiontodescribe = functionopt.get();
		return functiontodescribe.getAllowedSignatures();
		
	}
	
}
