package com.github.datasamudaya.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.calcite.sql.SqlFunction;
import org.junit.Test;

import com.github.datasamudaya.common.utils.sql.Functions;
import com.github.datasamudaya.common.utils.sql.SQLFunctionsList;

public class SQLFunctionsListTest {
	@Test
	public void testSQLFunctions() {
		List<String> sqlFunctions = SQLFunctionsList.getSupportedSQLFunctions();
		assertNotNull(sqlFunctions);
		sqlFunctions.forEach(functionname->assertNotNull(functionname));
		Functions.getAllSqlFunctions().stream().map(SqlFunction::getName).forEach(function->assertTrue(sqlFunctions.contains(function)));
	}
	
	@Test
	public void testDescribeSQLFunction() {
		String function = SQLFunctionsList.describeFunction("rightpad");
		assertNotNull(function);
		assertEquals("'rightpad(<STRING>, <INTEGER>)'", function);
	}
	
	@Test
	public void testDescribeSQLFunctionNotAvailable() {
		String function = SQLFunctionsList.describeFunction("padright");
		assertEquals("Function 'padright' not available", function);
	}
}
