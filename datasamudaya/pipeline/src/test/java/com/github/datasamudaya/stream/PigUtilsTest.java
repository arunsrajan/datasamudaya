package com.github.datasamudaya.stream;

import java.util.Iterator;
import java.util.List;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.pig.PigServer;
import org.apache.pig.newplan.Operator;
import org.apache.pig.newplan.logical.relational.LOFilter;
import org.apache.pig.newplan.logical.relational.LOLoad;
import org.apache.pig.parser.QueryParserDriver;
import org.junit.Test;

import com.github.datasamudaya.common.GlobalPigServer;
import com.github.datasamudaya.stream.pig.PigUtils;

import junit.framework.TestCase;

public class PigUtilsTest extends TestCase{

	@Test
	public void testGetParserDriver() throws Exception {
		assertNotNull(PigUtils.getQueryParserDriver("main"));
	}
	
	
	@Test
	public void testGetOperatorsLOLoad() throws Exception {
		QueryParserDriver queryparserdriver = PigUtils.getQueryParserDriver("main");
		assertNotNull(queryparserdriver);
		Iterator<Operator> loloadoperator =  PigUtils.getOperator("data = LOAD 'data.txt' AS (name:chararray, age:int);", queryparserdriver);
		assertNotNull(loloadoperator);
		assertTrue(loloadoperator.next() instanceof LOLoad);
	}
	
	@Test
	public void testGetOperatorsLOFilter() throws Exception {
		QueryParserDriver queryparserdriver = PigUtils.getQueryParserDriver("main");
		PigServer pigserver = GlobalPigServer.getPigServer();
		assertNotNull(queryparserdriver);
		pigserver.registerQuery("data = LOAD 'data.txt' AS (name:chararray, age:int);", 0);
		Iterator<Operator> loloadoperator =  PigUtils.getOperator("data = LOAD 'data.txt' AS (name:chararray, age:int);", queryparserdriver);
		assertNotNull(loloadoperator);
		assertTrue(loloadoperator.next() instanceof LOLoad);
		pigserver.registerQuery("filtered_data = FILTER data BY age >= 18 AND age <= 30;", 1);
		Iterator<Operator> lofilteroperator =  PigUtils.getOperator("data = LOAD 'data.txt' AS (name:chararray, age:int);filtered_data = FILTER data BY age >= 18 AND age <= 30;", queryparserdriver);
		assertNotNull(lofilteroperator);
		lofilteroperator.next();
		assertTrue(lofilteroperator.next() instanceof LOFilter);
	}
	
	@Test
	public void testGetLogicalSchemaForHeader() throws Exception {
		QueryParserDriver queryparserdriver = PigUtils.getQueryParserDriver("main");
		PigServer pigserver = GlobalPigServer.getPigServer();
		assertNotNull(queryparserdriver);
		pigserver.registerQuery("data = LOAD 'data.txt' AS (name:chararray, age:int);", 0);
		Iterator<Operator> loloadoperator =  PigUtils.getOperator("data = LOAD 'data.txt' AS (name:chararray, age:int);", queryparserdriver);
		assertNotNull(loloadoperator);
		LOLoad loload = (LOLoad) loloadoperator.next();
		assertTrue(loload instanceof LOLoad);
		String[] fields = PigUtils.getHeaderFromSchema(loload.getSchema());
		assertTrue(fields.length ==2);
		assertEquals("name",fields[0]);
		assertEquals("age",fields[1]);
	}
	
	@Test
	public void testGetLogicalSchemaForFieldTypes() throws Exception {
		QueryParserDriver queryparserdriver = PigUtils.getQueryParserDriver("main");
		PigServer pigserver = GlobalPigServer.getPigServer();
		assertNotNull(queryparserdriver);
		pigserver.registerQuery("data = LOAD 'data.txt' AS (name:chararray, age:int);", 0);
		Iterator<Operator> loloadoperator =  PigUtils.getOperator("data = LOAD 'data.txt' AS (name:chararray, age:int);", queryparserdriver);
		assertNotNull(loloadoperator);
		LOLoad loload = (LOLoad) loloadoperator.next();
		assertTrue(loload instanceof LOLoad);
		List<SqlTypeName> types = PigUtils.getTypesFromSchema(loload.getSchema());
		assertTrue(types.size() ==2);
		assertTrue(types.get(0) == SqlTypeName.VARCHAR);
		assertTrue(types.get(1) == SqlTypeName.INTEGER);
	}
}
