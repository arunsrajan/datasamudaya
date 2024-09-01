package com.github.datasamudaya.stream.utils;

import java.util.List;

import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.util.ChainedSqlOperatorTable;

import com.google.common.collect.ImmutableList;

/**
 * @author arun
 * The class represents List of sql operator tables to be chained 
 */
public class SqlFunctionsChainedOperatorTable extends ChainedSqlOperatorTable {

	public SqlFunctionsChainedOperatorTable(List<SqlOperatorTable> sqlOperatorsList) {
		this(ImmutableList.copyOf(sqlOperatorsList));
	}
	
	protected SqlFunctionsChainedOperatorTable(ImmutableList<SqlOperatorTable> tableList) {
		super(tableList);		
	}

}
