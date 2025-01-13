package com.github.datasamudaya.stream.sql.dataframe.build;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilder.AggCall;

import com.github.datasamudaya.common.utils.sql.Functions;

/**
 * The class builds the aggregate function
 * @author arun
 *
 */
public class AggregateFunctionBuilder {
	List<Function> functioncalls = new ArrayList<>();
	RelBuilder builder;

	private AggregateFunctionBuilder() {
	}

	public static AggregateFunctionBuilder builder() {
		return new AggregateFunctionBuilder();
	}

	/**
	 * Adds the sum function to the builder
	 * @param alias
	 * @param expression
	 * @return aggfunction builder object
	 */
	public AggregateFunctionBuilder sum(String alias, Object[] expression) {
		functioncalls.add(new Function("sum", alias, expression));
		return this;
	}

	/**
	 * Add the avg function to the builder
	 * @param alias
	 * @param expression
	 * @return aggfunction builder object
	 */
	public AggregateFunctionBuilder avg(String alias, Object[] expression) {
		functioncalls.add(new Function("avg", alias, expression));
		return this;
	}

	/**
	 * Add the count function to the builder
	 * @param alias
	 * @return aggfunction builder object
	 */
	public AggregateFunctionBuilder count(String alias) {
		functioncalls.add(new Function("count", alias, new Object[]{"*"}));
		return this;
	}

	/**
	 * Builds the functions to Aggregate Call
	 * @param builder
	 * @return aggregate call list
	 */
	protected List<AggCall> build(RelBuilder builder) {
		List<AggCall> functions = new ArrayList<>();
		List<SqlFunction> sqlfunctions = Functions.getAllSqlFunctions();
		for (Function function :functioncalls) {
			switch (function.getName()) {
				case "sum":
					functions.add(builder.sum(false, function.getAlias(), FunctionBuilder.getOperands(builder, function.getOperands(), sqlfunctions)[0]));
					break;
				case "avg":
					functions.add(builder.avg(false, function.getAlias(),  FunctionBuilder.getOperands(builder, function.getOperands(), sqlfunctions)[0]));
					break;
				case "count":
					functions.add(builder.countStar(function.getAlias()));
					break;
				default:
			}
		}
		return functions;
	}
}
