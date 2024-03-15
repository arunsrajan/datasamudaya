package com.github.datasamudaya.stream.sql.dataframe.build;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilder.AggCall;

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
	public AggregateFunctionBuilder sum(String alias, String expression) {
		functioncalls.add(new Function("sum", alias, expression));
		return this;
	}

	/**
	 * Add the avg function to the builder
	 * @param alias
	 * @param expression
	 * @return aggfunction builder object
	 */
	public AggregateFunctionBuilder avg(String alias, String expression) {
		functioncalls.add(new Function("avg", alias, expression));
		return this;
	}

	/**
	 * Add the count function to the builder
	 * @param alias
	 * @return aggfunction builder object
	 */
	public AggregateFunctionBuilder count(String alias) {
		functioncalls.add(new Function("count", alias, "*"));
		return this;
	}

	/**
	 * Builds the functions to Aggregate Call
	 * @param builder
	 * @return aggregate call list
	 */
	protected List<AggCall> build(RelBuilder builder) {
		List<AggCall> functions = new ArrayList<>();
		for (Function function :functioncalls) {
			switch (function.getName()) {
				case "sum":
					functions.add(builder.sum(false, function.getAlias(), builder.field(function.getExpression())));
					break;
				case "avg":
					functions.add(builder.avg(false, function.getAlias(), builder.field(function.getExpression())));
					break;
				case "count":
					functions.add(builder.countStar("cnt"));
					break;
				default:
			}
		}
		return functions;
	}
}
