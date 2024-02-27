package com.github.datasamudaya.stream.sql.dataframe.build;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.tools.RelBuilder;

import com.github.datasamudaya.stream.utils.SQLUtils;

import static java.util.Objects.nonNull;

/**
 * The class builds the non-aggregate function
 * 
 * @author arun
 *
 */
public class FunctionBuilder {
	List<Function> functioncalls = new ArrayList<>();
	RelBuilder builder;

	private FunctionBuilder() {
	}

	public static FunctionBuilder builder() {
		return new FunctionBuilder();
	}

	/**
	 * Adds Custom function to the builder
	 * 
	 * @param function
	 * @param alias
	 * @param param
	 * @return function builder object
	 */
	public FunctionBuilder addFunction(String function, String alias, String[] param) {
		functioncalls.add(new Function(function, alias, param[0]));
		return this;
	}

	/**
	 * The function adds the fields to the builder
	 * @param alias
	 * @param param
	 * @return function builder object
	 */
	public FunctionBuilder addField(String alias, String[] param) {
		functioncalls.add(new Function(null, alias, param[0]));
		return this;
	}
	
	/**
	 * Builds the functions to Aggregate Call
	 * 
	 * @param builder
	 * @return aggregate call list
	 */
	protected List<RexNode> build(RelBuilder builder) {
		List<RexNode> functiontocall = new ArrayList<>();
		List<SqlFunction> sqlfunctions = SQLUtils.getAllSqlFunctions();
		for (Function function : functioncalls) {
			if (nonNull(function.getName())) {
				switch (function.getName()) {
				case "abs":
					functiontocall.add(builder.call(sqlfunctions.get(14), builder.field(function.getExpression())));
					break;
				case "length":
					functiontocall.add(builder.call(sqlfunctions.get(1), builder.field(function.getExpression())));
					break;
				case "round":
					functiontocall.add(builder.call(sqlfunctions.get(13), builder.field(function.getExpression())));
					break;
				case "ceil":
					functiontocall.add(builder.call(sqlfunctions.get(11), builder.field(function.getExpression())));
					break;
				case "floor":
					functiontocall.add(builder.call(sqlfunctions.get(12), builder.field(function.getExpression())));
					break;
				case "pow":
					functiontocall.add(builder.call(sqlfunctions.get(9), builder.field(function.getExpression())));
					break;
				case "sqrt":
					functiontocall.add(builder.call(sqlfunctions.get(0), builder.field(function.getExpression())));
					break;
				case "exp":
					functiontocall.add(builder.call(sqlfunctions.get(10), builder.field(function.getExpression())));
					break;
				case "loge":
					functiontocall.add(builder.call(sqlfunctions.get(8), builder.field(function.getExpression())));
					break;
				case "lowercase":
					functiontocall.add(builder.call(sqlfunctions.get(7), builder.field(function.getExpression())));
					break;
				case "uppercase":
					functiontocall.add(builder.call(sqlfunctions.get(6), builder.field(function.getExpression())));
					break;
				case "base64encode":
					functiontocall.add(builder.call(sqlfunctions.get(4), builder.field(function.getExpression())));
					break;
				case "base64decode":
					functiontocall.add(builder.call(sqlfunctions.get(5), builder.field(function.getExpression())));
					break;
				case "normalizespaces":
					functiontocall.add(builder.call(sqlfunctions.get(2), builder.field(function.getExpression())));
					break;
				case "currentisodate":
					functiontocall.add(builder.call(sqlfunctions.get(15), builder.field(function.getExpression())));
					break;
				case "trimstr":
					functiontocall.add(builder.call(sqlfunctions.get(17), builder.field(function.getExpression())));
					break;
				case "substring":
					functiontocall.add(builder.call(sqlfunctions.get(3), builder.field(function.getExpression())));
					break;
				case "grpconcat":
					functiontocall.add(builder.call(sqlfunctions.get(16), builder.field(function.getExpression())));
					break;
				default:
					throw new UnsupportedOperationException("SQL Function Not Supported");
				}
			} else {
				if(nonNull(function.getAlias())) {
					functiontocall.add(builder.field(function.getAlias(),function.getExpression()));
				}
				else {
					functiontocall.add(builder.field(function.getExpression()));
				}
			}
		}
		return functiontocall;
	}
}
