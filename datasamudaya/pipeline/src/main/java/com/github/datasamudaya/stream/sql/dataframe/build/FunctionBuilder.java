package com.github.datasamudaya.stream.sql.dataframe.build;

import static java.util.Objects.nonNull;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

import com.github.datasamudaya.common.utils.sql.Functions;

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
	public FunctionBuilder addFunction(String function, String alias, Object[] param) {
		functioncalls.add(new Function(function, alias, param));
		return this;
	}
	
	/**
	 * The function adds the nested function
	 * @param function
	 * @return function builder object
	 */
	public FunctionBuilder addNestedFunction(Function function) {
		functioncalls.add(function);
		return this;
	}

	/**
	 * The function adds the nested function
	 * @param function
	 * @param alias
	 * @param functioninner
	 * @param param
	 * @return function
	 */
	public Function getNestedFunction(String function,Object[] param) {
		return new Function(function, null, param);
	}

	/**
	 * The function adds the fields to the builder
	 * @param alias
	 * @param param
	 * @return function builder object
	 */
	public FunctionBuilder addField(String alias, Object[] param) {
		functioncalls.add(new Function(null, alias, param));
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
		List<SqlFunction> sqlfunctions = Functions.getAllSqlFunctions();
		for (Function function : functioncalls) {
			functiontocall.add(getNestedFunctionWithOperands(function, builder, sqlfunctions));
		}
		return functiontocall;
	}
	
	/**
	 * The function builds the nested function
	 * @param function
	 * @param builder
	 * @param sqlFunctions
	 * @return RexNode
	 */
	protected static RexNode getNestedFunctionWithOperands(Function function,RelBuilder builder,List<SqlFunction> sqlFunctions) {

		if (nonNull(function.getName())) {
			switch (function.getName()) {
				case "abs":
					RexNode abs = builder.call(sqlFunctions.get(13),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						abs = builder.alias(abs, function.getAlias());
					}
					return abs;
				case "length":
					RexNode length = builder.call(sqlFunctions.get(1),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						length = builder.alias(length, function.getAlias());
					}
					return length;
				case "round":
					RexNode round = builder.call(sqlFunctions.get(12),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						round = builder.alias(round, function.getAlias());
					}
					return round;				
				case "ceil":
					RexNode ceil = builder.call(sqlFunctions.get(10),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						ceil = builder.alias(ceil, function.getAlias());
					}
					return ceil;
				case "floor":
					RexNode floor = builder.call(sqlFunctions.get(11),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						floor = builder.alias(floor, function.getAlias());
					}
					return floor;
				case "pow":
					RexNode pow = builder.call(sqlFunctions.get(8),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						pow = builder.alias(pow, function.getAlias());
					}
					return pow;
				case "sqrt":
					RexNode sqrt = builder.call(sqlFunctions.get(0),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						sqrt = builder.alias(sqrt, function.getAlias());
					}
					return sqrt;
				case "exp":
					RexNode exp = builder.call(sqlFunctions.get(9),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						exp = builder.alias(exp, function.getAlias());
					}
					return exp;
				case "loge":
					RexNode loge = builder.call(sqlFunctions.get(7),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						loge = builder.alias(loge, function.getAlias());
					}
					return loge;
				case "lowercase":
					RexNode lowercase = builder.call(sqlFunctions.get(6),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						lowercase = builder.alias(lowercase, function.getAlias());
					}
					return lowercase;
				case "uppercase":
					RexNode uppercase = builder.call(sqlFunctions.get(5),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						uppercase = builder.alias(uppercase, function.getAlias());
					}
					return uppercase;
				case "base64encode":
					RexNode base64encode = builder.call(sqlFunctions.get(3),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						base64encode = builder.alias(base64encode, function.getAlias());
					}
					return base64encode;
				case "base64decode":
					RexNode base64decode = builder.call(sqlFunctions.get(4),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						base64decode = builder.alias(base64decode, function.getAlias());
					}
					return base64decode;
				case "normalizespaces":
					RexNode normalizespaces = builder.call(sqlFunctions.get(2),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						normalizespaces = builder.alias(normalizespaces, function.getAlias());
					}
					return normalizespaces;
				case "currentisodate":
					RexNode currentisodate = builder.call(sqlFunctions.get(14),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						currentisodate = builder.alias(currentisodate, function.getAlias());
					}
					return currentisodate;
				case "trimstr":
					RexNode trimstr = builder.call(sqlFunctions.get(16),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						trimstr = builder.alias(trimstr, function.getAlias());
					}
					return trimstr;
				case "concat":
					RexNode concat = builder.call(sqlFunctions.get(15),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						concat = builder.alias(concat, function.getAlias());
					}
					return concat;
				case "overlay":
					RexNode overlay = builder.call(SqlStdOperatorTable.OVERLAY,getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						overlay = builder.alias(overlay, function.getAlias());
					}
					return overlay;
				case "initcap":
					RexNode initcap = builder.call(SqlStdOperatorTable.INITCAP,getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						initcap = builder.alias(initcap, function.getAlias());
					}
					return initcap;
				case "position":
					RexNode position = builder.call(SqlStdOperatorTable.POSITION,getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						initcap = builder.alias(position, function.getAlias());
					}
					return position;				
				default:
					throw new UnsupportedOperationException("SQL Function Not Supported");
			}
		} else {
			if (nonNull(function.getAlias())) {
				return builder.field(function.getAlias(), function.getOperands()[0].toString());
			} else {
				return getOperands(builder, function.getOperands(), sqlFunctions)[0];
			}
		}
	
		
	}
	
	/**
	 * The function builds the operands
	 * @param builder
	 * @param operands
	 * @param sqlfunctions
	 * @return RexNode array
	 */
	protected static RexNode[] getOperands(RelBuilder builder, Object[] operands, List<SqlFunction> sqlfunctions) {
		RexNode[] rexOperands = new RexNode[operands.length];
		for (int i = 0; i < operands.length; i++) {
			if(operands[i] instanceof Column column) {
                rexOperands[i] = builder.field(column.getName());
            } else if(operands[i] instanceof Literal literal) {
                rexOperands[i] = builder.literal(literal.getValue());
            } else if(operands[i] instanceof Function function) {
                rexOperands[i] = getNestedFunctionWithOperands(function, builder, sqlfunctions);
            } else {
            	rexOperands[i] = builder.field(operands[i].toString());
            }
		}
		return rexOperands;
	}
}
