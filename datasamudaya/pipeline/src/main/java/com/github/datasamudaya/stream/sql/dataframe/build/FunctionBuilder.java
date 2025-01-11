package com.github.datasamudaya.stream.sql.dataframe.build;

import static java.util.Objects.nonNull;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlFunction;
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
			if (nonNull(function.getName())) {
				switch (function.getName()) {
					case "abs":
						RexNode abs = builder.call(sqlfunctions.get(13),getOperands(builder, function.getOperands()));
						if(nonNull(function.getAlias())) {
							abs = builder.alias(abs, function.getAlias());
						}
						functiontocall.add(abs);
						break;
					case "length":
						RexNode length = builder.call(sqlfunctions.get(1),getOperands(builder, function.getOperands()));
						if(nonNull(function.getAlias())) {
							length = builder.alias(length, function.getAlias());
						}
						functiontocall.add(length);
						break;
					case "round":
						RexNode round = builder.call(sqlfunctions.get(12),getOperands(builder, function.getOperands()));
						if(nonNull(function.getAlias())) {
							round = builder.alias(round, function.getAlias());
						}
						functiontocall.add(round);
						break;						
					case "ceil":
						RexNode ceil = builder.call(sqlfunctions.get(10),getOperands(builder, function.getOperands()));
						if(nonNull(function.getAlias())) {
							ceil = builder.alias(ceil, function.getAlias());
						}
						functiontocall.add(ceil);
						break;
					case "floor":
						RexNode floor = builder.call(sqlfunctions.get(11),getOperands(builder, function.getOperands()));
						if(nonNull(function.getAlias())) {
							floor = builder.alias(floor, function.getAlias());
						}
						functiontocall.add(floor);
						break;
					case "pow":
						RexNode pow = builder.call(sqlfunctions.get(8),getOperands(builder, function.getOperands()));
						if(nonNull(function.getAlias())) {
							pow = builder.alias(pow, function.getAlias());
						}
						functiontocall.add(pow);
						break;
					case "sqrt":
						RexNode sqrt = builder.call(sqlfunctions.get(0),getOperands(builder, function.getOperands()));
						if(nonNull(function.getAlias())) {
							sqrt = builder.alias(sqrt, function.getAlias());
						}
						functiontocall.add(sqrt);
						break;
					case "exp":
						RexNode exp = builder.call(sqlfunctions.get(9),getOperands(builder, function.getOperands()));
						if(nonNull(function.getAlias())) {
							exp = builder.alias(exp, function.getAlias());
						}
						functiontocall.add(exp);
						break;
					case "loge":
						RexNode loge = builder.call(sqlfunctions.get(7),getOperands(builder, function.getOperands()));
						if(nonNull(function.getAlias())) {
							loge = builder.alias(loge, function.getAlias());
						}
						functiontocall.add(loge);
						break;
					case "lowercase":
						RexNode lowercase = builder.call(sqlfunctions.get(6),getOperands(builder, function.getOperands()));
						if(nonNull(function.getAlias())) {
							lowercase = builder.alias(lowercase, function.getAlias());
						}
						functiontocall.add(lowercase);
						break;
					case "uppercase":
						RexNode uppercase = builder.call(sqlfunctions.get(5),getOperands(builder, function.getOperands()));
						if(nonNull(function.getAlias())) {
							uppercase = builder.alias(uppercase, function.getAlias());
						}
						functiontocall.add(uppercase);
						break;
					case "base64encode":
						RexNode base64encode = builder.call(sqlfunctions.get(3),getOperands(builder, function.getOperands()));
						if(nonNull(function.getAlias())) {
							base64encode = builder.alias(base64encode, function.getAlias());
						}
						functiontocall.add(base64encode);
						break;
					case "base64decode":
						RexNode base64decode = builder.call(sqlfunctions.get(4),getOperands(builder, function.getOperands()));
						if(nonNull(function.getAlias())) {
							base64decode = builder.alias(base64decode, function.getAlias());
						}
						functiontocall.add(base64decode);
						break;
					case "normalizespaces":
						RexNode normalizespaces = builder.call(sqlfunctions.get(2),getOperands(builder, function.getOperands()));
						if(nonNull(function.getAlias())) {
							normalizespaces = builder.alias(normalizespaces, function.getAlias());
						}
						functiontocall.add(normalizespaces);
						break;
					case "currentisodate":
						RexNode currentisodate = builder.call(sqlfunctions.get(14),getOperands(builder, function.getOperands()));
						if(nonNull(function.getAlias())) {
							currentisodate = builder.alias(currentisodate, function.getAlias());
						}
						functiontocall.add(currentisodate);
						break;
					case "trimstr":
						RexNode trimstr = builder.call(sqlfunctions.get(16),getOperands(builder, function.getOperands()));
						if(nonNull(function.getAlias())) {
							trimstr = builder.alias(trimstr, function.getAlias());
						}
						functiontocall.add(trimstr);
						break;
					case "concat":
						RexNode concat = builder.call(sqlfunctions.get(15),getOperands(builder, function.getOperands()));
						if(nonNull(function.getAlias())) {
							concat = builder.alias(concat, function.getAlias());
						}
						functiontocall.add(concat);
						break;
					default:
						throw new UnsupportedOperationException("SQL Function Not Supported");
				}
			} else {
				if (nonNull(function.getAlias())) {
					functiontocall.add(builder.field(function.getAlias(), function.getOperands()[0].toString()));
				} else {
					functiontocall.add(getOperands(builder, function.getOperands())[0]);
				}
			}
		}
		return functiontocall;
	}
	
	/**
	 * The function gets the operands
	 * @param operands
	 * @return
	 */
	public static RexNode[] getOperands(RelBuilder builder, Object[] operands) {
		RexNode[] rexOperands = new RexNode[operands.length];
		for (int i = 0; i < operands.length; i++) {
			if(operands[i] instanceof Column column) {
                rexOperands[i] = builder.field(column.getName());
            } else if(operands[i] instanceof Literal literal) {
                rexOperands[i] = builder.literal(literal.getValue());
            } else {
            	rexOperands[i] = builder.field(operands[i].toString());
            }
		}
		return rexOperands;
	}
	
	
}
