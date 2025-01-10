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
						functiontocall.add(builder.call(sqlfunctions.get(13),getOperands(builder, function.getOperands())));
						break;
					case "length":
						functiontocall.add(builder.call(sqlfunctions.get(1),getOperands(builder, function.getOperands())));
						break;
					case "round":
						functiontocall.add(builder.call(sqlfunctions.get(12),getOperands(builder, function.getOperands())));
						break;
					case "ceil":
						functiontocall.add(builder.call(sqlfunctions.get(10),getOperands(builder, function.getOperands())));
						break;
					case "floor":
						functiontocall.add(builder.call(sqlfunctions.get(11),getOperands(builder, function.getOperands())));
						break;
					case "pow":
						functiontocall.add(builder.call(sqlfunctions.get(8),getOperands(builder, function.getOperands())));
						break;
					case "sqrt":
						functiontocall.add(builder.call(sqlfunctions.get(0),getOperands(builder, function.getOperands())));
						break;
					case "exp":
						functiontocall.add(builder.call(sqlfunctions.get(9),getOperands(builder, function.getOperands())));
						break;
					case "loge":
						functiontocall.add(builder.call(sqlfunctions.get(7),getOperands(builder, function.getOperands())));
						break;
					case "lowercase":
						functiontocall.add(builder.call(sqlfunctions.get(6),getOperands(builder, function.getOperands())));
						break;
					case "uppercase":
						functiontocall.add(builder.call(sqlfunctions.get(5),getOperands(builder, function.getOperands())));
						break;
					case "base64encode":
						functiontocall.add(builder.call(sqlfunctions.get(3),getOperands(builder, function.getOperands())));
						break;
					case "base64decode":
						functiontocall.add(builder.call(sqlfunctions.get(4),getOperands(builder, function.getOperands())));
						break;
					case "normalizespaces":
						functiontocall.add(builder.call(sqlfunctions.get(2),getOperands(builder, function.getOperands())));
						break;
					case "currentisodate":
						functiontocall.add(builder.call(sqlfunctions.get(14),getOperands(builder, function.getOperands())));
						break;
					case "trimstr":
						functiontocall.add(builder.call(sqlfunctions.get(16),getOperands(builder, function.getOperands())));
						break;					
					case "grpconcat":
						functiontocall.add(builder.call(sqlfunctions.get(15),getOperands(builder, function.getOperands())));
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
