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
						position = builder.alias(position, function.getAlias());
					}
					return position;
				case "acos":
					RexNode acos = builder.call(SqlStdOperatorTable.ACOS,getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						acos = builder.alias(acos, function.getAlias());
					}
					return acos;
				case "asin":
					RexNode asin = builder.call(SqlStdOperatorTable.ASIN,getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						asin = builder.alias(asin, function.getAlias());
					}
					return asin;
				case "atan":
					RexNode atan = builder.call(SqlStdOperatorTable.ATAN,getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						atan = builder.alias(atan, function.getAlias());
					}
					return atan;
				case "cos":
					RexNode cos = builder.call(SqlStdOperatorTable.COS,getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						cos = builder.alias(cos, function.getAlias());
					}
					return cos;
				case "sin":
					RexNode sin = builder.call(SqlStdOperatorTable.SIN,getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						sin = builder.alias(sin, function.getAlias());
					}
					return sin;
				case "tan":
					RexNode tan = builder.call(SqlStdOperatorTable.TAN,getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						tan = builder.alias(tan, function.getAlias());
					}
					return tan;
				case "cosec":
					RexNode cosec = builder.call(sqlFunctions.get(21),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						cosec = builder.alias(cosec, function.getAlias());
					}
					return cosec;
				case "sec":
					RexNode sec = builder.call(sqlFunctions.get(20),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						sec = builder.alias(sec, function.getAlias());
					}
					return sec;
				case "cot":
					RexNode cot = builder.call(sqlFunctions.get(22),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						cot = builder.alias(cot, function.getAlias());
					}
					return cot;
				case "cbrt":
					RexNode cbrt = builder.call(SqlStdOperatorTable.CBRT,getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						cbrt = builder.alias(cbrt, function.getAlias());
					}
					return cbrt;
				case "pii":
					RexNode pii = builder.call(sqlFunctions.get(19),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						pii = builder.alias(pii, function.getAlias());
					}
					return pii;
				case "degrees":
					RexNode degrees = builder.call(SqlStdOperatorTable.DEGREES,getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						degrees = builder.alias(degrees, function.getAlias());
					}
					return degrees;
				case "radians":
					RexNode radians = builder.call(SqlStdOperatorTable.RADIANS,getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						radians = builder.alias(radians, function.getAlias());
					}
					return radians;
				case "substring":
					RexNode substring = builder.call(SqlStdOperatorTable.SUBSTRING,getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						substring = builder.alias(substring, function.getAlias());
					}
					return substring;
				case "locate":
					RexNode locate = builder.call(sqlFunctions.get(30),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						locate = builder.alias(locate, function.getAlias());
					}
					return locate;
				case "cast":
					RexNode cast = builder.call(SqlStdOperatorTable.CAST,getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						cast = builder.alias(cast, function.getAlias());
					}
					return cast;
				case "group_concat":
					RexNode groupconcat = builder.call(sqlFunctions.get(17),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						groupconcat = builder.alias(groupconcat, function.getAlias());
					}
					return groupconcat;
				case "ascii":
					RexNode ascii = builder.call(SqlStdOperatorTable.ASCII,getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						ascii = builder.alias(ascii, function.getAlias());
					}
					return ascii;
				case "charac":
					RexNode charac = builder.call(sqlFunctions.get(23),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						charac = builder.alias(charac, function.getAlias());
					}
					return charac;
				case "insertstr":
					RexNode insertstr = builder.call(sqlFunctions.get(24),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						insertstr = builder.alias(insertstr, function.getAlias());
					}
					return insertstr;
				case "leftchars":
					RexNode leftchars = builder.call(sqlFunctions.get(27),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						leftchars = builder.alias(leftchars, function.getAlias());
					}
					return leftchars;
				case "rightchars":
					RexNode rightchars = builder.call(sqlFunctions.get(28),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						rightchars = builder.alias(rightchars, function.getAlias());
					}
					return rightchars;
				case "reverse":
					RexNode reverse = builder.call(sqlFunctions.get(29),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						reverse = builder.alias(reverse, function.getAlias());
					}
					return reverse;
				case "trim":
					RexNode trim = builder.call(sqlFunctions.get(16),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						trim = builder.alias(trim, function.getAlias());
					}
					return trim;
				case "ltrim":
					RexNode ltrim = builder.call(sqlFunctions.get(31),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						ltrim = builder.alias(ltrim, function.getAlias());
					}
					return ltrim;
				case "rtrim":
					RexNode rtrim = builder.call(sqlFunctions.get(32),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						rtrim = builder.alias(rtrim, function.getAlias());
					}
					return rtrim;
				case "curdate":
					RexNode curdate = builder.call(sqlFunctions.get(33),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						curdate = builder.alias(curdate, function.getAlias());
					}
					return curdate;
				case "curtime":
					RexNode curtime = builder.call(sqlFunctions.get(35),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						curtime = builder.alias(curtime, function.getAlias());
					}
					return curtime;
				case "now":
					RexNode now = builder.call(sqlFunctions.get(34),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						now = builder.alias(now, function.getAlias());
					}
					return now;
				case "year":
					RexNode year = builder.call(SqlStdOperatorTable.YEAR,getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						year = builder.alias(year, function.getAlias());
					}
					return year;
				case "month":
					RexNode month = builder.call(SqlStdOperatorTable.MONTH,getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						month = builder.alias(month, function.getAlias());
					}
					return month;
				case "day":
					RexNode day = builder.call(SqlStdOperatorTable.DAYOFMONTH,getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						day = builder.alias(day, function.getAlias());
					}
					return day;
				case "case":
					RexNode casefunction = builder.call(SqlStdOperatorTable.CASE,getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						casefunction = builder.alias(casefunction, function.getAlias());
					}
					return casefunction;
				case "indexof":
					RexNode indexof = builder.call(sqlFunctions.get(36),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						indexof = builder.alias(indexof, function.getAlias());
					}
					return indexof;
				case "indexofstartpos":
					RexNode indexofstartpos = builder.call(sqlFunctions.get(37),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						indexofstartpos = builder.alias(indexofstartpos, function.getAlias());
					}
					return indexofstartpos;
				case "indexofany":
					RexNode indexofany = builder.call(sqlFunctions.get(38),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						indexofany = builder.alias(indexofany, function.getAlias());
					}
					return indexofany;
				case "indexofanybut":
					RexNode indexofanybut = builder.call(sqlFunctions.get(39),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						indexofanybut = builder.alias(indexofanybut, function.getAlias());
					}
					return indexofanybut;
				case "indexofdiff":
					RexNode indexofdiff = builder.call(sqlFunctions.get(40),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						indexofdiff = builder.alias(indexofdiff, function.getAlias());
					}
					return indexofdiff;
				case "indexofignorecase":
					RexNode indexofignorecase = builder.call(sqlFunctions.get(41),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						indexofignorecase = builder.alias(indexofignorecase, function.getAlias());
					}
					return indexofignorecase;
				case "indexofignorecasestartpos":
					RexNode indexofignorecasestartpos = builder.call(sqlFunctions.get(42),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						indexofignorecasestartpos = builder.alias(indexofignorecasestartpos, function.getAlias());
					}
					return indexofignorecasestartpos;
				case "lastindexof":
					RexNode lastindexof = builder.call(sqlFunctions.get(43),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						lastindexof = builder.alias(lastindexof, function.getAlias());
					}
					return lastindexof;
				case "lastindexofstartpos":
					RexNode lastindexofstartpos = builder.call(sqlFunctions.get(44),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						lastindexofstartpos = builder.alias(lastindexofstartpos, function.getAlias());
					}
					return lastindexofstartpos;
				case "lastindexofany":
					RexNode lastindexofany = builder.call(sqlFunctions.get(45),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						lastindexofany = builder.alias(lastindexofany, function.getAlias());
					}
					return lastindexofany;
				case "leftpad":
					RexNode leftpad = builder.call(sqlFunctions.get(46),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						leftpad = builder.alias(leftpad, function.getAlias());
					}
					return leftpad;
				case "leftpadstring":
					RexNode leftpadstring = builder.call(sqlFunctions.get(47),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						leftpadstring = builder.alias(leftpadstring, function.getAlias());
					}
					return leftpadstring;
				case "remove":
					RexNode remove = builder.call(sqlFunctions.get(48),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						remove = builder.alias(remove, function.getAlias());
					}
					return remove;
				case "removeend":
					RexNode removeend = builder.call(sqlFunctions.get(49),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						removeend = builder.alias(removeend, function.getAlias());
					}
					return removeend;
				case "removeendignorecase":
					RexNode removeendignorecase = builder.call(sqlFunctions.get(50),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						removeendignorecase = builder.alias(removeendignorecase, function.getAlias());
					}
					return removeendignorecase;
				case "removeignorecase":
					RexNode removeignorecase = builder.call(sqlFunctions.get(51),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						removeignorecase = builder.alias(removeignorecase, function.getAlias());
					}
					return removeignorecase;
				case "removestart":
					RexNode removestart = builder.call(sqlFunctions.get(52),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						removestart = builder.alias(removestart, function.getAlias());
					}
					return removestart;
				case "removestartignorecase":
					RexNode removestartignorecase = builder.call(sqlFunctions.get(53),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						removestartignorecase = builder.alias(removestartignorecase, function.getAlias());
					}
					return removestartignorecase;
				case "repeat":
					RexNode repeat = builder.call(sqlFunctions.get(54),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						repeat = builder.alias(repeat, function.getAlias());
					}
					return repeat;
				case "repeatseparator":
					RexNode repeatseparator = builder.call(sqlFunctions.get(55),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						repeatseparator = builder.alias(repeatseparator, function.getAlias());
					}
					return repeatseparator;
				case "chop":
					RexNode chop = builder.call(sqlFunctions.get(56),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						chop = builder.alias(chop, function.getAlias());
					}
					return chop;
				case "getdigits":
					RexNode getdigits = builder.call(sqlFunctions.get(57),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						getdigits = builder.alias(getdigits, function.getAlias());
					}
					return getdigits;
				case "rightpad":
					RexNode rightpad = builder.call(sqlFunctions.get(58),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						rightpad = builder.alias(rightpad, function.getAlias());
					}
					return rightpad;
				case "rightpadstring":
					RexNode rightpadstring = builder.call(sqlFunctions.get(59),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						rightpadstring = builder.alias(rightpadstring, function.getAlias());
					}
					return rightpadstring;
				case "rotate":
					RexNode rotate = builder.call(sqlFunctions.get(60),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						rotate = builder.alias(rotate, function.getAlias());
					}
					return rotate;
				case "wrap":
					RexNode wrap = builder.call(sqlFunctions.get(61),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						wrap = builder.alias(wrap, function.getAlias());
					}
					return wrap;
				case "wrapifmissing":
					RexNode wrapifmissing = builder.call(sqlFunctions.get(62),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						wrapifmissing = builder.alias(wrapifmissing, function.getAlias());
					}
					return wrapifmissing;
				case "unwrap":
					RexNode unwrap = builder.call(sqlFunctions.get(63),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						unwrap = builder.alias(unwrap, function.getAlias());
					}
					return unwrap;
				case "uncapitalize":
					RexNode uncapitalize = builder.call(sqlFunctions.get(64),getOperands(builder, function.getOperands(), sqlFunctions));
					if(nonNull(function.getAlias())) {
						uncapitalize = builder.alias(uncapitalize, function.getAlias());
					}
					return uncapitalize;
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
