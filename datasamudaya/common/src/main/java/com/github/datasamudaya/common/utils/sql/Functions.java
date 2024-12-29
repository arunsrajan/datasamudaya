package com.github.datasamudaya.common.utils.sql;

import java.util.Arrays;
import java.util.List;

import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.util.Optionality;

/**
 * @author arun
 * The class for all sql functions.
 */
public class Functions {

	private Functions() {
	}

	/**
	 * The function returns all the supported sql functions
	 * 
	 * @return list of supported sql functions
	 */
	public static List<SqlFunction> getAllSqlFunctions() {

		SqlFunction sqrtFunction = new SqlFunction("sqrt", SqlKind.OTHER_FUNCTION, ReturnTypes.DOUBLE, null,
				OperandTypes.ANY, SqlFunctionCategory.USER_DEFINED_FUNCTION);


		SqlFunction secantfunction = new SqlFunction("sec", SqlKind.OTHER_FUNCTION, ReturnTypes.DOUBLE, null,
				OperandTypes.NUMERIC, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction cosecantfunction = new SqlFunction("cosec", SqlKind.OTHER_FUNCTION, ReturnTypes.DOUBLE, null,
				OperandTypes.NUMERIC, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction cotangentfunction = new SqlFunction("cot", SqlKind.OTHER_FUNCTION, ReturnTypes.DOUBLE, null,
				OperandTypes.NUMERIC, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction lengthFunction = new SqlFunction("length", SqlKind.OTHER_FUNCTION, ReturnTypes.INTEGER, null,
				OperandTypes.STRING, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction trimFunction = new SqlFunction("trimstr", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4, null,
				OperandTypes.STRING, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction normalizespaces = new SqlFunction("normalizespaces", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4,
				null, OperandTypes.STRING, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction base64encode = new SqlFunction("base64encode", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4, null,
				OperandTypes.STRING, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction base64decode = new SqlFunction("base64decode", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4, null,
				OperandTypes.STRING, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction uppercase = new SqlFunction("uppercase", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4, null,
				OperandTypes.STRING, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction ucase = new SqlFunction("ucase", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4, null,
				OperandTypes.STRING, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction reverse = new SqlFunction("reverse", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4, null,
				OperandTypes.STRING, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction lowercase = new SqlFunction("lowercase", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4, null,
				OperandTypes.STRING, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction ltrim = new SqlFunction("ltrim", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4, null,
				OperandTypes.STRING, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction rtrim = new SqlFunction("rtrim", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4, null,
				OperandTypes.STRING, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction lcase = new SqlFunction("lcase", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4, null,
				OperandTypes.STRING, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction loge = new SqlFunction("loge", SqlKind.OTHER_FUNCTION, ReturnTypes.DOUBLE, null,
				OperandTypes.NUMERIC, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction pow = new SqlFunction("pow", SqlKind.OTHER_FUNCTION, ReturnTypes.DOUBLE, null,
				OperandTypes.NUMERIC_NUMERIC, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction exp = new SqlFunction("exp", SqlKind.OTHER_FUNCTION, ReturnTypes.DOUBLE, null, OperandTypes.NUMERIC,
				SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction ceil = new SqlFunction("ceil", SqlKind.OTHER_FUNCTION, ReturnTypes.DOUBLE, null,
				OperandTypes.NUMERIC, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction floor = new SqlFunction("floor", SqlKind.OTHER_FUNCTION, ReturnTypes.DOUBLE, null,
				OperandTypes.NUMERIC, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction round = new SqlFunction("round", SqlKind.OTHER_FUNCTION, ReturnTypes.DOUBLE, null,
				OperandTypes.NUMERIC, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction abs = new SqlFunction("abs", SqlKind.OTHER_FUNCTION, ReturnTypes.INTEGER, null,
				OperandTypes.NUMERIC, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction currentisodate = new SqlFunction("currentisodate", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4,
				null, OperandTypes.NILADIC, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction curdate = new SqlFunction("curdate", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4,
				null, OperandTypes.NILADIC, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction curtime = new SqlFunction("curtime", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4,
				null, OperandTypes.NILADIC, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction now = new SqlFunction("now", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4,
				null, OperandTypes.NILADIC, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction currenttimemillis = new SqlFunction("current_timemillis", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4,
				null, OperandTypes.NILADIC, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction pii = new SqlFunction("pii", SqlKind.OTHER_FUNCTION, ReturnTypes.DOUBLE,
				null, OperandTypes.NILADIC, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction concat = new SqlFunction("concat", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4, null,
				OperandTypes.STRING_STRING, SqlFunctionCategory.USER_DEFINED_FUNCTION);


		SqlFunction insertstr = new SqlFunction("insertstr", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4, null,
				OperandTypes.STRING_STRING_INTEGER_INTEGER, SqlFunctionCategory.USER_DEFINED_FUNCTION);


		SqlFunction leftchars = new SqlFunction("leftchars", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4, null,
				OperandTypes.STRING_INTEGER, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction rightchars = new SqlFunction("rightchars", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4, null,
				OperandTypes.STRING_INTEGER, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction locate = new SqlFunction("locate", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4, null,
				OperandTypes.STRING_STRING_INTEGER, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlFunction charfunction = new SqlFunction("charac", SqlKind.OTHER_FUNCTION, ReturnTypes.CHAR, null,
				OperandTypes.INTEGER, SqlFunctionCategory.USER_DEFINED_FUNCTION);

		SqlAggFunction groupconcat = new SqlAggFunction("group_concat",
				null,
				SqlKind.OTHER_FUNCTION,
				ReturnTypes.VARCHAR_4,
				null,
				OperandTypes.STRING_STRING,
				SqlFunctionCategory.USER_DEFINED_FUNCTION,
				false,
				false,
				Optionality.FORBIDDEN) {
		};

		//Commons lang functions start
		SqlFunction indexof = new SqlFunction("indexof", SqlKind.OTHER_FUNCTION, ReturnTypes.INTEGER, null,
				OperandTypes.STRING_STRING, SqlFunctionCategory.USER_DEFINED_FUNCTION);
		
		SqlFunction indexofstartpos = new SqlFunction("indexofstartpos", SqlKind.OTHER_FUNCTION, ReturnTypes.INTEGER, null,
				OperandTypes.STRING_STRING_INTEGER, SqlFunctionCategory.USER_DEFINED_FUNCTION);
		
		SqlFunction indexofany = new SqlFunction("indexofany", SqlKind.OTHER_FUNCTION, ReturnTypes.INTEGER, null,
				OperandTypes.STRING_STRING, SqlFunctionCategory.USER_DEFINED_FUNCTION);
		
		SqlFunction indexofanybut = new SqlFunction("indexofanybut", SqlKind.OTHER_FUNCTION, ReturnTypes.INTEGER, null,
				OperandTypes.STRING_STRING, SqlFunctionCategory.USER_DEFINED_FUNCTION);
		
		SqlFunction indexofdiff = new SqlFunction("indexofdiff", SqlKind.OTHER_FUNCTION, ReturnTypes.INTEGER, null,
				OperandTypes.STRING_STRING, SqlFunctionCategory.USER_DEFINED_FUNCTION);
		
		SqlFunction indexofignorecase = new SqlFunction("indexofignorecase", SqlKind.OTHER_FUNCTION, ReturnTypes.INTEGER, null,
				OperandTypes.STRING_STRING, SqlFunctionCategory.USER_DEFINED_FUNCTION);
		
		SqlFunction indexofignorecasestartpos = new SqlFunction("indexofignorecasestartpos", SqlKind.OTHER_FUNCTION, ReturnTypes.INTEGER, null,
				OperandTypes.STRING_STRING_INTEGER, SqlFunctionCategory.USER_DEFINED_FUNCTION);
		
		SqlFunction lastindexof = new SqlFunction("lastindexof", SqlKind.OTHER_FUNCTION, ReturnTypes.INTEGER, null,
				OperandTypes.STRING_STRING, SqlFunctionCategory.USER_DEFINED_FUNCTION);
		
		SqlFunction lastindexofstartpos = new SqlFunction("lastindexofstartpos", SqlKind.OTHER_FUNCTION, ReturnTypes.INTEGER, null,
				OperandTypes.STRING_STRING_INTEGER, SqlFunctionCategory.USER_DEFINED_FUNCTION);
		
		SqlFunction lastindexofany = new SqlFunction("lastindexofany", SqlKind.OTHER_FUNCTION, ReturnTypes.INTEGER, null,
				OperandTypes.STRING_STRING, SqlFunctionCategory.USER_DEFINED_FUNCTION);
		
		SqlFunction leftpad = new SqlFunction("leftpad", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4, null,
				OperandTypes.STRING_INTEGER, SqlFunctionCategory.USER_DEFINED_FUNCTION);
		
		SqlFunction leftpadstring = new SqlFunction("leftpadstring", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4, null,
				OperandTypes.STRING_STRING_INTEGER, SqlFunctionCategory.USER_DEFINED_FUNCTION);
		
		SqlFunction removestring = new SqlFunction("remove", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4, null,
				OperandTypes.STRING_STRING, SqlFunctionCategory.USER_DEFINED_FUNCTION);
		
		SqlFunction removeendstring = new SqlFunction("removeend", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4, null,
				OperandTypes.STRING_STRING, SqlFunctionCategory.USER_DEFINED_FUNCTION);
		
		SqlFunction removeendignorecasestring = new SqlFunction("removeendignorecase", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4, null,
				OperandTypes.STRING_STRING, SqlFunctionCategory.USER_DEFINED_FUNCTION);
		
		SqlFunction removeignorecasestring = new SqlFunction("removeignorecase", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4, null,
				OperandTypes.STRING_STRING, SqlFunctionCategory.USER_DEFINED_FUNCTION);
		
		SqlFunction removestartstring = new SqlFunction("removestart", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4, null,
				OperandTypes.STRING_STRING, SqlFunctionCategory.USER_DEFINED_FUNCTION);
		
		SqlFunction removestartignorecasestring = new SqlFunction("removestartignorecase", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4, null,
				OperandTypes.STRING_STRING, SqlFunctionCategory.USER_DEFINED_FUNCTION);
		
		SqlFunction repeatstring = new SqlFunction("repeat", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4, null,
				OperandTypes.STRING_INTEGER, SqlFunctionCategory.USER_DEFINED_FUNCTION);
		
		SqlFunction repeatseparatorstring = new SqlFunction("repeatseparator", SqlKind.OTHER_FUNCTION, ReturnTypes.VARCHAR_4, null,
				OperandTypes.STRING_STRING_INTEGER, SqlFunctionCategory.USER_DEFINED_FUNCTION);
		//Commons lang functions end
		
		return Arrays.asList(sqrtFunction, lengthFunction, normalizespaces, base64encode, base64decode,
				uppercase, lowercase, loge, pow, exp, ceil, floor, round, abs, currentisodate, concat,
				trimFunction, groupconcat, currenttimemillis, pii, secantfunction, cosecantfunction, cotangentfunction,
				charfunction, insertstr, ucase, lcase, leftchars, rightchars, reverse, locate,
				ltrim, rtrim, curdate, now, curtime, indexof, indexofstartpos, indexofany, indexofanybut,
				indexofdiff, indexofignorecase, indexofignorecasestartpos, lastindexof,
				lastindexofstartpos, lastindexofany, leftpad, leftpadstring, 
				removestring, removeendstring, removeendignorecasestring, removeignorecasestring,
				removestartstring, removestartignorecasestring, repeatstring, repeatseparatorstring);

	}

}
