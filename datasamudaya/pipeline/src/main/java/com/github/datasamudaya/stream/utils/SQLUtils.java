package com.github.datasamudaya.stream.utils;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.DecimalVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.dictionary.Dictionary;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.ipc.ArrowStreamWriter;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.DictionaryEncoding;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.jgroups.util.UUID;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple1;
import org.jooq.lambda.tuple.Tuple10;
import org.jooq.lambda.tuple.Tuple11;
import org.jooq.lambda.tuple.Tuple12;
import org.jooq.lambda.tuple.Tuple13;
import org.jooq.lambda.tuple.Tuple14;
import org.jooq.lambda.tuple.Tuple15;
import org.jooq.lambda.tuple.Tuple16;
import org.jooq.lambda.tuple.Tuple2;
import org.jooq.lambda.tuple.Tuple3;
import org.jooq.lambda.tuple.Tuple4;
import org.jooq.lambda.tuple.Tuple5;
import org.jooq.lambda.tuple.Tuple6;
import org.jooq.lambda.tuple.Tuple7;
import org.jooq.lambda.tuple.Tuple8;
import org.jooq.lambda.tuple.Tuple9;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import com.github.datasamudaya.common.CompressedVectorSchemaRoot;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.FileSystemSupport;

import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.DoubleValue;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LongValue;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SubSelect;

/**
 * This class is utility class for getting datatype of h2 to calcite format.
 * @author arun
 *
 */
public class SQLUtils {
	
	private static Logger log = LoggerFactory.getLogger(SQLUtils.class);
	
	private SQLUtils() {}

	/**
	 * Static function H2 datatype to calcite format.
	 * @param h2datatype
	 * @return type in calcite format
	 */
	public static SqlTypeName getSQLTypeName(String h2datatype) {
		if(h2datatype.equals("4")) {
			return SqlTypeName.INTEGER;
		} else if(h2datatype.equals("8")) {
			return SqlTypeName.DOUBLE;
		} else if(h2datatype.equals("12")) {
			return SqlTypeName.VARCHAR;
		} else {
			return SqlTypeName.VARCHAR;
		}
	}
	
	/**
	 * Static function H2 datatype to calcite format.
	 * @param h2datatype
	 * @return type in calcite format
	 */
	public static SqlTypeName getSQLTypeNameMR(String h2datatype) {
		if(h2datatype.equals("4")) {
			return SqlTypeName.DOUBLE;
		} else if(h2datatype.equals("12")) {
			return SqlTypeName.VARCHAR;
		} else {
			return SqlTypeName.VARCHAR;
		}
	}
	
	/**
	 * This static method converts the value to given format. 
	 * @param value
	 * @param type
	 * @return value in given format.
	 */
	public static Object getValue(String value, SqlTypeName type) {
		try {
			if(type == SqlTypeName.INTEGER) {
				return Integer.valueOf(value);
			} else if(type == SqlTypeName.BIGINT) {
				return Long.valueOf(value);
			} else if(type == SqlTypeName.VARCHAR){
				return String.valueOf(value);
			} else if(type == SqlTypeName.FLOAT) {
				return Float.valueOf(value);			
			} else if(type == SqlTypeName.DOUBLE) {
				return Double.valueOf(value);
			} else {
				return String.valueOf(value);
			}
		} catch(Exception ex) {
			if(type == SqlTypeName.INTEGER) {
				return Integer.valueOf(0);
			} else if(type == SqlTypeName.BIGINT) {
				return Long.valueOf(0);
			} else if(type == SqlTypeName.VARCHAR){
				return String.valueOf(DataSamudayaConstants.EMPTY);
			} else if(type == SqlTypeName.FLOAT) {
				return Float.valueOf(0.0f);			
			} else if(type == SqlTypeName.DOUBLE) {
				return Double.valueOf(0.0d);
			} else {
				return String.valueOf(DataSamudayaConstants.EMPTY);
			}
		}
	}
	
	/**
	 * Get Column Index Map for columns in table
	 * @param column
	 * @return columnindexmap
	 */
	public static Map<String, Integer> getColumnIndexMap(List<String> column){
		Map<String, Integer> columnindexmap = new ConcurrentHashMap<>();
		for (int originalcolumnindex = 0; originalcolumnindex < column.size(); originalcolumnindex++) {
			columnindexmap.put(column.get(originalcolumnindex),
					Integer.valueOf(originalcolumnindex));
		}
		return columnindexmap;
	}
	
	/**
	 * This static method converts the mr values to given format. 
	 * @param value
	 * @param type
	 * @return value in given format.
	 */
	public static Object getValueMR(String value, SqlTypeName type) {
		try {
			if(type == SqlTypeName.INTEGER) {
				return Integer.valueOf(value);
			} else if(type == SqlTypeName.BIGINT) {
				return Long.valueOf(value);
			} else if(type == SqlTypeName.VARCHAR){
				return String.valueOf(value);
			} else if(type == SqlTypeName.FLOAT) {
				return Float.valueOf(value);			
			} else if(type == SqlTypeName.DOUBLE) {
				return Double.valueOf(value);
			} else {
				return String.valueOf(value);
			}
		} catch(Exception ex) {
			if(type == SqlTypeName.INTEGER) {
				return Integer.valueOf(0);
			} else if(type == SqlTypeName.BIGINT) {
				return Long.valueOf(0);
			} else if(type == SqlTypeName.VARCHAR){
				return String.valueOf(DataSamudayaConstants.EMPTY);
			} else if(type == SqlTypeName.FLOAT) {
				return Float.valueOf(0.0f);			
			} else if(type == SqlTypeName.DOUBLE) {
				return Double.valueOf(0.0d);
			} else {
				return String.valueOf(DataSamudayaConstants.EMPTY);
			}
		}
	}
	
	/**
	 * Get Columnar data from Csv
	 * @param records
	 * @param columns
	 * @param sqltypenames
	 * @return arrow vectors
	 */
	public static void getArrowVectors(Stream<CSVRecord> records, List<String> columns,
			Map<String, Integer> columnindexmap
			, List<SqlTypeName> sqltypenames ,
			CompressedVectorSchemaRoot compressedvectorschemaroot) {		
		try {
			RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
			List<Field> fields = new ArrayList<>();
			List<Dictionary> dictionaries = new ArrayList<>();
			for (int columnindex = 0; columnindex < columns.size(); columnindex++) {
				fields.add(getSchemaField(columns.get(columnindex), sqltypenames.get(columnindexmap.get(columns.get(columnindex)))));
			}
			Schema schema = new Schema(fields);
			try {
				VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);
				for (Field field:fields) {
					dictionaries.add(new Dictionary(root.getVector(field), new DictionaryEncoding(666L, false,new ArrowType.Int(32, true))));
				}
				AtomicInteger ai = new AtomicInteger(0);
				records.forEach(record -> {
					int index = ai.getAndIncrement();
					for (int columnindex = 0; columnindex < columns.size(); columnindex++) {
						Object vector = root.getVector(columns.get(columnindex));
						try {
							setValue(index, vector,
									SQLUtils.getValue(record.get(columns.get(columnindex)), sqltypenames.get(columnindexmap.get(columns.get(columnindex)))));
						} catch (Exception e) {
							log.error(DataSamudayaConstants.EMPTY, e);
						}
					}
				});
				final int totalrecordcount = ai.get();
				log.info("Total Record Count in arrow getArrowVector method {}",totalrecordcount);
				compressedvectorschemaroot.setRecordcount(totalrecordcount);
				columns.stream().forEachOrdered(col -> {
					Object vector =  root.getVector(col);
					if (vector instanceof IntVector iv) {
						iv.setValueCount(totalrecordcount);
					} else if (vector instanceof VarCharVector vcv) {
						vcv.setValueCount(totalrecordcount);
					} else if (vector instanceof Float4Vector f4v) {
						f4v.setValueCount(totalrecordcount);
					} else if (vector instanceof Float8Vector f8v) {
						f8v.setValueCount(totalrecordcount);
					} else if (vector instanceof DecimalVector dv) {
						dv.setValueCount(totalrecordcount);
					}
				});
				String arrowfilewithpath = getArrowFilePath();
				File file = new File(arrowfilewithpath);				
				try(FileOutputStream out = new FileOutputStream(file);
						SnappyOutputStream snappyOutputStream = new SnappyOutputStream(out);
				ArrowStreamWriter writer = new ArrowStreamWriter(root,null, snappyOutputStream);){
					writer.start();
					writer.writeBatch();
					writer.end();
					String key = UUID.randomUUID().toString();
					columns.stream().forEachOrdered(col -> {
						compressedvectorschemaroot.getColumnvectorschemarootkeymap().put(col, key);
						compressedvectorschemaroot.getVectorschemarootkeybytesmap().put(key, arrowfilewithpath);
					});
					file.deleteOnExit();
				}
			} catch (IOException e) {
				log.error(DataSamudayaConstants.EMPTY, e);
			}finally {}
		} finally {

		}
	}
	
	/**
	 * Get Columnar data from Csv
	 * @param records
	 * @param columns
	 * @param columnindexmap
	 * @param sqltypenames
	 * @param compressedvectorschemaroot
	 * @param reccount
	 * @return schemaroot
	 */
	public static VectorSchemaRoot getArrowVectors(Stream<CSVRecord> records, List<String> columns,
			Map<String, Integer> columnindexmap
			, List<SqlTypeName> sqltypenames ,
			CompressedVectorSchemaRoot compressedvectorschemaroot,
			int reccount) {		
		try {
			RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
			List<Field> fields = new ArrayList<>();
			for (int columnindex = 0; columnindex < columns.size(); columnindex++) {
				fields.add(getSchemaField(columns.get(columnindex), sqltypenames.get(columnindexmap.get(columns.get(columnindex)))));
			}
			Schema schema = new Schema(fields);
			try {
				VectorSchemaRoot root = VectorSchemaRoot.create(schema, allocator);	
				root.allocateNew();
				for(String column:columns) {
					Object vector = root.getVector(column);
					allocateNewCapacity(vector, reccount);
					log.info("Allocating {} for new capacity {} for column {}", vector.getClass().getName(), reccount, column);					
				}
				AtomicInteger ai = new AtomicInteger(0);
				records.sequential().forEach(record -> {
					int index = ai.getAndIncrement();
					if(index<reccount) {
						for (int columnindex = 0; columnindex < columns.size(); columnindex++) {
							Object vector = root.getVector(columns.get(columnindex));
							try {
								setValue(index, vector,
										SQLUtils.getValue(record.get(columns.get(columnindex)), sqltypenames.get(columnindexmap.get(columns.get(columnindex)))));
							} catch (Exception e) {
								log.error("{}", e);
							}
						}
					} else {
						log.info("Index {} Exceeded Limit {}", index,reccount);
					}
				});
				final int totalrecordcount = ai.get();
				compressedvectorschemaroot.setRecordcount(reccount);
				columns.stream().forEachOrdered(col -> {
					Object vector =  root.getVector(col);
					if (vector instanceof IntVector iv) {
						iv.setValueCount(totalrecordcount);
					} else if (vector instanceof VarCharVector vcv) {
						vcv.setValueCount(totalrecordcount);
					} else if (vector instanceof Float4Vector f4v) {
						f4v.setValueCount(totalrecordcount);
					} else if (vector instanceof Float8Vector f8v) {
						f8v.setValueCount(totalrecordcount);
					} else if (vector instanceof DecimalVector dv) {
						dv.setValueCount(totalrecordcount);
					}
				});
				String arrowfilewithpath = getArrowFilePath();
				File file = new File(arrowfilewithpath);				
				try(FileOutputStream out = new FileOutputStream(file);
						SnappyOutputStream snappyOutputStream = new SnappyOutputStream(out);
				ArrowStreamWriter writer = new ArrowStreamWriter(root,null, snappyOutputStream);){
					writer.start();
					writer.writeBatch();
					writer.end();
					String key = UUID.randomUUID().toString();
					columns.stream().forEachOrdered(col -> {
						compressedvectorschemaroot.getColumnvectorschemarootkeymap().put(col, key);
						compressedvectorschemaroot.getVectorschemarootkeybytesmap().put(key, arrowfilewithpath);
					});
					file.deleteOnExit();
				}
				return root;
			} catch (IOException e) {
				log.error(DataSamudayaConstants.EMPTY, e);
			}finally {}
		} finally {

		}
		return null;
	}
	
	/**
	 * Get arrow file to store columnar data
	 * @return
	 */
	protected static String getArrowFilePath() {
		String tmpdir = isNull(DataSamudayaProperties.get())?System.getProperty(DataSamudayaConstants.TMPDIR):
			DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TMPDIR, System.getProperty(DataSamudayaConstants.TMPDIR));
		new File(tmpdir + DataSamudayaConstants.FORWARD_SLASH + 
				FileSystemSupport.MDS).mkdirs();
		return tmpdir + DataSamudayaConstants.FORWARD_SLASH + 
				FileSystemSupport.MDS + DataSamudayaConstants.FORWARD_SLASH + UUID.randomUUID().toString() 
				+ DataSamudayaConstants.ARROWFILE_EXT;
	}
	
	/**
	 * Decompress VectorSchemaRoot from bytes
	 * @param bytes
	 * @return VectorSchemaRoot
	 * @throws Exception
	 */
	public static ArrowStreamReader decompressVectorSchemaRootBytes(String arrowfilewithpath) throws Exception {
		try(FileInputStream in = new FileInputStream(arrowfilewithpath);
				SnappyInputStream snappyInputStream = new SnappyInputStream(in);){
			ArrowStreamReader reader = new ArrowStreamReader(snappyInputStream, new RootAllocator());
			if(reader.loadNextBatch()) {
				return reader;
			}
		} catch(Exception ex) {
			log.error(DataSamudayaConstants.EMPTY,ex);
		}
		return null;
		
	}
	
	/**
	 * Gets Schema Field for given column and type.
	 * @param column
	 * @param type
	 * @return Field
	 */
	protected static Field getSchemaField(String column, SqlTypeName type) {
		if(type == SqlTypeName.INTEGER) {
			return Field.nullable(column, new ArrowType.Int(32, true));
		} else if(type == SqlTypeName.BIGINT) {
			return Field.nullable(column, new ArrowType.Int(32, true));
		} else if(type == SqlTypeName.VARCHAR){
			return Field.nullable(column, new ArrowType.Utf8());
		} else if(type == SqlTypeName.FLOAT) {
			return Field.nullable(column, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
		} else if(type == SqlTypeName.DOUBLE) {
			return Field.nullable(column, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
		} else {
			return Field.nullable(column, new ArrowType.Utf8());
		}
	}
	
	/**
	 * Get Vectors based on Column Type
	 * @param column
	 * @param type
	 * @param allocator
	 * @return vector of given sql types
	 */
	protected static Object getVector(String column, SqlTypeName type, BufferAllocator allocator) {
		if(type == SqlTypeName.INTEGER) {
			IntVector vector = new IntVector(column, allocator);
			return vector;
		} else if(type == SqlTypeName.BIGINT) {
			IntVector vector = new IntVector(column, allocator);
			return vector;
		} else if(type == SqlTypeName.VARCHAR){
			VarCharVector vector = new VarCharVector(column, allocator);
			return vector;
		} else if(type == SqlTypeName.FLOAT) {
			Float4Vector vector = new Float4Vector(column, allocator);
			return vector;
		} else if(type == SqlTypeName.DOUBLE) {
			Float8Vector vector = new Float8Vector(column, allocator);
			return vector;
		} else {
			VarCharVector vector = new VarCharVector(column, allocator);
			return vector;
		}
	}
	
	/**
	 * Sets Value in Arrow Vector for given value and index
	 * @param index
	 * @param vector
	 * @param value
	 * @throws Exception 
	 */
	protected static void setValue(int index, Object vector, Object value) throws Exception {
		if(vector instanceof IntVector iv) {
			iv.setSafe(index, (int) value);
		} else if(vector instanceof VarCharVector vcv) {
			byte[] bt = ((String)value).getBytes();
			vcv.setSafe(index, bt);
		} else if(vector instanceof Float4Vector f4v) {			
			f4v.setSafe(index, (float) value);
		} else if(vector instanceof Float8Vector f8v) {			
			f8v.setSafe(index, (double) value);
		}
	}
	
	/**
	 * Allocates vector for capacity
	 * @param vector
	 * @param capacity
	 */
	protected static void allocateNewCapacity(Object vector, int capacity) {
		if(vector instanceof IntVector iv) {
			iv.allocateNew(capacity);
		} else if(vector instanceof VarCharVector vcv) {
			vcv.allocateNew(capacity);
		} else if(vector instanceof Float4Vector f4v) {			
			f4v.allocateNew(capacity);
		} else if(vector instanceof Float8Vector f8v) {			
			f8v.allocateNew(capacity);
		}
	}
	
	/**
	 * Gets value from vector for given index.
	 * @param index
	 * @param vector
	 * @return object value
	 */
	public static Object getVectorValue(int index, Object vector) {
		if(vector instanceof IntVector iv) {
			return iv.get(index);
		} else if(vector instanceof VarCharVector vcv) {
			return new String(vcv.get(index));
		} else if(vector instanceof Float4Vector f4v) {			
			return f4v.get(index);
		} else if(vector instanceof Float8Vector f8v) {			
			return f8v.get(index);
		}
		return DataSamudayaConstants.EMPTY;
	}
	
	/**
	 * Get all columns from expression
	 * @param expression
	 * @param columns
	 */
	public static void getColumnsFromExpression(Expression expression, List<Column> columns) {
		if(expression instanceof BinaryExpression bex) {
		    Expression leftExpression = bex.getLeftExpression();
		    Expression rightExpression = bex.getRightExpression();
		    if (leftExpression instanceof BinaryExpression) {
		    	getColumnsFromExpression((BinaryExpression) leftExpression, columns);
		    }else if(leftExpression instanceof Column column) {
				columns.add(column);
			}
		    if (rightExpression instanceof BinaryExpression) {
		    	getColumnsFromExpression((BinaryExpression) rightExpression, columns);
		    } else if(rightExpression instanceof Column column) {
				columns.add(column);
			} else if (leftExpression instanceof Parenthesis) {
				Parenthesis parenthesis = (Parenthesis) leftExpression;
				Expression subExpression = parenthesis.getExpression();
				getColumnsFromExpression(subExpression, columns);
			}
		    
		} else if(expression instanceof Column column) {
			columns.add(column);
		} else if (expression instanceof Parenthesis) {
			Parenthesis parenthesis = (Parenthesis) expression;
			Expression subExpression = parenthesis.getExpression();
			getColumnsFromExpression(subExpression, columns);
		}
	}
	
	public static Object evaluateFunctionsWithType(Object value, Object powerval, String name) {
		switch (name) {
		case "abs":
			// Get the absolute value of the first parameter
			if (value instanceof Double dv) {
				return Math.abs(dv);
			} else if (value instanceof Long lv) {
				return Math.abs(lv);
			} else if (value instanceof Float fv) {
				return Math.abs(fv);
			} else if (value instanceof Integer iv) {
				return Math.abs(iv);
			}
		case "length":
			// Get the length of string value
			String val = (String) value;
			// return the result to the stack
			return Long.valueOf(val.length());

		case "round":
			// Get the absolute value of the first parameter
			if (value instanceof Double dv) {
				return Math.round(dv);
			} else if (value instanceof Long lv) {
				return Math.round(lv);
			} else if (value instanceof Float fv) {
				return Math.round(fv);
			} else if (value instanceof Integer iv) {
				return Math.round(iv);
			}
		case "ceil":
			// Get the absolute value of the first parameter
			if (value instanceof Double dv) {
				return Math.ceil(dv);
			} else if (value instanceof Long lv) {
				return Math.ceil(lv);
			} else if (value instanceof Float fv) {
				return Math.ceil(fv);
			} else if (value instanceof Integer iv) {
				return Math.ceil(iv);
			}
		case "floor":
			// Get the absolute value of the first parameter
			if (value instanceof Double dv) {
				return Math.floor(dv);
			} else if (value instanceof Long lv) {
				return Math.floor(lv);
			} else if (value instanceof Float fv) {
				return Math.floor(fv);
			} else if (value instanceof Integer iv) {
				return Math.floor(iv);
			}
		case "pow":
			// Get the absolute value of the first parameter
			if (value instanceof Double dv && powerval instanceof Integer powval) {
				return Math.pow(dv, powval);
			} else if (value instanceof Long lv && powerval instanceof Integer powval) {
				return Math.pow(lv, powval);
			} else if (value instanceof Float fv && powerval instanceof Integer powval) {
				return Math.pow(fv, powval);
			} else if (value instanceof Integer iv && powerval instanceof Integer powval) {
				return Math.pow(iv, powval);
			}
		case "sqrt":
			// Get the absolute value of the first parameter
			if (value instanceof Double dv) {
				return Math.sqrt(dv);
			} else if (value instanceof Long lv) {
				return Math.sqrt(lv);
			} else if (value instanceof Float fv) {
				return Math.sqrt(fv);
			} else if (value instanceof Integer iv) {
				return Math.sqrt(iv);
			}
		case "exp":
			// Get the absolute value of the first parameter
			if (value instanceof Double dv) {
				return Math.exp(dv);
			} else if (value instanceof Long lv) {
				return Math.exp(lv);
			} else if (value instanceof Float fv) {
				return Math.exp(fv);
			} else if (value instanceof Integer iv) {
				return Math.exp(iv);
			}
		case "loge":
			// Get the absolute value of the first parameter
			if (value instanceof Double dv) {
				return Math.log(dv);
			} else if (value instanceof Long lv) {
				return Math.log(lv);
			} else if (value instanceof Float fv) {
				return Math.log(fv);
			} else if (value instanceof Integer iv) {
				return Math.log(iv);
			}
		case "lowercase":
			// Get the absolute value of the first parameter
			return ((String) value).toLowerCase();
		case "uppercase":
			// Get the absolute value of the first parameter
			return ((String) value).toUpperCase();
		case "base64encode":
			// Get the absolute value of the first parameter
			return Base64.getEncoder().encodeToString(((String) value).getBytes());
		case "base64decode":
			// Get the absolute value of the first parameter
			return new String(Base64.getDecoder().decode(((String) value).getBytes()));
		case "normalizespaces":
			// Get the absolute value of the first parameter
			return StringUtils.normalizeSpace((String) value);
		}
		return name;
	}
	
	/**
	 * Evaluate non aggregate expression for addition, subtraction, multiplication and division 
	 * and some math functions
	 * @param expression
	 * @param row
	 * @return
	 */
	public static Object evaluateBinaryExpression(Expression expression, Map<String,Object> row) {
		if(expression instanceof Function fn) {
			String name = fn.getName().toLowerCase();
			List<Expression> expfunc = fn.getParameters().getExpressions();
			switch (name) {
				case "abs":
	                // Get the absolute value of the first parameter	               
	                return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row), null, "abs");
				case "length":
	                // Get the length of string value	                
	                return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row), null, "length");
	                
				case "round":
	                // Get the absolute value of the first parameter
	                return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row), null, "round");
				case "ceil":
	                // Get the absolute value of the first parameter
	                return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row), null, "ceil");
				case "floor":
	                // Get the absolute value of the first parameter
	                return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row), null, "floor");
				case "pow":
	                // Get the absolute value of the first parameter
	                return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row), evaluateBinaryExpression(expfunc.get(1), row), "pow");
				case "sqrt":
	                // Get the absolute value of the first parameter
	                return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row), null, "sqrt");
				case "exp":
	                // Get the absolute value of the first parameter
	                return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row), null, "exp");
				case "loge":
	                // Get the absolute value of the first parameter
	                return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row), null, "loge");
				case "lowercase":
	                // Get the absolute value of the first parameter
	                return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row), null, "lowercase");
				case "uppercase":
	                // Get the absolute value of the first parameter
	                return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row), null, "uppercase");
				case "base64encode":
	                // Get the absolute value of the first parameter
	                return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row), null, "base64encode");
				case "base64decode":
	                // Get the absolute value of the first parameter
	                return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row), null, "base64decode");
				case "normalizespaces":
	                // Get the absolute value of the first parameter
	                return evaluateFunctionsWithType(evaluateBinaryExpression(expfunc.get(0), row), null, "normalizespaces");
				case "substring":
	                // Get the absolute value of the first parameter
					LongValue pos = (LongValue) expfunc.get(1);
					LongValue length = (LongValue) expfunc.get(2);
					String val = (String)evaluateBinaryExpression(expfunc.get(0), row);
	                return (val).substring((int)pos.getValue(), Math.min(((String)val).length(),(int)pos.getValue()+(int)length.getValue()));
			}
		} else if(expression instanceof BinaryExpression bex) {
		    Expression leftExpression = bex.getLeftExpression();
		    Expression rightExpression = bex.getRightExpression();
		    String operator = bex.getStringExpression();
		    Object leftValue=null;
		    Object rightValue=null;
		    if(leftExpression instanceof Function fn) {
		    	leftValue = evaluateBinaryExpression(leftExpression, row);
		    }
		    else if (leftExpression instanceof LongValue lv) {
		    	leftValue =  lv.getValue();
		    } else if (leftExpression instanceof DoubleValue dv) {
		    	leftValue =  dv.getValue();
		    } else if (leftExpression instanceof StringValue sv) {
		    	leftValue = sv.getValue();
		    } else if (leftExpression instanceof Column) {
		        Column column = (Column) leftExpression;	        
		        String columnName = column.getColumnName();
				Object value = row.get(columnName);
				leftValue =  value;
		    } else if (leftExpression instanceof BinaryExpression) {
		    	leftValue = evaluateBinaryExpression(leftExpression, row);
		    } else if (leftExpression instanceof Parenthesis) {
				Parenthesis parenthesis = (Parenthesis) leftExpression;
				Expression subExpression = parenthesis.getExpression();
				leftValue = evaluateBinaryExpression(subExpression, row);
			}
		    
		    if(rightExpression instanceof Function fn) {
		    	rightValue = evaluateBinaryExpression(rightExpression, row);
		    } else if (rightExpression instanceof LongValue lv) {
		    	rightValue = lv.getValue();
		    }else if (rightExpression instanceof DoubleValue dv) {
		    	rightValue = dv.getValue();
		    } else if (rightExpression instanceof StringValue sv) {
		    	rightValue = sv.getValue();
		    } else if (rightExpression instanceof Column) {
		        Column column = (Column) rightExpression;
		        Object value = row.get(column.getColumnName());
		        rightValue = value;
		    } else if (rightExpression instanceof BinaryExpression) {
		    	rightValue = evaluateBinaryExpression((BinaryExpression) rightExpression, row);
		    } else if (rightExpression instanceof Parenthesis) {
				Parenthesis parenthesis = (Parenthesis) rightExpression;
				Expression subExpression = parenthesis.getExpression();
				rightValue = evaluateBinaryExpression(subExpression, row);
			}
		    switch (operator) {
		        case "+":
		            return evaluateValuesByOperator(leftValue,rightValue, operator);
		        case "-":
		            return evaluateValuesByOperator(leftValue,rightValue, operator);
		        case "*":
		            return evaluateValuesByOperator(leftValue,rightValue, operator);
		        case "/":
		            return evaluateValuesByOperator(leftValue,rightValue, operator);
		        default:
		            throw new IllegalArgumentException("Invalid operator: " + operator);
		    }
		} 
		else if (expression instanceof LongValue lv) {
	        return Double.valueOf(lv.getValue());
	    } else if (expression instanceof DoubleValue dv) {
	        return dv.getValue();
	    } else if (expression instanceof StringValue sv) {
	        return sv.getValue();
	    }  else if (expression instanceof Parenthesis parenthesis) {
	        return evaluateBinaryExpression(parenthesis.getExpression(), row);
	    } else if (expression instanceof Column) {
	        Column column = (Column) expression;
	        Object value = row.get(column.getColumnName());
	        return value;
	    }
		return Double.valueOf(0.0d);
	}

	/**
	 * Evaluates tuple using operator
	 * @param leftValue
	 * @param rightValue
	 * @param operator
	 * @return evaluated value
	 */
	public static Object evaluateValuesByOperator(Object leftValue, Object rightValue, String operator) {
		switch (operator) {
		case "+":
			if (leftValue instanceof String lv && rightValue instanceof Double rv) {
				return lv + rv;
			} else if(leftValue instanceof Double lv && rightValue instanceof Double rv) {
				return lv + rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Double rv) {
				return lv + rv;
			} else if(leftValue instanceof Double lv && rightValue instanceof Long rv) {
				return lv + rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Long rv) {
				return lv + rv;
			} else if(leftValue instanceof String lv && rightValue instanceof Long rv) {
				return lv + rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof String rv) {
				return lv + rv;
			} else if(leftValue instanceof Double lv && rightValue instanceof String rv) {
				return lv + rv;
			} else if(leftValue instanceof Integer lv && rightValue instanceof Integer rv) {
				return lv + rv;
			} else if (leftValue instanceof String lv && rightValue instanceof String rv) {
				return lv + rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Integer rv) {
				return lv + rv;
			} else if(leftValue instanceof Integer lv && rightValue instanceof Long rv) {
				return lv + rv;
			}
		case "-":
			if(leftValue instanceof Double lv && rightValue instanceof Double rv) {
				return lv - rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Double rv) {
				return lv - rv;
			} else if(leftValue instanceof Double lv && rightValue instanceof Long rv) {
				return lv - rv;
			}  else if(leftValue instanceof Integer lv && rightValue instanceof Integer rv) {
				return lv - rv;
			} else if(leftValue instanceof Integer lv && rightValue instanceof Long rv) {
				return lv - rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Integer rv) {
				return lv - rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Long rv) {
				return lv - rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Integer rv) {
				return lv - rv;
			} else if(leftValue instanceof Integer lv && rightValue instanceof Long rv) {
				return lv - rv;
			}
		case "*":
			if(leftValue instanceof Double lv && rightValue instanceof Double rv) {
				return lv * rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Double rv) {
				return lv * rv;
			} else if(leftValue instanceof Double lv && rightValue instanceof Integer rv) {
				return lv * rv;
			} else if(leftValue instanceof Integer lv && rightValue instanceof Double rv) {
				return lv * rv;
			} else if(leftValue instanceof Double lv && rightValue instanceof Long rv) {
				return lv * rv;
			} else if(leftValue instanceof Integer lv && rightValue instanceof Integer rv) {
				return lv * rv;
			} else if(leftValue instanceof Integer lv && rightValue instanceof Long rv) {
				return lv * rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Integer rv) {
				return lv * rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Long rv) {
				return lv * rv;
			}
		case "/":
			if(leftValue instanceof Double lv && rightValue instanceof Double rv) {
				return lv / rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Double rv) {
				return lv / rv;
			} else if(leftValue instanceof Double lv && rightValue instanceof Integer rv) {
				return lv / rv;
			} else if(leftValue instanceof Integer lv && rightValue instanceof Double rv) {
				return lv / rv;
			} else if(leftValue instanceof Double lv && rightValue instanceof Long rv) {
				return lv / rv;
			} else if(leftValue instanceof Integer lv && rightValue instanceof Integer rv) {
				return lv / (double)rv;
			} else if(leftValue instanceof Integer lv && rightValue instanceof Long rv) {
				return lv / (double) rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Integer rv) {
				return lv / (double) rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Long rv) {
				return lv / (double)rv;
			}
		default:
			throw new IllegalArgumentException("Invalid operator: " + operator);
		}
		
	}
	
	/**
	 * Get list of columns from list of functions.
	 * 
	 * @param functions
	 * @return list of columns
	 */
	public static List<Expression> getExpressions(Function function) {
		if(nonNull(function.getParameters()))
		return function.getParameters().getExpressions();
		return null;
	}

	
	
	public static Object evaluateValuesByFunctionMin(Object leftValue, Object rightValue) {
		if(leftValue instanceof Double lv && rightValue instanceof Double rv) {
			return Math.min(lv, rv);
		} else if(leftValue instanceof Long lv && rightValue instanceof Long rv) {
			return Math.min(lv, rv);
		} else if(leftValue instanceof Integer lv && rightValue instanceof Integer rv) {
			return Math.min(lv, rv);
		} else if(leftValue instanceof Float lv && rightValue instanceof Float rv) {
			return Math.min(lv, rv);
		} else {
			throw new IllegalArgumentException("Unknown type: " + leftValue+" "+rightValue);
		}
	}
	
	public static Object evaluateValuesByFunctionMax(Object leftValue, Object rightValue) {
		if(leftValue instanceof Double lv && rightValue instanceof Double rv) {
			return Math.max(lv, rv);
		} else if(leftValue instanceof Long lv && rightValue instanceof Long rv) {
			return Math.max(lv, rv);
		} else if(leftValue instanceof Integer lv && rightValue instanceof Integer rv) {
			return Math.max(lv, rv);
		} else if(leftValue instanceof Float lv && rightValue instanceof Float rv) {
			return Math.max(lv, rv);
		} else {
			throw new IllegalArgumentException("Unknown type: " + leftValue+" "+rightValue);
		}
	}

	/**
	 * Evaluates the expression with row to get value.
	 * 
	 * @param expression
	 * @param row
	 * @return get the record column data from expression.
	 */
	private static Object getValueString(Expression expression, Map<String, Object> row) {
		if (expression instanceof LongValue) {
			return ((LongValue) expression).getValue();
		} else if (expression instanceof StringValue) {
			return ((StringValue) expression).getValue();
		} else if (expression instanceof DoubleValue) {
			return Double.valueOf(((DoubleValue) expression).getValue());
		} else if(expression instanceof Addition addition){
			return evaluateValuesByOperator(getValueString(addition.getLeftExpression() , row),
					getValueString(addition.getRightExpression() , row), "+");
		} else if(expression instanceof Subtraction subtraction){
			return evaluateValuesByOperator(getValueString(subtraction.getLeftExpression() , row),
					getValueString(subtraction.getRightExpression() , row), "-");
		} else if(expression instanceof Multiplication multiplication){
			return evaluateValuesByOperator(getValueString(multiplication.getLeftExpression() , row),
					getValueString(multiplication.getRightExpression() , row), "*");
		} else if(expression instanceof Division division){
			return evaluateValuesByOperator(getValueString(division.getLeftExpression() , row),
					getValueString(division.getRightExpression() , row), "/");
		}
		else {
			Column column = (Column) expression;
			String columnName = column.getColumnName();
			Object value = row.get(columnName);
			if(value instanceof String stringval) {
				return String.valueOf(stringval);
			}
			else if(value instanceof Double doubleval) {
				return doubleval;
			}
			else if(value instanceof Integer intval) {
				return intval;
			} else if(value instanceof Long longval) {
				return longval;
			}else if(value instanceof String stringval && NumberUtils.isParsable(stringval))
			return Double.valueOf(stringval);
			return String.valueOf(value);
		}
	}

	/**
	 * This function evaluates the expression for a given row record.
	 * 
	 * @param expression
	 * @param row
	 * @return evaluates to true if expression satisfies the condition else false
	 */
	public static boolean evaluateExpression(Expression expression, Map<String,Object> row) {
		if (expression instanceof Between) {
	        Between betweenExpression = (Between) expression;
	        Expression leftExpression = betweenExpression.getLeftExpression();
	        Expression startExpression = betweenExpression.getBetweenExpressionStart();
	        Expression endExpression = betweenExpression.getBetweenExpressionEnd();
	        Object leftValue = getValueString(leftExpression, row);
	        Object startExpressionValue = getValueString(startExpression, row);
	        Object endExpressionValue = getValueString(endExpression, row);
	        if(evaluatePredicate(leftValue, startExpressionValue, ">") && evaluatePredicate(leftValue, endExpressionValue, "<")) {
	        	return true;
	        }
	        return false;
		}
		else if (expression instanceof InExpression) {
	        InExpression inExpression = (InExpression) expression;
	        Expression leftExpression = inExpression.getLeftExpression();
	        ItemsList itemsList = inExpression.getRightItemsList();

	        // Handle the left expression
	        Object leftValue = getValueString(leftExpression, row);

	        // Handle the IN clause values
	        if (itemsList instanceof ExpressionList) {
	            ExpressionList expressionList = (ExpressionList) itemsList;
	            List<Expression> inValues = expressionList.getExpressions();

	            // Process each value in the IN clause
	            for (Expression expressioninlist : inValues) {
	            	Object rightValue = getValueString(expressioninlist, row);	            	
	            	if(evaluatePredicate(leftValue, rightValue, "=")) {
	            		return true;
	            	}
	            }	            
	        }
	        return false;

	    } else if (expression instanceof LikeExpression) {
	        LikeExpression likeExpression = (LikeExpression) expression;
	        Expression leftExpression = likeExpression.getLeftExpression();
	        Expression rightExpression = likeExpression.getRightExpression();

	        // Handle the left expression
	        String leftValue = (String) getValueString(leftExpression, row);

	        // Handle the right expression (LIKE pattern)
	        String rightValue = (String) getValueString(rightExpression, row);
	        
	        return leftValue.contains(rightValue);

	    } else if (expression instanceof BinaryExpression) {
			BinaryExpression binaryExpression = (BinaryExpression) expression;
			String operator = binaryExpression.getStringExpression();
			Expression leftExpression = binaryExpression.getLeftExpression();
			Expression rightExpression = binaryExpression.getRightExpression();

			switch (operator.toUpperCase()) {
			case "AND":
				return evaluateExpression(leftExpression, row) && evaluateExpression(rightExpression, row);
			case "OR":
				return evaluateExpression(leftExpression, row) || evaluateExpression(rightExpression, row);
			case ">":
				Object leftValue = getValueString(leftExpression, row);
				Object rightValue = getValueString(rightExpression, row);
				return evaluatePredicate(leftValue, rightValue, operator);
			case ">=":
				leftValue = getValueString(leftExpression, row);
				rightValue = getValueString(rightExpression, row);
				return evaluatePredicate(leftValue, rightValue, operator);
			case "<":
				leftValue = getValueString(leftExpression, row);
				rightValue = getValueString(rightExpression, row);
				return evaluatePredicate(leftValue, rightValue, operator);
			case "<=":
				leftValue = getValueString(leftExpression, row);
				rightValue = getValueString(rightExpression, row);
				return evaluatePredicate(leftValue, rightValue, operator);
			case "=":
				leftValue = getValueString(leftExpression, row);
				rightValue = getValueString(rightExpression, row);
				return evaluatePredicate(leftValue, rightValue, operator);
			case "<>":
				leftValue = getValueString(leftExpression, row);
				rightValue = getValueString(rightExpression, row);
				return evaluatePredicate(leftValue, rightValue, operator);			
			default:
				throw new UnsupportedOperationException("Unsupported operator: " + operator);
			}
		} else if (expression instanceof Parenthesis) {
			Parenthesis parenthesis = (Parenthesis) expression;
			Expression subExpression = parenthesis.getExpression();
			return evaluateExpression(subExpression, row);
		} else {
			Object value = getValueString(expression, row);
			return Boolean.parseBoolean((String) value);
		}
	}

	public static boolean evaluatePredicate(Object leftvalue,Object rightvalue, String operator) {
		switch (operator.trim()) {
		case ">":
			if(leftvalue instanceof Double lv && rightvalue instanceof Double rv) {
				return lv > rv;
			} else if(leftvalue instanceof Long lv && rightvalue instanceof Double rv) {
				return lv > rv;
			} else if(leftvalue instanceof Double lv && rightvalue instanceof Long rv) {
				return lv > rv;
			} else if(leftvalue instanceof Integer lv && rightvalue instanceof Double rv) {
				return lv > rv;
			} else if(leftvalue instanceof Double lv && rightvalue instanceof Integer rv) {
				return lv > rv;
			} else if(leftvalue instanceof Integer lv && rightvalue instanceof Integer rv) {
				return lv > rv;
			} else if(leftvalue instanceof Integer lv && rightvalue instanceof Long rv) {
				return lv > rv;
			} else if(leftvalue instanceof Long lv && rightvalue instanceof Integer rv) {
				return lv > rv;
			} else if(leftvalue instanceof Long lv && rightvalue instanceof Long rv) {
				return lv > rv;
			} else {
				return false;
			}
		case ">=":
			if(leftvalue instanceof Double lvgt && rightvalue instanceof Double rvgt) {
				return lvgt >= rvgt;
			} else if(leftvalue instanceof Long lvgt && rightvalue instanceof Double rvgt) {
				return lvgt >= rvgt;
			} else if(leftvalue instanceof Double lvgt && rightvalue instanceof Long rvgt) {
				return lvgt >= rvgt;
			} else if(leftvalue instanceof Integer lvgt && rightvalue instanceof Double rvgt) {
				return lvgt >= rvgt;
			} else if(leftvalue instanceof Double lvgt && rightvalue instanceof Integer rvgt) {
				return lvgt >= rvgt;
			} else if(leftvalue instanceof Integer lvgt && rightvalue instanceof Integer rvgt) {
				return lvgt >= rvgt;
			} else if(leftvalue instanceof Integer lvgt && rightvalue instanceof Long rvgt) {
				return lvgt >= rvgt;
			} else if(leftvalue instanceof Long lvgt && rightvalue instanceof Integer rvgt) {
				return lvgt >= rvgt;
			} else if(leftvalue instanceof Long lvgt && rightvalue instanceof Long rvgt) {
				return lvgt >= rvgt;
			} else {
				return false;
			}
		case "<":
			if(leftvalue instanceof Double lvlt && rightvalue instanceof Double rvlt) {
				return lvlt < rvlt;
			} else if(leftvalue instanceof Long lvlt && rightvalue instanceof Double rvlt) {
				return lvlt < rvlt;
			} else if(leftvalue instanceof Double lvlt && rightvalue instanceof Long rvlt) {
				return lvlt < rvlt;
			} else if(leftvalue instanceof Integer lvlt && rightvalue instanceof Double rvlt) {
				return lvlt < rvlt;
			} else if(leftvalue instanceof Double lvlt && rightvalue instanceof Integer rvlt) {
				return lvlt < rvlt;
			} else if(leftvalue instanceof Integer lvlt && rightvalue instanceof Integer rvlt) {
				return lvlt < rvlt;
			} else if(leftvalue instanceof Integer lvlt && rightvalue instanceof Long rvlt) {
				return lvlt < rvlt;
			} else if(leftvalue instanceof Long lvlt && rightvalue instanceof Integer rvlt) {
				return lvlt < rvlt;
			} else if(leftvalue instanceof Long lvlt && rightvalue instanceof Long rvlt) {
				return lvlt < rvlt;
			} else {
				return false;
			}
		case "<=":
			if(leftvalue instanceof Double lvle && rightvalue instanceof Double rvle) {
				return lvle <= rvle;
			} else if(leftvalue instanceof Long lvle && rightvalue instanceof Double rvle) {
				return lvle <= rvle;
			} else if(leftvalue instanceof Double lvle && rightvalue instanceof Long rvle) {
				return lvle <= rvle;
			} else if(leftvalue instanceof Integer lvle && rightvalue instanceof Double rvle) {
				return lvle <= rvle;
			} else if(leftvalue instanceof Double lvle && rightvalue instanceof Integer rvle) {
				return lvle <= rvle;
			} else if(leftvalue instanceof Integer lvle && rightvalue instanceof Integer rvle) {
				return lvle <= rvle;
			} else if(leftvalue instanceof Integer lvle && rightvalue instanceof Long rvle) {
				return lvle <= rvle;
			} else if(leftvalue instanceof Long lvle && rightvalue instanceof Integer rvle) {
				return lvle <= rvle;
			} else if(leftvalue instanceof Long lvle && rightvalue instanceof Long rvle) {
				return lvle <= rvle;
			} else {
				return false;
			}
		case "=":
			if(leftvalue instanceof Double lveq && rightvalue instanceof Double rveq) {
				return lveq == rveq;
			} else if(leftvalue instanceof Long lveq && rightvalue instanceof Double rveq) {
				return lveq.doubleValue() == rveq.doubleValue();
			} else if(leftvalue instanceof Double lveq && rightvalue instanceof Long rveq) {
				return lveq.doubleValue() == rveq.doubleValue();
			} else if(leftvalue instanceof Integer lveq && rightvalue instanceof Double rveq) {
				return lveq.doubleValue() == rveq.doubleValue();
			} else if(leftvalue instanceof Double lveq && rightvalue instanceof Integer rveq) {
				return lveq.doubleValue() == rveq.doubleValue();
			} else if(leftvalue instanceof Integer lveq && rightvalue instanceof Long rveq) {
				return lveq.longValue() == rveq.longValue();
			} else if(leftvalue instanceof Long lveq && rightvalue instanceof Integer rveq) {
				return lveq.longValue() == rveq.longValue();
			} else if(leftvalue instanceof Integer lveq && rightvalue instanceof Integer rveq) {
				return lveq.intValue() == rveq.intValue();
			} else if(leftvalue instanceof Long lveq && rightvalue instanceof Long rveq) {
				return lveq.longValue() == rveq.longValue();
			} else if(leftvalue instanceof String lveq && rightvalue instanceof String rveq) {
				return lveq.equals(rveq);
			} else {
				return false;
			}
		case "<>":
			if(leftvalue instanceof Double lvne && rightvalue instanceof Double rvne) {
				return lvne != rvne;
			} else if(leftvalue instanceof Long lvne && rightvalue instanceof Double rvne) {
				return lvne.longValue() != rvne.longValue();
			} else if(leftvalue instanceof Double lvne && rightvalue instanceof Long rvne) {
				return lvne.longValue() != rvne.longValue();
			} else if(leftvalue instanceof Integer lvne && rightvalue instanceof Double rvne) {
				return lvne.longValue() != rvne.longValue();
			} else if(leftvalue instanceof Double lvne && rightvalue instanceof Integer rvne) {
				return lvne.longValue() != rvne.longValue();
			} else if(leftvalue instanceof Long lvne && rightvalue instanceof Integer rvne) {
				return lvne.longValue() != rvne.longValue();
			} else if(leftvalue instanceof Integer lvne && rightvalue instanceof Long rvne) {
				return lvne.longValue() != rvne.longValue();
			} else if(leftvalue instanceof Integer lvne && rightvalue instanceof Integer rvne) {
				return lvne.longValue() != rvne.longValue();
			} else if(leftvalue instanceof Long lvne && rightvalue instanceof Long rvne) {
				return lvne != rvne;
			} else if(leftvalue instanceof String lvne && rightvalue instanceof String rvne) {
				return !lvne.equals(rvne);
			} else {
				return false;
			}
		default:
			throw new UnsupportedOperationException("Unsupported operator: " + operator);
		}
	}

	/**
	 * Compare two objects to sort in order.
	 * 
	 * @param obj1
	 * @param obj2
	 * @return value in Double for sorting.
	 */
	public static int compareTo(Object obj1, Object obj2) {
		if (obj1 instanceof Double val1 && obj2 instanceof Double val2) {
			return val1.compareTo(val2);
		} else if (obj1 instanceof Long val1 && obj2 instanceof Long val2) {
			return val1.compareTo(val2);
		} else if (obj1 instanceof Integer val1 && obj2 instanceof Integer val2) {
			return val1.compareTo(val2);
		} else if (obj1 instanceof Float val1 && obj2 instanceof Float val2) {
			return val1.compareTo(val2);
		} else if (obj1 instanceof String val1 && obj2 instanceof String val2) {
			return val1.compareTo(val2);
		}
		return 0;
	}

	/**
	 * This function gets the column from functions.
	 * 
	 * @param functions
	 * @return column referred in function like sum, min,max.
	 */
	public Column getColumn(List<Function> functions) {
		List<Expression> parameters = functions.get(0).getParameters().getExpressions();
		return (Column) parameters.get(0);
	}

	/**
	 * This function gets the column from single function.
	 * 
	 * @param functions
	 * @return column referred in function like sum, min,max.
	 */
	public Column getColumn(Function function) {
		List<Expression> parameters = function.getParameters().getExpressions();
		return (Column) parameters.get(0);
	}

	/**
	 * Get list of columns from list of functions.
	 * 
	 * @param functions
	 * @return list of columns
	 */
	public static List<String> getColumns(List<Function> functions) {
		return functions.parallelStream()
				.filter((Serializable & Predicate<? super Function>) (fn -> nonNull(fn.getParameters())))
				.map((Serializable & java.util.function.Function<Function, String>) (fn -> ((Column) fn.getParameters()
						.getExpressions().get(0)).getColumnName()))
				.collect(Collectors.toList());
	}

	/**
	 * Adds all the required columns from select items.
	 * 
	 * @param allRequiredColumns
	 * @param allColumnsSelectItems
	 */
	public static void addAllRequiredColumnsFromSelectItems(Map<String, Set<String>> allRequiredColumns,
			Map<String, Set<String>> allColumnsSelectItems) {
		allColumnsSelectItems.keySet().parallelStream().forEach((Serializable & Consumer<String>) (key -> {
			Set<String> allReqColumns = allRequiredColumns.get(key);
			Set<String> allSelectItem = allColumnsSelectItems.get(key);
			if (isNull(allReqColumns)) {
				allRequiredColumns.put(key, allSelectItem);
			} else {
				allReqColumns.addAll(allSelectItem);
			}
		}));
	}

	

	/**
	 * Evaluates the expression of object array.
	 * 
	 * @param expression
	 * @param row
	 * @param columnsforeachjoin
	 * @return evaluates to true if expression succeeds and false if fails.
	 */
	public static boolean evaluateExpression(Expression expression, Map<String, Object> row, List<String> columnsforeachjoin) {
		if (expression instanceof Between) {
	        Between betweenExpression = (Between) expression;
	        Expression leftExpression = betweenExpression.getLeftExpression();
	        Expression startExpression = betweenExpression.getBetweenExpressionStart();
	        Expression endExpression = betweenExpression.getBetweenExpressionEnd();
	        Object leftValue = getValueString(leftExpression, row, columnsforeachjoin);
	        Object startExpressionValue = getValueString(startExpression, row, columnsforeachjoin);
	        Object endExpressionValue = getValueString(endExpression, row, columnsforeachjoin);
	        if(evaluatePredicate(leftValue, startExpressionValue, ">") && evaluatePredicate(leftValue, endExpressionValue, "<")) {
	        	return true;
	        }
	        return false;
		}
		else if (expression instanceof InExpression) {
	        InExpression inExpression = (InExpression) expression;
	        Expression leftExpression = inExpression.getLeftExpression();
	        ItemsList itemsList = inExpression.getRightItemsList();

	        // Handle the left expression
	        Object leftValue = getValueString(leftExpression, row, columnsforeachjoin);

	        // Handle the IN clause values
	        if (itemsList instanceof ExpressionList) {
	            ExpressionList expressionList = (ExpressionList) itemsList;
	            List<Expression> inValues = expressionList.getExpressions();

	            // Process each value in the IN clause
	            for (Expression expressioninlist : inValues) {
	            	Object rightValue = getValueString(expressioninlist, row, columnsforeachjoin);
	            	if(evaluatePredicate(leftValue, rightValue, "=")) {
	            		return true;
	            	}
	            }	            
	        }
	        return false;

	    } else if (expression instanceof LikeExpression) {
	        LikeExpression likeExpression = (LikeExpression) expression;
	        Expression leftExpression = likeExpression.getLeftExpression();
	        Expression rightExpression = likeExpression.getRightExpression();

	        // Handle the left expression
	        String leftValue = (String) getValueString(leftExpression, row, columnsforeachjoin);

	        // Handle the right expression (LIKE pattern)
	        String rightValue = (String) getValueString(rightExpression, row, columnsforeachjoin);
	        
	        return leftValue.contains(rightValue);

	    } else if (expression instanceof BinaryExpression) {
			BinaryExpression binaryExpression = (BinaryExpression) expression;
			String operator = binaryExpression.getStringExpression();
			Expression leftExpression = binaryExpression.getLeftExpression();
			Expression rightExpression = binaryExpression.getRightExpression();

			switch (operator.toUpperCase()) {
			case "AND":
				return evaluateExpression(leftExpression, row, columnsforeachjoin)
						&& evaluateExpression(rightExpression, row, columnsforeachjoin);
			case "OR":
				return evaluateExpression(leftExpression, row, columnsforeachjoin)
						|| evaluateExpression(rightExpression, row, columnsforeachjoin);
			case ">":
				Object leftValue = getValueString(leftExpression, row, columnsforeachjoin);
				Object rightValue = getValueString(rightExpression, row, columnsforeachjoin);
				return evaluatePredicate(leftValue, rightValue, operator);
			case ">=":
				leftValue = getValueString(leftExpression, row, columnsforeachjoin);
				rightValue = getValueString(rightExpression, row, columnsforeachjoin);
				return evaluatePredicate(leftValue, rightValue, operator);
			case "<":
				leftValue = getValueString(leftExpression, row, columnsforeachjoin);
				rightValue = getValueString(rightExpression, row, columnsforeachjoin);
				return evaluatePredicate(leftValue, rightValue, operator);
			case "<=":
				leftValue = getValueString(leftExpression, row, columnsforeachjoin);
				rightValue = getValueString(rightExpression, row, columnsforeachjoin);
				return evaluatePredicate(leftValue, rightValue, operator);
			case "=":
				leftValue = getValueString(leftExpression, row, columnsforeachjoin);
				rightValue = getValueString(rightExpression, row, columnsforeachjoin);
				return evaluatePredicate(leftValue, rightValue, operator);
			case "<>":
				leftValue = getValueString(leftExpression, row, columnsforeachjoin);
				rightValue = getValueString(rightExpression, row, columnsforeachjoin);
				return evaluatePredicate(leftValue, rightValue, operator);
			default:
				throw new UnsupportedOperationException("Unsupported operator: " + operator);
			}
		} else if (expression instanceof Parenthesis) {
			Parenthesis parenthesis = (Parenthesis) expression;
			Expression subExpression = parenthesis.getExpression();
			return evaluateExpression(subExpression, row, columnsforeachjoin);
		} else {
			Object value = getValueString(expression, row, columnsforeachjoin);
			return Boolean.parseBoolean((String) value);
		}
	}

	/**
	 * Evaluates the join condition or expression and returns true or false.
	 * 
	 * @param expression
	 * @param jointable
	 * @param row1
	 * @param row2
	 * @param leftablecolumns
	 * @param righttablecolumns
	 * @return true or false
	 */
	public static boolean evaluateExpressionJoin(Expression expression, String jointable, Map<String, Object> row1, Map<String, Object> row2,
			List<String> leftablecolumns, List<String> righttablecolumns) {
		if (expression instanceof BinaryExpression) {
			BinaryExpression binaryExpression = (BinaryExpression) expression;
			String operator = binaryExpression.getStringExpression();
			Expression leftExpression = binaryExpression.getLeftExpression();
			Expression rightExpression = binaryExpression.getRightExpression();
			Map<String, Object> rowleft = null;
			List<String> columnsrowleft = null;
			if (leftExpression instanceof Column column && column.getTable().getName().equals(jointable)) {
				rowleft = row2;
				columnsrowleft = righttablecolumns;
			} else {
				rowleft = row1;
				columnsrowleft = leftablecolumns;
			}
			Map<String, Object> rowright = null;
			List<String> columnsrowright = null;
			if (rightExpression instanceof Column column && column.getTable().getName().equals(jointable)) {
				rowright = row2;
				columnsrowright = righttablecolumns;
			} else {
				rowright = row1;
				columnsrowright = leftablecolumns;
			}
			switch (operator.toUpperCase()) {
			case "AND":
				return evaluateExpressionJoin(leftExpression, jointable, rowleft, rowright, columnsrowleft,
						columnsrowright)
						&& evaluateExpressionJoin(rightExpression, jointable, rowleft, rowright, columnsrowleft,
								columnsrowright);
			case "OR":
				return evaluateExpressionJoin(leftExpression, jointable, rowleft, rowright, columnsrowleft,
						righttablecolumns)
						|| evaluateExpressionJoin(rightExpression, jointable, rowleft, rowright, columnsrowleft,
								columnsrowright);
			case ">":
				Double leftValue = (Double) getValueString(leftExpression, rowleft, columnsrowleft);
				Double rightValue = (Double) getValueString(rightExpression, rowright, columnsrowright);
				return Double.valueOf(leftValue) > Double.valueOf(rightValue);
			case ">=":
				leftValue = (Double) getValueString(leftExpression, rowleft, columnsrowleft);
				rightValue = (Double) getValueString(rightExpression, rowright, columnsrowright);
				return Double.valueOf(leftValue) >= Double.valueOf(rightValue);
			case "<":
				leftValue = (Double) getValueString(leftExpression, rowleft, columnsrowleft);
				rightValue = (Double) getValueString(rightExpression, rowright, columnsrowright);
				return Double.valueOf(leftValue) < Double.valueOf(rightValue);
			case "<=":
				leftValue = (Double) getValueString(leftExpression, rowleft, columnsrowleft);
				rightValue = (Double) getValueString(rightExpression, rowright, columnsrowright);
				return Double.valueOf(leftValue) <= Double.valueOf(rightValue);
			case "=":
				Object leftValueO = getValueString(leftExpression, rowleft, columnsrowleft);
				Object rightValueO = getValueString(rightExpression, rowright, columnsrowright);
				return compareTo(leftValueO,rightValueO) == 0;
			case "<>":
				leftValueO = getValueString(leftExpression, rowleft, columnsrowleft);
				leftValueO = getValueString(rightExpression, rowright, righttablecolumns);
				return compareTo(leftValueO,leftValueO) != 0;
			default:
				throw new UnsupportedOperationException("Unsupported operator: " + operator);
			}
		} else if (expression instanceof Parenthesis) {
			Parenthesis parenthesis = (Parenthesis) expression;
			Expression subExpression = parenthesis.getExpression();
			return evaluateExpressionJoin(subExpression, jointable, row1, row2, leftablecolumns, righttablecolumns);
		} else {
			Map<String, Object> row = null;
			List<String> columnsrow = null;
			if (expression instanceof Column column && column.getTable().getName().equals(jointable)) {
				row = row2;
				columnsrow = righttablecolumns;
			} else {
				row = row1;
				columnsrow = leftablecolumns;
			}
			Object value = getValueString(expression, row, columnsrow);
			return Boolean.parseBoolean((String) value);
		}
	}

	
	public static void getColumnsFromSelectItemsFunctions(Expression expression,
			Map<String, Set<String>> tablerequiredcolumns,
			List<String> columnsselect,
			List<String> functioncols) {
		if (expression instanceof BinaryExpression) {
			BinaryExpression binaryExpression = (BinaryExpression) expression;
			Expression leftExpression = binaryExpression.getLeftExpression();
			Expression rightExpression = binaryExpression.getRightExpression();
			getColumnsFromSelectItemsFunctions(leftExpression, tablerequiredcolumns, columnsselect, functioncols);
			getColumnsFromSelectItemsFunctions(rightExpression, tablerequiredcolumns, columnsselect, functioncols);
		}
		if (expression instanceof Function function) {
			if (nonNull(function.getParameters())) {
				List<Expression> expressions = function.getParameters().getExpressions();
				for (Expression exp : expressions) {
					getColumnsFromSelectItemsFunctions(exp, tablerequiredcolumns,
							columnsselect, functioncols);
				}
			}
		}
		else if (expression instanceof Parenthesis) {
			Parenthesis parenthesis = (Parenthesis) expression;
			Expression subExpression = parenthesis.getExpression();
			getColumnsFromSelectItemsFunctions(subExpression, tablerequiredcolumns,
					columnsselect, functioncols);
		} else if(expression instanceof Column column && nonNull(column.getTable())) {
			Set<String> requiredcolumns = tablerequiredcolumns.get(column.getTable().getName());
			if (isNull(requiredcolumns)) {
				requiredcolumns = new LinkedHashSet<>();
				tablerequiredcolumns.put(column.getTable().getName(), requiredcolumns);
			}
			requiredcolumns.add(column.getColumnName());
			columnsselect.add(column.getColumnName());
			functioncols.add(column.getColumnName());
		} else if(expression instanceof Column column) {
			columnsselect.add(column.getColumnName());
			functioncols.add(column.getColumnName());
		}
	}
	
	/**
	 * Gets the value of string given expression in column and records in object
	 * array and column index.
	 * 
	 * @param expression
	 * @param row
	 * @param columns
	 * @return value in string format
	 */
	private static Object getValueString(Expression expression,Map<String, Object> row, List<String> columns) {
		if (expression instanceof LongValue) {
			return Double.valueOf(((LongValue) expression).getValue());
		} else if (expression instanceof StringValue) {
			return ((StringValue) expression).getValue();
		} else if (expression instanceof DoubleValue) {
			return Double.valueOf(((DoubleValue) expression).getValue());
		} else if(expression instanceof Addition addition){
			return evaluateValuesByOperator(getValueString(addition.getLeftExpression() , row),
					getValueString(addition.getRightExpression() , row), "+");
		} else if(expression instanceof Subtraction subtraction){
			return evaluateValuesByOperator(getValueString(subtraction.getLeftExpression() , row),
					getValueString(subtraction.getRightExpression() , row), "-");
		} else if(expression instanceof Multiplication multiplication){
			return evaluateValuesByOperator(getValueString(multiplication.getLeftExpression() , row),
					getValueString(multiplication.getRightExpression() , row), "*");
		} else if(expression instanceof Division division){
			return evaluateValuesByOperator(getValueString(division.getLeftExpression() , row),
					getValueString(division.getRightExpression() , row), "/");
		} else {
			Column column = (Column) expression;
			String columnName = column.getColumnName();
			Object value = row.get(columnName);
			if(value instanceof String stringval) {
				return String.valueOf(stringval);
			}
			else if(value instanceof Double doubleval) {
				return doubleval;
			}
			else if(value instanceof Integer intval) {
				return intval;
			} else if(value instanceof Long longval) {
				return longval;
			}
			return String.valueOf(value);
		}
	}

	/**
	 * Gets the required columns from all the join tables
	 * 
	 * @param plainSelect
	 * @param tablerequiredcolumns
	 * @param expressionsTable
	 * @param joinTableExpressions
	 */
	public static void getRequiredColumnsForAllTables(PlainSelect plainSelect, Map<String, Set<String>> tablerequiredcolumns,
			Map<String, List<Expression>> expressionsTable, Map<String, List<Expression>> joinTableExpressions) {

		List<Expression> expressions = new Vector<>();

		if (nonNull(plainSelect.getJoins())) {
			plainSelect.getJoins().parallelStream()
					.map((Serializable & java.util.function.Function<? super Join, ? extends Expression>) join -> join
							.getOnExpression())
					.forEach((Serializable & Consumer<Expression>) expression -> expressions.add(expression));
		}
		if (nonNull(plainSelect.getWhere())) {
			getColumnsFromBinaryExpression(plainSelect.getWhere(), tablerequiredcolumns, expressionsTable,
					expressionsTable);
		}
		if (nonNull(plainSelect.getGroupBy())) {
			getColumnsFromGroupByExpression(plainSelect.getGroupBy(), tablerequiredcolumns);
		}
		for (Expression onExpression : expressions) {
			getColumnsFromBinaryExpression(onExpression, tablerequiredcolumns, joinTableExpressions,
					joinTableExpressions);
		}
	}

	/**
	 * Gets all the columns from the expressions.
	 * 
	 * @param expression
	 * @param tablerequiredcolumns
	 * @param expressions
	 * @param joinTableExpressions
	 */
	public static void getColumnsFromBinaryExpression(Expression expression, Map<String, Set<String>> tablerequiredcolumns,
			Map<String, List<Expression>> expressions, Map<String, List<Expression>> joinTableExpressions) {
		if (expression instanceof Parenthesis parenthesis) {
			Expression subExpression = parenthesis.getExpression();
			getColumnsFromBinaryExpression(subExpression, tablerequiredcolumns, expressions,
					joinTableExpressions);
		} else if (expression instanceof Between) {
	        Between betweenExpression = (Between) expression;
	        Expression leftExpression = betweenExpression.getLeftExpression();
	        Expression startExpression = betweenExpression.getBetweenExpressionStart();
	        Expression endExpression = betweenExpression.getBetweenExpressionEnd();
	        getColumnsFromBinaryExpression(leftExpression, tablerequiredcolumns, expressions,
					joinTableExpressions);
	        getColumnsFromBinaryExpression(startExpression, tablerequiredcolumns, expressions,
					joinTableExpressions);
	        getColumnsFromBinaryExpression(endExpression, tablerequiredcolumns, expressions,
					joinTableExpressions);
		}
		else if (expression instanceof InExpression) {
	        InExpression inExpression = (InExpression) expression;
	        Expression leftExpression = inExpression.getLeftExpression();
	        ItemsList itemsList = inExpression.getRightItemsList();

	        // Handle the left expression
	        getColumnsFromBinaryExpression(leftExpression, tablerequiredcolumns, expressions,
					joinTableExpressions);

	        // Handle the IN clause values
	        if (itemsList instanceof ExpressionList) {
	            ExpressionList expressionList = (ExpressionList) itemsList;
	            List<Expression> inValues = expressionList.getExpressions();

	            // Process each value in the IN clause
	            for (Expression value : inValues) {
	            	getColumnsFromBinaryExpression(value, tablerequiredcolumns, expressions,
	    					joinTableExpressions);
	            }
	        }

	    } else if (expression instanceof LikeExpression) {
	        LikeExpression likeExpression = (LikeExpression) expression;
	        Expression leftExpression = likeExpression.getLeftExpression();
	        Expression rightExpression = likeExpression.getRightExpression();

	        // Handle the left expression
	        getColumnsFromBinaryExpression(leftExpression, tablerequiredcolumns, expressions,
					joinTableExpressions);

	        // Handle the right expression (LIKE pattern)
	        getColumnsFromBinaryExpression(rightExpression, tablerequiredcolumns, expressions,
					joinTableExpressions);

	    } else if (expression instanceof BinaryExpression binaryExpression) {
	    	
	    	String operator = binaryExpression.getStringExpression();
	    	
			Expression leftExpression = binaryExpression.getLeftExpression();
			
			getColumnsFromBinaryExpression(leftExpression, tablerequiredcolumns, expressions, joinTableExpressions);
			
			Expression rightExpression = binaryExpression.getRightExpression();
			
			getColumnsFromBinaryExpression(rightExpression, tablerequiredcolumns, expressions,
						joinTableExpressions);
						
			
			if (leftExpression instanceof Column column1 && !(rightExpression instanceof Column)
					&& (operator.equals("=") ||
							operator.equals("<")
							||operator.equals("<=")
							||operator.equals(">")
							||operator.equals(">=")
							||operator.equals("<>"))) {
				if (nonNull(column1.getTable())) {
					List<Expression> expressionsTable = expressions.get(column1.getTable().getName());
					if (isNull(expressionsTable)) {
						expressionsTable = new Vector<>();
						expressions.put(column1.getTable().getName(), expressionsTable);
					}
					expressionsTable.add(binaryExpression);
				}
			} else if (!(leftExpression instanceof Column) && rightExpression instanceof Column column1
					&& (operator.equals("=") ||
							operator.equals("<")
							||operator.equals("<=")
							||operator.equals(">")
							||operator.equals(">=")
							||operator.equals("<>"))) {
				if (nonNull(column1.getTable())) {
					List<Expression> expressionsTable = expressions.get(column1.getTable().getName());
					if (isNull(expressionsTable)) {
						expressionsTable = new Vector<>();
						expressions.put(column1.getTable().getName(), expressionsTable);
					}
					expressionsTable.add(binaryExpression);
				}
			} else if (leftExpression instanceof Column col1 && rightExpression instanceof Column col2
					&& (operator.equals("=") ||
							operator.equals("<")
							||operator.equals("<=")
							||operator.equals(">")
							||operator.equals(">=")
							||operator.equals("<>"))) {
				List<Expression> expressionsTable = joinTableExpressions
						.get(col1.getTable().getName() + "-" + col2.getTable().getName());
				if (isNull(expressionsTable)) {
					expressionsTable = new Vector<>();
					joinTableExpressions.put(col1.getTable().getName() + "-" + col2.getTable().getName(),
							expressionsTable);
				}
				expressionsTable.add(binaryExpression);
			} else if (leftExpression instanceof BinaryExpression expr && !(rightExpression instanceof Column)
					&& (operator.equals("=") ||
							operator.equals("<")
							||operator.equals("<=")
							||operator.equals(">")
							||operator.equals(">=")
							||operator.equals("<>"))) {
				List<Column> columnsFromExpression = new ArrayList<>(); 
				getColumnsFromExpression(expr, columnsFromExpression);
				for(Column column1:columnsFromExpression) {
					if (nonNull(column1.getTable())) {
						List<Expression> expressionsTable = expressions.get(column1.getTable().getName());
						if (isNull(expressionsTable)) {
							expressionsTable = new Vector<>();
							expressions.put(column1.getTable().getName(), expressionsTable);
						}
						expressionsTable.add(binaryExpression);
					}
				}
				
			} else if (!(leftExpression instanceof Column) && rightExpression instanceof BinaryExpression expr
					&& (operator.equals("=") ||
							operator.equals("<")
							||operator.equals("<=")
							||operator.equals(">")
							||operator.equals(">=")
							||operator.equals("<>"))) {
				List<Column> columnsFromExpression = new ArrayList<>(); 
				getColumnsFromExpression(expr, columnsFromExpression);
				for(Column column1:columnsFromExpression) {
					if (nonNull(column1.getTable())) {
						List<Expression> expressionsTable = expressions.get(column1.getTable().getName());
						if (isNull(expressionsTable)) {
							expressionsTable = new Vector<>();
							expressions.put(column1.getTable().getName(), expressionsTable);
						}
						expressionsTable.add(binaryExpression);
					}
				}
			}
			// Check if either left or right expression is a column
			if (leftExpression instanceof Column column) {
				if (nonNull(column.getTable())) {
					Set<String> columns = tablerequiredcolumns.get(column.getTable().getName());
					if (isNull(columns)) {
						columns = new LinkedHashSet<>();
						tablerequiredcolumns.put(column.getTable().getName(), columns);
					}
					columns.add(column.getColumnName());
				}
			}

			if (rightExpression instanceof Column column) {
				if (nonNull(column.getTable())) {
					Set<String> columns = tablerequiredcolumns.get(column.getTable().getName());
					if (isNull(columns)) {
						columns = new LinkedHashSet<>();
						tablerequiredcolumns.put(column.getTable().getName(), columns);
					}
					columns.add(column.getColumnName());
				}
			}
		} else if (expression instanceof Column column && nonNull(column.getTable())) {
			Set<String> columns = tablerequiredcolumns.get(column.getTable().getName());
			if (isNull(columns)) {
				columns = new LinkedHashSet<>();
				tablerequiredcolumns.put(column.getTable().getName(), columns);
			}
			columns.add(column.getColumnName());
		} 
	}
	
	/**
	 * Get columns from group by expression
	 * @param gbe
	 * @param tablerequiredcolumns
	 */
	public static void getColumnsFromGroupByExpression(GroupByElement gbe, Map<String, Set<String>> tablerequiredcolumns) {
		ExpressionList el = gbe.getGroupByExpressionList();
		List<Expression> expressions = el.getExpressions();
		for (Expression exp : expressions) {
			if (exp instanceof Column column) {
				if (nonNull(column.getTable())) {
					Set<String> columns = tablerequiredcolumns.get(column.getTable().getName());
					if (isNull(columns)) {
						columns = new LinkedHashSet<>();
						tablerequiredcolumns.put(column.getTable().getName(), columns);
					}
					columns.add(column.getColumnName());
				}
			}
		}
	}
	
	/**
	 * This functions returns initial filter or expressions.
	 * 
	 * @param leftTableExpressions
	 * @return expression
	 */
	public static Expression getFilterExpression(List<Expression> leftTableExpressions) {
		Expression expression = null;
		if (CollectionUtils.isNotEmpty(leftTableExpressions)) {
			expression = leftTableExpressions.get(0);
			for (int expressioncount = 1; expressioncount < leftTableExpressions.size(); expressioncount++) {
				expression = new OrExpression(expression, leftTableExpressions.get(expressioncount));
			}
			return expression;
		}
		return null;
	}

	/**
	 * This function generates column index map for given columns in order.
	 * 
	 * @param columns
	 * @return map of indexed columns.
	 */
	public static Map<String, Integer> getColumnsIndex(List<String> columns) {
		Map<String, Integer> columnindexmap = new ConcurrentHashMap<>();
		for (int columnindex = 0; columnindex < columns.size(); columnindex++) {
			columnindexmap.put(columns.get(columnindex), (int) columnindex);
		}
		return columnindexmap;
	}
	
	/**
	 * Converts Object array to Tuple
	 * @param obj
	 * @return Tuple
	 */
	public static Tuple convertObjectToTuple(Object[] obj) {
		if(isNull(obj) || obj.length == 0) {
			return Tuple.tuple(DataSamudayaConstants.EMPTY);
		} else if(obj.length == 1) {
			return Tuple.tuple(obj[0]);
		} else if(obj.length == 2) {
			return Tuple.tuple(obj[0], obj[1]);
		} else if(obj.length == 3) {
			return Tuple.tuple(obj[0], obj[1], obj[2]);
		} else if(obj.length == 4) {
			return Tuple.tuple(obj[0], obj[1], obj[2], obj[3]);
		} else if(obj.length == 5) {
			return Tuple.tuple(obj[0], obj[1], obj[2], obj[3], obj[4]);
		} else if(obj.length == 6) {
			return Tuple.tuple(obj[0], obj[1], obj[2], obj[3], obj[4], obj[5]);
		} else if(obj.length == 7) {
			return Tuple.tuple(obj[0], obj[1], obj[2], obj[3], obj[4], obj[5], obj[6]);
		} else if(obj.length == 8) {
			return Tuple.tuple(obj[0], obj[1], obj[2], obj[3], obj[4], obj[5], obj[6], obj[7]);
		} else if(obj.length == 9) {
			return Tuple.tuple(obj[0], obj[1], obj[2], obj[3], obj[4], obj[5], obj[6], obj[7], obj[8]);
		} else if(obj.length == 10) {
			return Tuple.tuple(obj[0], obj[1], obj[2], obj[3], obj[4], obj[5], obj[6], obj[7], obj[8], obj[9]);
		} else if(obj.length == 11) {
			return Tuple.tuple(obj[0], obj[1], obj[2], obj[3], obj[4], obj[5], obj[6], obj[7], obj[8], obj[9], obj[10]);
		} else if(obj.length == 12) {
			return Tuple.tuple(obj[0], obj[1], obj[2], obj[3], obj[4], obj[5], obj[6], obj[7], obj[8], obj[9], obj[10], obj[11]);
		} else if(obj.length == 13) {
			return Tuple.tuple(obj[0], obj[1], obj[2], obj[3], obj[4], obj[5], obj[6], obj[7], obj[8], obj[9], obj[10], obj[11], obj[12]);
		} else if(obj.length == 14) {
			return Tuple.tuple(obj[0], obj[1], obj[2], obj[3], obj[4], obj[5], obj[6], obj[7], obj[8], obj[9], obj[10], obj[11], obj[12], obj[13]);
		} else if(obj.length == 15) {
			return Tuple.tuple(obj[0], obj[1], obj[2], obj[3], obj[4], obj[5], obj[6], obj[7], obj[8], obj[9], obj[10], obj[11], obj[12], obj[13], obj[14]);
		} else if(obj.length == 16) {
			return Tuple.tuple(obj[0], obj[1], obj[2], obj[3], obj[4], obj[5], obj[6], obj[7], obj[8], obj[9], obj[10], obj[11], obj[12], obj[13], obj[14], obj[15]);
		} else {
			throw new UnsupportedOperationException("Max supported Column is 16");
		}
	}
	
	
	/**
	 * Populates map from tuple for 
	 * @param mapvalues
	 * @param tuple
	 * @param groupby
	 */
	public static void populateMapFromTuple(Map<String, Object> mapvalues, Tuple tuple, List<String> groupby) {
		
		if(groupby.isEmpty()) {
			return;
		} else if(tuple instanceof Tuple1 tup) {
			mapvalues.put(groupby.get(0), tup.v1());
		} else if(tuple instanceof Tuple2 tup) {
			mapvalues.put(groupby.get(0), tup.v1());
			mapvalues.put(groupby.get(1), tup.v2());
		} else if(tuple instanceof Tuple3 tup) {
			mapvalues.put(groupby.get(0), tup.v1());
			mapvalues.put(groupby.get(1), tup.v2());
			mapvalues.put(groupby.get(2), tup.v3());
		} else if(tuple instanceof Tuple4 tup) {
			mapvalues.put(groupby.get(0), tup.v1());
			mapvalues.put(groupby.get(1), tup.v2());
			mapvalues.put(groupby.get(2), tup.v3());
			mapvalues.put(groupby.get(3), tup.v4());
		}else if(tuple instanceof Tuple5 tup) {
			mapvalues.put(groupby.get(0), tup.v1());
			mapvalues.put(groupby.get(1), tup.v2());
			mapvalues.put(groupby.get(2), tup.v3());
			mapvalues.put(groupby.get(3), tup.v4());
			mapvalues.put(groupby.get(4), tup.v5());
		}else if(tuple instanceof Tuple6 tup) {
			mapvalues.put(groupby.get(0), tup.v1());
			mapvalues.put(groupby.get(1), tup.v2());
			mapvalues.put(groupby.get(2), tup.v3());
			mapvalues.put(groupby.get(3), tup.v4());
			mapvalues.put(groupby.get(4), tup.v5());
			mapvalues.put(groupby.get(5), tup.v6());
		}else if(tuple instanceof Tuple7 tup) {
			mapvalues.put(groupby.get(0), tup.v1());
			mapvalues.put(groupby.get(1), tup.v2());
			mapvalues.put(groupby.get(2), tup.v3());
			mapvalues.put(groupby.get(3), tup.v4());
			mapvalues.put(groupby.get(4), tup.v5());
			mapvalues.put(groupby.get(5), tup.v6());
			mapvalues.put(groupby.get(6), tup.v7());
		}else if(tuple instanceof Tuple8 tup) {
			mapvalues.put(groupby.get(0), tup.v1());
			mapvalues.put(groupby.get(1), tup.v2());
			mapvalues.put(groupby.get(2), tup.v3());
			mapvalues.put(groupby.get(3), tup.v4());
			mapvalues.put(groupby.get(4), tup.v5());
			mapvalues.put(groupby.get(5), tup.v6());
			mapvalues.put(groupby.get(6), tup.v7());
			mapvalues.put(groupby.get(7), tup.v8());
		}else if(tuple instanceof Tuple9 tup) {
			mapvalues.put(groupby.get(0), tup.v1());
			mapvalues.put(groupby.get(1), tup.v2());
			mapvalues.put(groupby.get(2), tup.v3());
			mapvalues.put(groupby.get(3), tup.v4());
			mapvalues.put(groupby.get(4), tup.v5());
			mapvalues.put(groupby.get(5), tup.v6());
			mapvalues.put(groupby.get(6), tup.v7());
			mapvalues.put(groupby.get(7), tup.v8());
			mapvalues.put(groupby.get(8), tup.v9());
		}else if(tuple instanceof Tuple10 tup) {
			mapvalues.put(groupby.get(0), tup.v1());
			mapvalues.put(groupby.get(1), tup.v2());
			mapvalues.put(groupby.get(2), tup.v3());
			mapvalues.put(groupby.get(3), tup.v4());
			mapvalues.put(groupby.get(4), tup.v5());
			mapvalues.put(groupby.get(5), tup.v6());
			mapvalues.put(groupby.get(6), tup.v7());
			mapvalues.put(groupby.get(7), tup.v8());
			mapvalues.put(groupby.get(8), tup.v9());
			mapvalues.put(groupby.get(9), tup.v10());
		}else if(tuple instanceof Tuple11 tup) {
			mapvalues.put(groupby.get(0), tup.v1());
			mapvalues.put(groupby.get(1), tup.v2());
			mapvalues.put(groupby.get(2), tup.v3());
			mapvalues.put(groupby.get(3), tup.v4());
			mapvalues.put(groupby.get(4), tup.v5());
			mapvalues.put(groupby.get(5), tup.v6());
			mapvalues.put(groupby.get(6), tup.v7());
			mapvalues.put(groupby.get(7), tup.v8());
			mapvalues.put(groupby.get(8), tup.v9());
			mapvalues.put(groupby.get(9), tup.v10());
			mapvalues.put(groupby.get(10), tup.v11());
		}else if(tuple instanceof Tuple12 tup) {
			mapvalues.put(groupby.get(0), tup.v1());
			mapvalues.put(groupby.get(1), tup.v2());
			mapvalues.put(groupby.get(2), tup.v3());
			mapvalues.put(groupby.get(3), tup.v4());
			mapvalues.put(groupby.get(4), tup.v5());
			mapvalues.put(groupby.get(5), tup.v6());
			mapvalues.put(groupby.get(6), tup.v7());
			mapvalues.put(groupby.get(7), tup.v8());
			mapvalues.put(groupby.get(8), tup.v9());
			mapvalues.put(groupby.get(9), tup.v10());
			mapvalues.put(groupby.get(10), tup.v11());
			mapvalues.put(groupby.get(11), tup.v12());
		} else if(tuple instanceof Tuple13 tup) {
			mapvalues.put(groupby.get(0), tup.v1());
			mapvalues.put(groupby.get(1), tup.v2());
			mapvalues.put(groupby.get(2), tup.v3());
			mapvalues.put(groupby.get(3), tup.v4());
			mapvalues.put(groupby.get(4), tup.v5());
			mapvalues.put(groupby.get(5), tup.v6());
			mapvalues.put(groupby.get(6), tup.v7());
			mapvalues.put(groupby.get(7), tup.v8());
			mapvalues.put(groupby.get(8), tup.v9());
			mapvalues.put(groupby.get(9), tup.v10());
			mapvalues.put(groupby.get(10), tup.v11());
			mapvalues.put(groupby.get(11), tup.v12());
			mapvalues.put(groupby.get(12), tup.v13());
		} else if(tuple instanceof Tuple14 tup) {
			mapvalues.put(groupby.get(0), tup.v1());
			mapvalues.put(groupby.get(1), tup.v2());
			mapvalues.put(groupby.get(2), tup.v3());
			mapvalues.put(groupby.get(3), tup.v4());
			mapvalues.put(groupby.get(4), tup.v5());
			mapvalues.put(groupby.get(5), tup.v6());
			mapvalues.put(groupby.get(6), tup.v7());
			mapvalues.put(groupby.get(7), tup.v8());
			mapvalues.put(groupby.get(8), tup.v9());
			mapvalues.put(groupby.get(9), tup.v10());
			mapvalues.put(groupby.get(10), tup.v11());
			mapvalues.put(groupby.get(11), tup.v12());
			mapvalues.put(groupby.get(12), tup.v13());
			mapvalues.put(groupby.get(13), tup.v14());
		}else if(tuple instanceof Tuple15 tup) {
			mapvalues.put(groupby.get(0), tup.v1());
			mapvalues.put(groupby.get(1), tup.v2());
			mapvalues.put(groupby.get(2), tup.v3());
			mapvalues.put(groupby.get(3), tup.v4());
			mapvalues.put(groupby.get(4), tup.v5());
			mapvalues.put(groupby.get(5), tup.v6());
			mapvalues.put(groupby.get(6), tup.v7());
			mapvalues.put(groupby.get(7), tup.v8());
			mapvalues.put(groupby.get(8), tup.v9());
			mapvalues.put(groupby.get(9), tup.v10());
			mapvalues.put(groupby.get(10), tup.v11());
			mapvalues.put(groupby.get(11), tup.v12());
			mapvalues.put(groupby.get(12), tup.v13());
			mapvalues.put(groupby.get(13), tup.v14());
			mapvalues.put(groupby.get(14), tup.v15());
		}else if(tuple instanceof Tuple16 tup) {
			mapvalues.put(groupby.get(0), tup.v1());
			mapvalues.put(groupby.get(1), tup.v2());
			mapvalues.put(groupby.get(2), tup.v3());
			mapvalues.put(groupby.get(3), tup.v4());
			mapvalues.put(groupby.get(4), tup.v5());
			mapvalues.put(groupby.get(5), tup.v6());
			mapvalues.put(groupby.get(6), tup.v7());
			mapvalues.put(groupby.get(7), tup.v8());
			mapvalues.put(groupby.get(8), tup.v9());
			mapvalues.put(groupby.get(9), tup.v10());
			mapvalues.put(groupby.get(10), tup.v11());
			mapvalues.put(groupby.get(11), tup.v12());
			mapvalues.put(groupby.get(12), tup.v13());
			mapvalues.put(groupby.get(13), tup.v14());
			mapvalues.put(groupby.get(14), tup.v15());
			mapvalues.put(groupby.get(15), tup.v16());
		}else {
			throw new UnsupportedOperationException("Max supported Column is 16");
		}
	}
	
	public static Long getCountFromTuple(Tuple tuple) {
		if(tuple instanceof Tuple1 tup) {
			return (Long) tup.v1;
		} else if(tuple instanceof Tuple2 tup) {
			return (Long) tup.v2;
		} else if(tuple instanceof Tuple3 tup) {
			return (Long) tup.v3;
		} else if(tuple instanceof Tuple4 tup) {
			return (Long) tup.v4;
		} else if(tuple instanceof Tuple5 tup) {
			return (Long) tup.v5;
		} else if(tuple instanceof Tuple6 tup) {
			return (Long) tup.v6;
		} else if(tuple instanceof Tuple7 tup) {
			return (Long) tup.v7;
		} else if(tuple instanceof Tuple8 tup) {
			return (Long) tup.v8;
		} else if(tuple instanceof Tuple9 tup) {
			return (Long) tup.v9;
		} else if(tuple instanceof Tuple10 tup) {
			return (Long) tup.v10;
		} else if(tuple instanceof Tuple11 tup) {
			return (Long) tup.v11;
		} else if(tuple instanceof Tuple12 tup) {
			return (Long) tup.v12;
		} else if(tuple instanceof Tuple13 tup) {
			return (Long) tup.v13;
		} else if(tuple instanceof Tuple14 tup) {
			return (Long) tup.v14;
		} else if(tuple instanceof Tuple15 tup) {
			return (Long) tup.v15;
		} else if(tuple instanceof Tuple16 tup) {
			return (Long) tup.v16;
		} else {
			throw new UnsupportedOperationException("Max supported Column is 16");
		}
	}
	
	/**
	 * Populate Map values from function.
	 * @param mapvalues
	 * @param tuple
	 * @param functions
	 * @param functionalias
	 */
	public static void populateMapFromFunctions(Map<String, Object> mapvalues, Tuple tuple, List<Function> functions,
			Map<Function, String> functionalias) {
		if(tuple instanceof Tuple1 tup) {
			mapvalues.put(getAliasForFunction(functions.get(0), functionalias), tup.v1());
		} else if(tuple instanceof Tuple2 tup) {
			mapvalues.put(getAliasForFunction(functions.get(0), functionalias), tup.v1());
			mapvalues.put(getAliasForFunction(functions.get(1), functionalias), tup.v2());
		} else if(tuple instanceof Tuple3 tup) {
			mapvalues.put(getAliasForFunction(functions.get(0), functionalias), tup.v1());
			mapvalues.put(getAliasForFunction(functions.get(1), functionalias), tup.v2());
			mapvalues.put(getAliasForFunction(functions.get(2), functionalias), tup.v3());
		} else if(tuple instanceof Tuple4 tup) {
			mapvalues.put(getAliasForFunction(functions.get(0), functionalias), tup.v1());
			mapvalues.put(getAliasForFunction(functions.get(1), functionalias), tup.v2());
			mapvalues.put(getAliasForFunction(functions.get(2), functionalias), tup.v3());
			mapvalues.put(getAliasForFunction(functions.get(3), functionalias), tup.v4());
		}else if(tuple instanceof Tuple5 tup) {
			mapvalues.put(getAliasForFunction(functions.get(0), functionalias), tup.v1());
			mapvalues.put(getAliasForFunction(functions.get(1), functionalias), tup.v2());
			mapvalues.put(getAliasForFunction(functions.get(2), functionalias), tup.v3());
			mapvalues.put(getAliasForFunction(functions.get(3), functionalias), tup.v4());
			mapvalues.put(getAliasForFunction(functions.get(4), functionalias), tup.v5());
		}else if(tuple instanceof Tuple6 tup) {
			mapvalues.put(getAliasForFunction(functions.get(0), functionalias), tup.v1());
			mapvalues.put(getAliasForFunction(functions.get(1), functionalias), tup.v2());
			mapvalues.put(getAliasForFunction(functions.get(2), functionalias), tup.v3());
			mapvalues.put(getAliasForFunction(functions.get(3), functionalias), tup.v4());
			mapvalues.put(getAliasForFunction(functions.get(4), functionalias), tup.v5());
			mapvalues.put(getAliasForFunction(functions.get(5), functionalias), tup.v6());
		}else if(tuple instanceof Tuple7 tup) {
			mapvalues.put(getAliasForFunction(functions.get(0), functionalias), tup.v1());
			mapvalues.put(getAliasForFunction(functions.get(1), functionalias), tup.v2());
			mapvalues.put(getAliasForFunction(functions.get(2), functionalias), tup.v3());
			mapvalues.put(getAliasForFunction(functions.get(3), functionalias), tup.v4());
			mapvalues.put(getAliasForFunction(functions.get(4), functionalias), tup.v5());
			mapvalues.put(getAliasForFunction(functions.get(5), functionalias), tup.v6());
			mapvalues.put(getAliasForFunction(functions.get(6), functionalias), tup.v7());
		}else if(tuple instanceof Tuple8 tup) {
			mapvalues.put(getAliasForFunction(functions.get(0), functionalias), tup.v1());
			mapvalues.put(getAliasForFunction(functions.get(1), functionalias), tup.v2());
			mapvalues.put(getAliasForFunction(functions.get(2), functionalias), tup.v3());
			mapvalues.put(getAliasForFunction(functions.get(3), functionalias), tup.v4());
			mapvalues.put(getAliasForFunction(functions.get(4), functionalias), tup.v5());
			mapvalues.put(getAliasForFunction(functions.get(5), functionalias), tup.v6());
			mapvalues.put(getAliasForFunction(functions.get(6), functionalias), tup.v7());
			mapvalues.put(getAliasForFunction(functions.get(7), functionalias), tup.v8());
		}else if(tuple instanceof Tuple9 tup) {
			mapvalues.put(getAliasForFunction(functions.get(0), functionalias), tup.v1());
			mapvalues.put(getAliasForFunction(functions.get(1), functionalias), tup.v2());
			mapvalues.put(getAliasForFunction(functions.get(2), functionalias), tup.v3());
			mapvalues.put(getAliasForFunction(functions.get(3), functionalias), tup.v4());
			mapvalues.put(getAliasForFunction(functions.get(4), functionalias), tup.v5());
			mapvalues.put(getAliasForFunction(functions.get(5), functionalias), tup.v6());
			mapvalues.put(getAliasForFunction(functions.get(6), functionalias), tup.v7());
			mapvalues.put(getAliasForFunction(functions.get(7), functionalias), tup.v8());
			mapvalues.put(getAliasForFunction(functions.get(8), functionalias), tup.v9());
		}else if(tuple instanceof Tuple10 tup) {
			mapvalues.put(getAliasForFunction(functions.get(0), functionalias), tup.v1());
			mapvalues.put(getAliasForFunction(functions.get(1), functionalias), tup.v2());
			mapvalues.put(getAliasForFunction(functions.get(2), functionalias), tup.v3());
			mapvalues.put(getAliasForFunction(functions.get(3), functionalias), tup.v4());
			mapvalues.put(getAliasForFunction(functions.get(4), functionalias), tup.v5());
			mapvalues.put(getAliasForFunction(functions.get(5), functionalias), tup.v6());
			mapvalues.put(getAliasForFunction(functions.get(6), functionalias), tup.v7());
			mapvalues.put(getAliasForFunction(functions.get(7), functionalias), tup.v8());
			mapvalues.put(getAliasForFunction(functions.get(8), functionalias), tup.v9());
			mapvalues.put(getAliasForFunction(functions.get(9), functionalias), tup.v10());
		}else if(tuple instanceof Tuple11 tup) {
			mapvalues.put(getAliasForFunction(functions.get(0), functionalias), tup.v1());
			mapvalues.put(getAliasForFunction(functions.get(1), functionalias), tup.v2());
			mapvalues.put(getAliasForFunction(functions.get(2), functionalias), tup.v3());
			mapvalues.put(getAliasForFunction(functions.get(3), functionalias), tup.v4());
			mapvalues.put(getAliasForFunction(functions.get(4), functionalias), tup.v5());
			mapvalues.put(getAliasForFunction(functions.get(5), functionalias), tup.v6());
			mapvalues.put(getAliasForFunction(functions.get(6), functionalias), tup.v7());
			mapvalues.put(getAliasForFunction(functions.get(7), functionalias), tup.v8());
			mapvalues.put(getAliasForFunction(functions.get(8), functionalias), tup.v9());
			mapvalues.put(getAliasForFunction(functions.get(9), functionalias), tup.v10());
			mapvalues.put(getAliasForFunction(functions.get(10), functionalias), tup.v11());
		}else if(tuple instanceof Tuple12 tup) {
			mapvalues.put(getAliasForFunction(functions.get(0), functionalias), tup.v1());
			mapvalues.put(getAliasForFunction(functions.get(1), functionalias), tup.v2());
			mapvalues.put(getAliasForFunction(functions.get(2), functionalias), tup.v3());
			mapvalues.put(getAliasForFunction(functions.get(3), functionalias), tup.v4());
			mapvalues.put(getAliasForFunction(functions.get(4), functionalias), tup.v5());
			mapvalues.put(getAliasForFunction(functions.get(5), functionalias), tup.v6());
			mapvalues.put(getAliasForFunction(functions.get(6), functionalias), tup.v7());
			mapvalues.put(getAliasForFunction(functions.get(7), functionalias), tup.v8());
			mapvalues.put(getAliasForFunction(functions.get(8), functionalias), tup.v9());
			mapvalues.put(getAliasForFunction(functions.get(9), functionalias), tup.v10());
			mapvalues.put(getAliasForFunction(functions.get(10), functionalias), tup.v11());
			mapvalues.put(getAliasForFunction(functions.get(11), functionalias), tup.v12());
		} else if(tuple instanceof Tuple13 tup) {
			mapvalues.put(getAliasForFunction(functions.get(0), functionalias), tup.v1());
			mapvalues.put(getAliasForFunction(functions.get(1), functionalias), tup.v2());
			mapvalues.put(getAliasForFunction(functions.get(2), functionalias), tup.v3());
			mapvalues.put(getAliasForFunction(functions.get(3), functionalias), tup.v4());
			mapvalues.put(getAliasForFunction(functions.get(4), functionalias), tup.v5());
			mapvalues.put(getAliasForFunction(functions.get(5), functionalias), tup.v6());
			mapvalues.put(getAliasForFunction(functions.get(6), functionalias), tup.v7());
			mapvalues.put(getAliasForFunction(functions.get(7), functionalias), tup.v8());
			mapvalues.put(getAliasForFunction(functions.get(8), functionalias), tup.v9());
			mapvalues.put(getAliasForFunction(functions.get(9), functionalias), tup.v10());
			mapvalues.put(getAliasForFunction(functions.get(10), functionalias), tup.v11());
			mapvalues.put(getAliasForFunction(functions.get(11), functionalias), tup.v12());
			mapvalues.put(getAliasForFunction(functions.get(12), functionalias), tup.v13());
		} else if(tuple instanceof Tuple14 tup) {
			mapvalues.put(getAliasForFunction(functions.get(0), functionalias), tup.v1());
			mapvalues.put(getAliasForFunction(functions.get(1), functionalias), tup.v2());
			mapvalues.put(getAliasForFunction(functions.get(2), functionalias), tup.v3());
			mapvalues.put(getAliasForFunction(functions.get(3), functionalias), tup.v4());
			mapvalues.put(getAliasForFunction(functions.get(4), functionalias), tup.v5());
			mapvalues.put(getAliasForFunction(functions.get(5), functionalias), tup.v6());
			mapvalues.put(getAliasForFunction(functions.get(6), functionalias), tup.v7());
			mapvalues.put(getAliasForFunction(functions.get(7), functionalias), tup.v8());
			mapvalues.put(getAliasForFunction(functions.get(8), functionalias), tup.v9());
			mapvalues.put(getAliasForFunction(functions.get(9), functionalias), tup.v10());
			mapvalues.put(getAliasForFunction(functions.get(10), functionalias), tup.v11());
			mapvalues.put(getAliasForFunction(functions.get(11), functionalias), tup.v12());
			mapvalues.put(getAliasForFunction(functions.get(12), functionalias), tup.v13());
			mapvalues.put(getAliasForFunction(functions.get(13), functionalias), tup.v14());
		}else if(tuple instanceof Tuple15 tup) {
			mapvalues.put(getAliasForFunction(functions.get(0), functionalias), tup.v1());
			mapvalues.put(getAliasForFunction(functions.get(1), functionalias), tup.v2());
			mapvalues.put(getAliasForFunction(functions.get(2), functionalias), tup.v3());
			mapvalues.put(getAliasForFunction(functions.get(3), functionalias), tup.v4());
			mapvalues.put(getAliasForFunction(functions.get(4), functionalias), tup.v5());
			mapvalues.put(getAliasForFunction(functions.get(5), functionalias), tup.v6());
			mapvalues.put(getAliasForFunction(functions.get(6), functionalias), tup.v7());
			mapvalues.put(getAliasForFunction(functions.get(7), functionalias), tup.v8());
			mapvalues.put(getAliasForFunction(functions.get(8), functionalias), tup.v9());
			mapvalues.put(getAliasForFunction(functions.get(9), functionalias), tup.v10());
			mapvalues.put(getAliasForFunction(functions.get(10), functionalias), tup.v11());
			mapvalues.put(getAliasForFunction(functions.get(11), functionalias), tup.v12());
			mapvalues.put(getAliasForFunction(functions.get(12), functionalias), tup.v13());
			mapvalues.put(getAliasForFunction(functions.get(13), functionalias), tup.v14());
			mapvalues.put(getAliasForFunction(functions.get(14), functionalias), tup.v15());
		}else if(tuple instanceof Tuple16 tup) {
			mapvalues.put(getAliasForFunction(functions.get(0), functionalias), tup.v1());
			mapvalues.put(getAliasForFunction(functions.get(1), functionalias), tup.v2());
			mapvalues.put(getAliasForFunction(functions.get(2), functionalias), tup.v3());
			mapvalues.put(getAliasForFunction(functions.get(3), functionalias), tup.v4());
			mapvalues.put(getAliasForFunction(functions.get(4), functionalias), tup.v5());
			mapvalues.put(getAliasForFunction(functions.get(5), functionalias), tup.v6());
			mapvalues.put(getAliasForFunction(functions.get(6), functionalias), tup.v7());
			mapvalues.put(getAliasForFunction(functions.get(7), functionalias), tup.v8());
			mapvalues.put(getAliasForFunction(functions.get(8), functionalias), tup.v9());
			mapvalues.put(getAliasForFunction(functions.get(9), functionalias), tup.v10());
			mapvalues.put(getAliasForFunction(functions.get(10), functionalias), tup.v11());
			mapvalues.put(getAliasForFunction(functions.get(11), functionalias), tup.v12());
			mapvalues.put(getAliasForFunction(functions.get(12), functionalias), tup.v13());
			mapvalues.put(getAliasForFunction(functions.get(13), functionalias), tup.v14());
			mapvalues.put(getAliasForFunction(functions.get(14), functionalias), tup.v15());
			mapvalues.put(getAliasForFunction(functions.get(15), functionalias), tup.v16());
		}else {
			throw new UnsupportedOperationException("Max supported Column is 16");
		}
	}
	
	
	public static String getAliasForFunction(Function function, Map<Function, String> functionalias) {
		String aliasfunction = functionalias.get(function);
		String alias = nonNull(aliasfunction) ? aliasfunction
				: function.toString();
		return alias;
	}
	
	/**
	 * Evaluate Tuple for functions
	 * @param tuple1
	 * @param tuple2
	 * @param aggfunctions
	 * @return Tuple
	 */
	public static Tuple evaluateTuple(Tuple tuple1,Tuple tuple2, List<Function> aggfunctions) {
		if(tuple1 instanceof Tuple1 tup11 && tuple2 instanceof Tuple1 tup21) {
			return Tuple.tuple(evaluateFunction(tup11.v1(), tup21.v1(), aggfunctions.get(0)));
		} else if(tuple1 instanceof Tuple2 tup12 && tuple2 instanceof Tuple2 tup22) {
			return Tuple.tuple(evaluateFunction(tup12.v1(), tup22.v1(), aggfunctions.get(0)), 
					evaluateFunction(tup12.v2(), tup22.v2(), aggfunctions.get(1)));
		} else if(tuple1 instanceof Tuple3 tup13 && tuple2 instanceof Tuple3 tup23) {
			return Tuple.tuple(evaluateFunction(tup13.v1(), tup23.v1(), aggfunctions.get(0)), 
					evaluateFunction(tup13.v2(), tup23.v2(), aggfunctions.get(1)), 
					evaluateFunction(tup13.v3(), tup23.v3(), aggfunctions.get(2)));
		} else if(tuple1 instanceof Tuple4 tup14 && tuple2 instanceof Tuple4 tup24) {
			return Tuple.tuple(evaluateFunction(tup14.v1(), tup24.v1(), aggfunctions.get(0)), 
					evaluateFunction(tup14.v2(), tup24.v2(), aggfunctions.get(1)), 
					evaluateFunction(tup14.v3(), tup24.v3(), aggfunctions.get(2)),
					evaluateFunction(tup14.v4(), tup24.v4(), aggfunctions.get(3)));
		} else if(tuple1 instanceof Tuple5 tup15 && tuple2 instanceof Tuple5 tup25) {
			return Tuple.tuple(evaluateFunction(tup15.v1(), tup25.v1(), aggfunctions.get(0)), 
					evaluateFunction(tup15.v2(), tup25.v2(), aggfunctions.get(1)), 
					evaluateFunction(tup15.v3(), tup25.v3(), aggfunctions.get(2)),
					evaluateFunction(tup15.v4(), tup25.v4(), aggfunctions.get(3)),
					evaluateFunction(tup15.v5(), tup25.v5(), aggfunctions.get(4)));
		} else if(tuple1 instanceof Tuple6 tup16 && tuple2 instanceof Tuple6 tup26) {
			return Tuple.tuple(evaluateFunction(tup16.v1(), tup26.v1(), aggfunctions.get(0)), 
					evaluateFunction(tup16.v2(), tup26.v2(), aggfunctions.get(1)), 
					evaluateFunction(tup16.v3(), tup26.v3(), aggfunctions.get(2)),
					evaluateFunction(tup16.v4(), tup26.v4(), aggfunctions.get(3)),
					evaluateFunction(tup16.v5(), tup26.v5(), aggfunctions.get(4)),
					evaluateFunction(tup16.v6(), tup26.v6(), aggfunctions.get(5)));
		} else if(tuple1 instanceof Tuple7 tup17 && tuple2 instanceof Tuple7 tup27) {
			return Tuple.tuple(evaluateFunction(tup17.v1(), tup27.v1(), aggfunctions.get(0)), 
					evaluateFunction(tup17.v2(), tup27.v2(), aggfunctions.get(1)), 
					evaluateFunction(tup17.v3(), tup27.v3(), aggfunctions.get(2)),
					evaluateFunction(tup17.v4(), tup27.v4(), aggfunctions.get(3)),
					evaluateFunction(tup17.v5(), tup27.v5(), aggfunctions.get(4)),
					evaluateFunction(tup17.v6(), tup27.v6(), aggfunctions.get(5)),
					evaluateFunction(tup17.v7(), tup27.v7(), aggfunctions.get(6)));
		} else if(tuple1 instanceof Tuple8 tup18 && tuple2 instanceof Tuple8 tup28) {
			return Tuple.tuple(evaluateFunction(tup18.v1(), tup28.v1(), aggfunctions.get(0)), 
					evaluateFunction(tup18.v2(), tup28.v2(), aggfunctions.get(1)), 
					evaluateFunction(tup18.v3(), tup28.v3(), aggfunctions.get(2)),
					evaluateFunction(tup18.v4(), tup28.v4(), aggfunctions.get(3)),
					evaluateFunction(tup18.v5(), tup28.v5(), aggfunctions.get(4)),
					evaluateFunction(tup18.v6(), tup28.v6(), aggfunctions.get(5)),
					evaluateFunction(tup18.v7(), tup28.v7(), aggfunctions.get(6)),
					evaluateFunction(tup18.v8(), tup28.v8(), aggfunctions.get(7)));
		} else if(tuple1 instanceof Tuple9 tup19 && tuple2 instanceof Tuple9 tup29) {
			return Tuple.tuple(evaluateFunction(tup19.v1(), tup29.v1(), aggfunctions.get(0)), 
					evaluateFunction(tup19.v2(), tup29.v2(), aggfunctions.get(1)), 
					evaluateFunction(tup19.v3(), tup29.v3(), aggfunctions.get(2)),
					evaluateFunction(tup19.v4(), tup29.v4(), aggfunctions.get(3)),
					evaluateFunction(tup19.v5(), tup29.v5(), aggfunctions.get(4)),
					evaluateFunction(tup19.v6(), tup29.v6(), aggfunctions.get(5)),
					evaluateFunction(tup19.v7(), tup29.v7(), aggfunctions.get(6)),
					evaluateFunction(tup19.v8(), tup29.v8(), aggfunctions.get(7)),
					evaluateFunction(tup19.v9(), tup29.v9(), aggfunctions.get(8)));
		} else if(tuple1 instanceof Tuple10 tup1_10 && tuple2 instanceof Tuple10 tup2_10) {
			return Tuple.tuple(evaluateFunction(tup1_10.v1(), tup2_10.v1(), aggfunctions.get(0)), 
					evaluateFunction(tup1_10.v2(), tup2_10.v2(), aggfunctions.get(1)), 
					evaluateFunction(tup1_10.v3(), tup2_10.v3(), aggfunctions.get(2)),
					evaluateFunction(tup1_10.v4(), tup2_10.v4(), aggfunctions.get(3)),
					evaluateFunction(tup1_10.v5(), tup2_10.v5(), aggfunctions.get(4)),
					evaluateFunction(tup1_10.v6(), tup2_10.v6(), aggfunctions.get(5)),
					evaluateFunction(tup1_10.v7(), tup2_10.v7(), aggfunctions.get(6)),
					evaluateFunction(tup1_10.v8(), tup2_10.v8(), aggfunctions.get(7)),
					evaluateFunction(tup1_10.v9(), tup2_10.v9(), aggfunctions.get(8)),
					evaluateFunction(tup1_10.v10(), tup2_10.v10(), aggfunctions.get(9)));
		} else if(tuple1 instanceof Tuple11 tup1_11 && tuple2 instanceof Tuple11 tup2_11) {
			return Tuple.tuple(evaluateFunction(tup1_11.v1(), tup2_11.v1(), aggfunctions.get(0)), 
					evaluateFunction(tup1_11.v2(), tup2_11.v2(), aggfunctions.get(1)), 
					evaluateFunction(tup1_11.v3(), tup2_11.v3(), aggfunctions.get(2)),
					evaluateFunction(tup1_11.v4(), tup2_11.v4(), aggfunctions.get(3)),
					evaluateFunction(tup1_11.v5(), tup2_11.v5(), aggfunctions.get(4)),
					evaluateFunction(tup1_11.v6(), tup2_11.v6(), aggfunctions.get(5)),
					evaluateFunction(tup1_11.v7(), tup2_11.v7(), aggfunctions.get(6)),
					evaluateFunction(tup1_11.v8(), tup2_11.v8(), aggfunctions.get(7)),
					evaluateFunction(tup1_11.v9(), tup2_11.v9(), aggfunctions.get(8)),
					evaluateFunction(tup1_11.v10(), tup2_11.v10(), aggfunctions.get(9)),
					evaluateFunction(tup1_11.v11(), tup2_11.v11(), aggfunctions.get(10)));
		} else if(tuple1 instanceof Tuple12 tup1_12 && tuple2 instanceof Tuple12 tup2_12) {
			return Tuple.tuple(evaluateFunction(tup1_12.v1(), tup2_12.v1(), aggfunctions.get(0)), 
					evaluateFunction(tup1_12.v2(), tup2_12.v2(), aggfunctions.get(1)), 
					evaluateFunction(tup1_12.v3(), tup2_12.v3(), aggfunctions.get(2)),
					evaluateFunction(tup1_12.v4(), tup2_12.v4(), aggfunctions.get(3)),
					evaluateFunction(tup1_12.v5(), tup2_12.v5(), aggfunctions.get(4)),
					evaluateFunction(tup1_12.v6(), tup2_12.v6(), aggfunctions.get(5)),
					evaluateFunction(tup1_12.v7(), tup2_12.v7(), aggfunctions.get(6)),
					evaluateFunction(tup1_12.v8(), tup2_12.v8(), aggfunctions.get(7)),
					evaluateFunction(tup1_12.v9(), tup2_12.v9(), aggfunctions.get(8)),
					evaluateFunction(tup1_12.v10(), tup2_12.v10(), aggfunctions.get(9)),
					evaluateFunction(tup1_12.v11(), tup2_12.v11(), aggfunctions.get(10)),
					evaluateFunction(tup1_12.v12(), tup2_12.v12(), aggfunctions.get(11)));
		} else if(tuple1 instanceof Tuple13 tup1_13 && tuple2 instanceof Tuple13 tup2_13) {
			return Tuple.tuple(evaluateFunction(tup1_13.v1(), tup2_13.v1(), aggfunctions.get(0)), 
					evaluateFunction(tup1_13.v2(), tup2_13.v2(), aggfunctions.get(1)), 
					evaluateFunction(tup1_13.v3(), tup2_13.v3(), aggfunctions.get(2)),
					evaluateFunction(tup1_13.v4(), tup2_13.v4(), aggfunctions.get(3)),
					evaluateFunction(tup1_13.v5(), tup2_13.v5(), aggfunctions.get(4)),
					evaluateFunction(tup1_13.v6(), tup2_13.v6(), aggfunctions.get(5)),
					evaluateFunction(tup1_13.v7(), tup2_13.v7(), aggfunctions.get(6)),
					evaluateFunction(tup1_13.v8(), tup2_13.v8(), aggfunctions.get(7)),
					evaluateFunction(tup1_13.v9(), tup2_13.v9(), aggfunctions.get(8)),
					evaluateFunction(tup1_13.v10(), tup2_13.v10(), aggfunctions.get(9)),
					evaluateFunction(tup1_13.v11(), tup2_13.v11(), aggfunctions.get(10)),
					evaluateFunction(tup1_13.v12(), tup2_13.v12(), aggfunctions.get(11)),
					evaluateFunction(tup1_13.v13(), tup2_13.v13(), aggfunctions.get(12)));
		} else if(tuple1 instanceof Tuple14 tup1_14 && tuple2 instanceof Tuple14 tup2_14) {
			return Tuple.tuple(evaluateFunction(tup1_14.v1(), tup2_14.v1(), aggfunctions.get(0)), 
					evaluateFunction(tup1_14.v2(), tup2_14.v2(), aggfunctions.get(1)), 
					evaluateFunction(tup1_14.v3(), tup2_14.v3(), aggfunctions.get(2)),
					evaluateFunction(tup1_14.v4(), tup2_14.v4(), aggfunctions.get(3)),
					evaluateFunction(tup1_14.v5(), tup2_14.v5(), aggfunctions.get(4)),
					evaluateFunction(tup1_14.v6(), tup2_14.v6(), aggfunctions.get(5)),
					evaluateFunction(tup1_14.v7(), tup2_14.v7(), aggfunctions.get(6)),
					evaluateFunction(tup1_14.v8(), tup2_14.v8(), aggfunctions.get(7)),
					evaluateFunction(tup1_14.v9(), tup2_14.v9(), aggfunctions.get(8)),
					evaluateFunction(tup1_14.v10(), tup2_14.v10(), aggfunctions.get(9)),
					evaluateFunction(tup1_14.v11(), tup2_14.v11(), aggfunctions.get(10)),
					evaluateFunction(tup1_14.v12(), tup2_14.v12(), aggfunctions.get(11)),
					evaluateFunction(tup1_14.v13(), tup2_14.v13(), aggfunctions.get(12)),
					evaluateFunction(tup1_14.v14(), tup2_14.v14(), aggfunctions.get(13)));
		} else if(tuple1 instanceof Tuple15 tup1_15 && tuple2 instanceof Tuple15 tup2_15) {
			return Tuple.tuple(evaluateFunction(tup1_15.v1(), tup2_15.v1(), aggfunctions.get(0)), 
					evaluateFunction(tup1_15.v2(), tup2_15.v2(), aggfunctions.get(1)), 
					evaluateFunction(tup1_15.v3(), tup2_15.v3(), aggfunctions.get(2)),
					evaluateFunction(tup1_15.v4(), tup2_15.v4(), aggfunctions.get(3)),
					evaluateFunction(tup1_15.v5(), tup2_15.v5(), aggfunctions.get(4)),
					evaluateFunction(tup1_15.v6(), tup2_15.v6(), aggfunctions.get(5)),
					evaluateFunction(tup1_15.v7(), tup2_15.v7(), aggfunctions.get(6)),
					evaluateFunction(tup1_15.v8(), tup2_15.v8(), aggfunctions.get(7)),
					evaluateFunction(tup1_15.v9(), tup2_15.v9(), aggfunctions.get(8)),
					evaluateFunction(tup1_15.v10(), tup2_15.v10(), aggfunctions.get(9)),
					evaluateFunction(tup1_15.v11(), tup2_15.v11(), aggfunctions.get(10)),
					evaluateFunction(tup1_15.v12(), tup2_15.v12(), aggfunctions.get(11)),
					evaluateFunction(tup1_15.v13(), tup2_15.v13(), aggfunctions.get(12)),
					evaluateFunction(tup1_15.v14(), tup2_15.v14(), aggfunctions.get(13)),
					evaluateFunction(tup1_15.v15(), tup2_15.v15(), aggfunctions.get(14)));
		} else if(tuple1 instanceof Tuple16 tup1_16 && tuple2 instanceof Tuple16 tup2_16) {
			return Tuple.tuple(evaluateFunction(tup1_16.v1(), tup2_16.v1(), aggfunctions.get(0)), 
					evaluateFunction(tup1_16.v2(), tup2_16.v2(), aggfunctions.get(1)), 
					evaluateFunction(tup1_16.v3(), tup2_16.v3(), aggfunctions.get(2)),
					evaluateFunction(tup1_16.v4(), tup2_16.v4(), aggfunctions.get(3)),
					evaluateFunction(tup1_16.v5(), tup2_16.v5(), aggfunctions.get(4)),
					evaluateFunction(tup1_16.v6(), tup2_16.v6(), aggfunctions.get(5)),
					evaluateFunction(tup1_16.v7(), tup2_16.v7(), aggfunctions.get(6)),
					evaluateFunction(tup1_16.v8(), tup2_16.v8(), aggfunctions.get(7)),
					evaluateFunction(tup1_16.v9(), tup2_16.v9(), aggfunctions.get(8)),
					evaluateFunction(tup1_16.v10(), tup2_16.v10(), aggfunctions.get(9)),
					evaluateFunction(tup1_16.v11(), tup2_16.v11(), aggfunctions.get(10)),
					evaluateFunction(tup1_16.v12(), tup2_16.v12(), aggfunctions.get(11)),
					evaluateFunction(tup1_16.v13(), tup2_16.v13(), aggfunctions.get(12)),
					evaluateFunction(tup1_16.v14(), tup2_16.v14(), aggfunctions.get(13)),
					evaluateFunction(tup1_16.v15(), tup2_16.v15(), aggfunctions.get(14)),
					evaluateFunction(tup1_16.v16(), tup2_16.v16(), aggfunctions.get(15)));
		} else {
			throw new UnsupportedOperationException("Max supported Column is 16");
		}
	}
	
	/**
	 * Evaluate values based on functions
	 * @param leftValue
	 * @param rightValue
	 * @param function
	 * @return evaluated value
	 */
	public static Object evaluateFunction(Object leftValue, Object rightValue, Function function) {
		String functionname = function.getName().toLowerCase();
		if (functionname.startsWith("count") || functionname.startsWith("sum") || functionname.startsWith("avg")) {
			return evaluateValuesByOperator(leftValue, rightValue, "+");
		} else if (functionname.startsWith("grpconcat")) {
			StringValue sv = (StringValue) function.getParameters().getExpressions().get(1);
			return evaluateGroupConcat(leftValue, rightValue, sv.getValue());
		} else if (functionname.startsWith("min")) {
			return SQLUtils.evaluateValuesByFunctionMin(leftValue, rightValue);
		} else if (functionname.startsWith("max")) {
			return SQLUtils.evaluateValuesByFunctionMax(leftValue, rightValue);
		}
		return null;
	}
	
	/**
	 * Evaluates group concat with append string
	 * @param leftValue
	 * @param rightValue
	 * @param appendstring
	 * @return concatenated string
	 */
	public static Object evaluateGroupConcat(Object leftValue, Object rightValue, String appendstring) {
			if (leftValue instanceof String lv && rightValue instanceof Double rv) {
				return lv + appendstring + rv;
			} else if(leftValue instanceof Double lv && rightValue instanceof Double rv) {
				return lv + appendstring + rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Double rv) {
				return lv + appendstring + rv;
			} else if(leftValue instanceof Double lv && rightValue instanceof Long rv) {
				return lv + appendstring + rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Long rv) {
				return lv + appendstring + rv;
			} else if(leftValue instanceof String lv && rightValue instanceof Long rv) {
				return lv + appendstring + rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof String rv) {
				return lv + appendstring + rv;
			} else if(leftValue instanceof Double lv && rightValue instanceof String rv) {
				return lv + appendstring + rv;
			} else if(leftValue instanceof Integer lv && rightValue instanceof Integer rv) {
				return lv + appendstring + rv;
			} else if (leftValue instanceof String lv && rightValue instanceof String rv) {
				return lv + appendstring + rv;
			} else if(leftValue instanceof Long lv && rightValue instanceof Integer rv) {
				return lv + appendstring + rv;
			} else if(leftValue instanceof Integer lv && rightValue instanceof Long rv) {
				return lv + appendstring + rv;
			}
			return leftValue + appendstring + rightValue;
	}
	
	
	/**
	 * Validates and optimizes sql query
	 * @param tablecolumnsmap
	 * @param tablecolumntypesmap
	 * @param sql
	 * @param db
	 * @return optimized query
	 * @throws Exception
	 */
	public static void validateSql(ConcurrentMap<String, List<String>> tablecolumnsmap,
			ConcurrentMap<String, List<SqlTypeName>> tablecolumntypesmap,
			String sql,
			String db) throws Exception {
		Set<String> tablesfromconfig = tablecolumnsmap.keySet();
		SimpleSchema.Builder builder = SimpleSchema.newBuilder(db);
		for (String table : tablesfromconfig) {
			List<String> columns = tablecolumnsmap.get(table);
			List<SqlTypeName> sqltypes = tablecolumntypesmap.get(table);
			builder.addTable(getSimpleTable(table, columns.toArray(new String[columns.size()]), sqltypes.toArray(new SqlTypeName[sqltypes.size()])));
		}
		SimpleSchema schema = builder.build();
		Optimizer optimizer = Optimizer.create(schema);
		SqlNode sqlTree = optimizer.parse(sql);
		optimizer.validate(sqlTree);
		
	}
	
	/**
	 * Get simple table given the tablename, fields and types
	 * @param tablename
	 * @param fields
	 * @param types
	 * @return
	 */
	private static SimpleTable getSimpleTable(String tablename, String[] fields, SqlTypeName[] types) {
		SimpleTable.Builder builder = SimpleTable.newBuilder(tablename);
		int typecount = 0;
		for (String field : fields) {
			builder = builder.addField(field, types[typecount]);
			typecount++;
		}
		return builder.withRowCount(60000L).build();
	}
	
	/**
	 * Get All Tables from Statement Object
	 * @param statement
	 * @param tables
	 * @param tmptables
	 */
	public static void getAllTables(Object statement,List<Table> tables,Set<String> tmptables){
		if ((statement instanceof Select select && select.getSelectBody() instanceof PlainSelect)
				|| statement instanceof PlainSelect) {
			var plainSelect = (PlainSelect) (statement instanceof PlainSelect ? statement
					: ((Select) statement).getSelectBody());
			FromItem fromItem = plainSelect.getFromItem();
			if (fromItem instanceof SubSelect) {
				SelectBody subSelectBody = ((SubSelect) fromItem).getSelectBody();
				if (subSelectBody instanceof PlainSelect) {
					getAllTables(subSelectBody, tables, tmptables);
				}
			} else if (fromItem instanceof Table table) {
				if(!tmptables.contains(table.getName())) {
					tables.add(table);
					if (nonNull(plainSelect.getJoins())) {
						for (Join join : plainSelect.getJoins()) {
							tables.add(((Table) join.getRightItem()));
							tmptables.add(((Table) join.getRightItem()).getName());
						}
					}
				}
				tmptables.add(table.getName());
			}
		}
	}
	
}
