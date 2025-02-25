package com.github.datasamudaya.stream.utils;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.arrow.vector.types.FloatingPointPrecision;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.arrow.vector.util.Text;
import org.apache.calcite.adapter.enumerable.EnumerableAggregateBase;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRules;
import org.apache.calcite.linq4j.Ord;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlFloorFunction;
import org.apache.calcite.sql.fun.SqlTrimFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql2rel.RelFieldTrimmer;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RuleSet;
import org.apache.calcite.tools.RuleSets;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.RangeSets;
import org.apache.calcite.util.Sarg;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.OrcFile.WriterOptions;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.ehcache.Cache;
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

import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.Command;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.ExecuteTaskActor;
import com.github.datasamudaya.common.FileSystemSupport;
import com.github.datasamudaya.common.GetTaskActor;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.TaskStatus;
import com.github.datasamudaya.common.TaskType;
import com.github.datasamudaya.common.functions.Coalesce;
import com.github.datasamudaya.common.functions.DistributedDistinct;
import com.github.datasamudaya.common.functions.FullOuterJoin;
import com.github.datasamudaya.common.functions.IntersectionFunction;
import com.github.datasamudaya.common.functions.JoinPredicate;
import com.github.datasamudaya.common.functions.LeftJoin;
import com.github.datasamudaya.common.functions.LeftOuterJoinPredicate;
import com.github.datasamudaya.common.functions.ReduceByKeyFunction;
import com.github.datasamudaya.common.functions.RightJoin;
import com.github.datasamudaya.common.functions.RightOuterJoinPredicate;
import com.github.datasamudaya.common.functions.ShuffleStage;
import com.github.datasamudaya.common.functions.UnionFunction;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.common.utils.sql.JoinKeysSQL;
import com.github.datasamudaya.common.utils.sql.Optimizer;
import com.github.datasamudaya.common.utils.sql.SimpleSchema;
import com.github.datasamudaya.common.utils.sql.SimpleTable;
import com.github.datasamudaya.stream.CsvOptionsSQL;
import com.github.datasamudaya.stream.executors.actors.ProcessCoalesce;
import com.github.datasamudaya.stream.executors.actors.ProcessDistributedDistinct;
import com.github.datasamudaya.stream.executors.actors.ProcessDistributedSort;
import com.github.datasamudaya.stream.executors.actors.ProcessFullOuterJoin;
import com.github.datasamudaya.stream.executors.actors.ProcessInnerJoin;
import com.github.datasamudaya.stream.executors.actors.ProcessIntersection;
import com.github.datasamudaya.stream.executors.actors.ProcessLeftOuterJoin;
import com.github.datasamudaya.stream.executors.actors.ProcessMapperByBlocksLocation;
import com.github.datasamudaya.stream.executors.actors.ProcessMapperByStream;
import com.github.datasamudaya.stream.executors.actors.ProcessReduce;
import com.github.datasamudaya.stream.executors.actors.ProcessRightOuterJoin;
import com.github.datasamudaya.stream.executors.actors.ProcessShuffle;
import com.github.datasamudaya.stream.executors.actors.ProcessUnion;
import com.google.common.collect.Range;

import akka.actor.Address;
import akka.actor.typed.ActorRef;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.RecipientRef;
import akka.actor.typed.javadsl.Behaviors;
import akka.cluster.Cluster;
import akka.cluster.sharding.external.ExternalShardAllocation;
import akka.cluster.sharding.external.ExternalShardAllocationStrategy;
import akka.cluster.sharding.external.javadsl.ExternalShardAllocationClient;
import akka.cluster.sharding.typed.ShardingEnvelope;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.Entity;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import jp.co.yahoo.yosegi.message.objects.BooleanObj;
import jp.co.yahoo.yosegi.message.objects.DoubleObj;
import jp.co.yahoo.yosegi.message.objects.FloatObj;
import jp.co.yahoo.yosegi.message.objects.IntegerObj;
import jp.co.yahoo.yosegi.message.objects.LongObj;
import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import jp.co.yahoo.yosegi.message.objects.StringObj;
import jp.co.yahoo.yosegi.reader.YosegiSchemaReader;
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
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.GroupByElement;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;

/**
 * This class is utility class for getting datatype of h2 to calcite format.
 * 
 * @author arun
 *
 */
public class SQLUtils {

	private static final Logger log = LoggerFactory.getLogger(SQLUtils.class);

	private SQLUtils() {
	}

	/**
	 * Static function H2 datatype to calcite format.
	 * 
	 * @param h2datatype
	 * @return type in calcite format
	 */
	public static SqlTypeName getSQLTypeName(String h2datatype) {
		if ("4".equals(h2datatype)) {
			return SqlTypeName.INTEGER;
		} else if ("8".equals(h2datatype)) {
			return SqlTypeName.DOUBLE;
		} else if ("12".equals(h2datatype)) {
			return SqlTypeName.VARCHAR;
		} else {
			return SqlTypeName.VARCHAR;
		}
	}

	/**
	 * The function returns calcite sql type from hive datatype
	 * 
	 * @param hivedatatype
	 * @return calcite datatype
	 */
	public static SqlTypeName getHiveSQLTypeName(String hivedatatype) {
		if ("int".equals(hivedatatype)) {
			return SqlTypeName.INTEGER;
		} else if ("double".equals(hivedatatype)) {
			return SqlTypeName.DOUBLE;
		} else if ("string".equals(hivedatatype)) {
			return SqlTypeName.VARCHAR;
		} else {
			return SqlTypeName.VARCHAR;
		}
	}

	/**
	 * Static function H2 datatype to calcite format.
	 * 
	 * @param h2datatype
	 * @return type in calcite format
	 */
	public static SqlTypeName getSQLTypeNameMR(String h2datatype) {
		if ("4".equals(h2datatype)) {
			return SqlTypeName.DOUBLE;
		} else if ("12".equals(h2datatype)) {
			return SqlTypeName.VARCHAR;
		} else {
			return SqlTypeName.VARCHAR;
		}
	}

	/**
	 * This static method converts the value to given format.
	 * 
	 * @param value
	 * @param type
	 * @return value in given format.
	 */
	public static Object getValue(String value, SqlTypeName type) {
		try {
			if (type == SqlTypeName.INTEGER) {
				return Integer.valueOf(value);
			} else if (type == SqlTypeName.BIGINT) {
				return Long.valueOf(value);
			} else if (type == SqlTypeName.VARCHAR) {
				return String.valueOf(value);
			} else if (type == SqlTypeName.CHAR) {
				return String.valueOf(value);
			} else if (type == SqlTypeName.FLOAT) {
				return Float.valueOf(value);
			} else if (type == SqlTypeName.DOUBLE) {
				return Double.valueOf(value);
			} else if (type == SqlTypeName.DECIMAL) {
				return Double.valueOf(value);
			} else {
				return String.valueOf(value);
			}
		} catch (Exception ex) {
			if (type == SqlTypeName.INTEGER) {
				return Integer.valueOf(0);
			} else if (type == SqlTypeName.BIGINT) {
				return Long.valueOf(0);
			} else if (type == SqlTypeName.VARCHAR) {
				return String.valueOf(DataSamudayaConstants.EMPTY);
			} else if (type == SqlTypeName.FLOAT) {
				return Float.valueOf(0.0f);
			} else if (type == SqlTypeName.DOUBLE) {
				return Double.valueOf(0.0d);
			} else {
				return String.valueOf(DataSamudayaConstants.EMPTY);
			}
		}
	}

	/**
	 * Get Column Index Map for columns in table
	 * 
	 * @param column
	 * @return columnindexmap
	 */
	public static Map<String, Integer> getColumnIndexMap(List<String> column) {
		Map<String, Integer> columnindexmap = new ConcurrentHashMap<>();
		for (int originalcolumnindex = 0;originalcolumnindex < column.size();originalcolumnindex++) {
			columnindexmap.put(column.get(originalcolumnindex), Integer.valueOf(originalcolumnindex));
		}
		return columnindexmap;
	}

	/**
	 * This static method converts the mr values to given format.
	 * 
	 * @param value
	 * @param type
	 * @return value in given format.
	 */
	public static Object getValueMR(String value, SqlTypeName type) {
		try {
			if (type == SqlTypeName.INTEGER) {
				return Integer.valueOf(value);
			} else if (type == SqlTypeName.BIGINT) {
				return Long.valueOf(value);
			} else if (type == SqlTypeName.VARCHAR) {
				return String.valueOf(value);
			} else if (type == SqlTypeName.FLOAT) {
				return Float.valueOf(value);
			} else if (type == SqlTypeName.DOUBLE) {
				return Double.valueOf(value);
			} else {
				return String.valueOf(value);
			}
		} catch (Exception ex) {
			if (type == SqlTypeName.INTEGER) {
				return Integer.valueOf(0);
			} else if (type == SqlTypeName.BIGINT) {
				return Long.valueOf(0);
			} else if (type == SqlTypeName.VARCHAR) {
				return String.valueOf(DataSamudayaConstants.EMPTY);
			} else if (type == SqlTypeName.FLOAT) {
				return Float.valueOf(0.0f);
			} else if (type == SqlTypeName.DOUBLE) {
				return Double.valueOf(0.0d);
			} else {
				return String.valueOf(DataSamudayaConstants.EMPTY);
			}
		}
	}

	/**
	 * Get arrow file to store columnar data
	 * 
	 * @return
	 */
	protected static String getArrowFilePath() {
		String tmpdir = isNull(DataSamudayaProperties.get()) ? System.getProperty(DataSamudayaConstants.TMPDIR)
				: DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TMPDIR,
				System.getProperty(DataSamudayaConstants.TMPDIR));
		new File(tmpdir + DataSamudayaConstants.FORWARD_SLASH + FileSystemSupport.MDS).mkdirs();
		return tmpdir + DataSamudayaConstants.FORWARD_SLASH + FileSystemSupport.MDS
				+ DataSamudayaConstants.FORWARD_SLASH + UUID.randomUUID().toString()
				+ DataSamudayaConstants.ARROWFILE_EXT;
	}

	/**
	 * Decompress VectorSchemaRoot from bytes
	 * 
	 * @param arrowfilewithpath
	 * @return vector schema root
	 * @throws Exception
	 */
	public static VectorSchemaRoot decompressVectorSchemaRootBytes(String arrowfilewithpath) throws Exception {
		RootAllocator rootallocator = new RootAllocator(Long.MAX_VALUE);
		try (FileInputStream in = new FileInputStream(arrowfilewithpath);
				SnappyInputStream snappyInputStream = new SnappyInputStream(in);
				ArrowStreamReader reader = new ArrowStreamReader(snappyInputStream, rootallocator);) {
			reader.loadNextBatch();
			// Initialize the VectorSchemaRoot
			Schema schema = reader.getVectorSchemaRoot().getSchema();
			VectorSchemaRoot mergedRoot = VectorSchemaRoot.create(schema, rootallocator);
			long rowcount = 0;
			// Iterate through record batches and merge data
			do {
				for (int i = 0;i < schema.getFields().size();i++) {
					// Get the source vector in the current batch
					FieldVector sourceVector = reader.getVectorSchemaRoot()
							.getVector(schema.getFields().get(i).getName());

					// Get the destination vector in the merged root
					FieldVector destVector = mergedRoot.getVector(schema.getFields().get(i).getName());

					// Transfer data from source to destination vector
					accumulateVector(destVector, sourceVector);
				}
				rowcount += reader.getVectorSchemaRoot().getRowCount();
			} while (reader.loadNextBatch());
			// Optionally adjust the valueCount and fieldNodeCount in mergedRoot
			mergedRoot.setRowCount(Long.valueOf(rowcount).intValue());
			return mergedRoot;
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
		return null;

	}

	/**
	 * Accumulate destination vector from source vector
	 * 
	 * @param destVector
	 * @param sourceVector
	 * @throws Exception
	 */
	private static void accumulateVector(FieldVector destVector, FieldVector sourceVector) throws Exception {
		// Implement the accumulation logic based on the specific data types
		// (e.g., addition for numeric types)
		int valueCount = sourceVector.getValueCount();
		for (int i = 0;i < valueCount;i++) {
			if (!sourceVector.isNull(i)) {
				setValue(destVector.getValueCount() + i, destVector, getVectorValue(i, sourceVector));
			}
		}
		destVector.setValueCount(destVector.getValueCount() + valueCount);
	}

	/**
	 * Gets Schema Field for given column and type.
	 * 
	 * @param column
	 * @param type
	 * @return Field
	 */
	protected static Field getSchemaField(String column, SqlTypeName type) {
		if (type == SqlTypeName.INTEGER) {
			return Field.nullable(column, new ArrowType.Int(32, true));
		} else if (type == SqlTypeName.BIGINT) {
			return Field.nullable(column, new ArrowType.Int(32, true));
		} else if (type == SqlTypeName.VARCHAR) {
			return Field.nullable(column, new ArrowType.Utf8());
		} else if (type == SqlTypeName.FLOAT) {
			return Field.nullable(column, new ArrowType.FloatingPoint(FloatingPointPrecision.SINGLE));
		} else if (type == SqlTypeName.DOUBLE) {
			return Field.nullable(column, new ArrowType.FloatingPoint(FloatingPointPrecision.DOUBLE));
		} else {
			return Field.nullable(column, new ArrowType.Utf8());
		}
	}

	/**
	 * Get Vectors based on Column Type
	 * 
	 * @param column
	 * @param type
	 * @param allocator
	 * @return vector of given sql types
	 */
	protected static Object getVector(String column, SqlTypeName type, BufferAllocator allocator) {
		if (type == SqlTypeName.INTEGER) {
			IntVector vector = new IntVector(column, allocator);
			return vector;
		} else if (type == SqlTypeName.BIGINT) {
			IntVector vector = new IntVector(column, allocator);
			return vector;
		} else if (type == SqlTypeName.VARCHAR) {
			VarCharVector vector = new VarCharVector(column, allocator);
			return vector;
		} else if (type == SqlTypeName.FLOAT) {
			Float4Vector vector = new Float4Vector(column, allocator);
			return vector;
		} else if (type == SqlTypeName.DOUBLE) {
			Float8Vector vector = new Float8Vector(column, allocator);
			return vector;
		} else {
			VarCharVector vector = new VarCharVector(column, allocator);
			return vector;
		}
	}

	/**
	 * Sets Value in Arrow Vector for given value and index
	 * 
	 * @param index
	 * @param vector
	 * @param value
	 * @throws Exception
	 */
	protected static synchronized void setValue(int index, Object vector, Object value) throws Exception {
		if (vector instanceof IntVector iv) {
			iv.setSafe(index, (int) value);
		} else if (vector instanceof VarCharVector vcv) {
			Text text = new Text();
			text.set((String) value);
			vcv.setSafe(index, text);
		} else if (vector instanceof Float4Vector f4v) {
			f4v.setSafe(index, (float) value);
		} else if (vector instanceof Float8Vector f8v) {
			f8v.setSafe(index, (double) value);
		}
	}

	/**
	 * Allocates vector for capacity
	 * 
	 * @param vector
	 * @param capacity
	 */
	protected static void allocateNewCapacity(Object vector) {
		if (vector instanceof IntVector iv) {
			iv.allocateNew();
		} else if (vector instanceof VarCharVector vcv) {
			vcv.allocateNew();
		} else if (vector instanceof Float4Vector f4v) {
			f4v.allocateNew();
		} else if (vector instanceof Float8Vector f8v) {
			f8v.allocateNew();
		}
	}

	/**
	 * Gets value from vector for given index.
	 * 
	 * @param index
	 * @param vector
	 * @return object value
	 */
	public static Object getVectorValue(int index, Object vector) {
		try {
			if (vector instanceof IntVector iv) {
				return iv.get(index);
			} else if (vector instanceof VarCharVector vcv) {
				return new String(vcv.get(index));
			} else if (vector instanceof Float4Vector f4v) {
				return f4v.get(index);
			} else if (vector instanceof Float8Vector f8v) {
				return f8v.get(index);
			}
		} catch (Exception ex) {
			log.debug("Error in Index: {}", index);
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
		return DataSamudayaConstants.EMPTY;
	}

	/**
	 * Get all columns from expression
	 * 
	 * @param expression
	 * @param columns
	 */
	public static void getColumnsFromExpression(Expression expression, List<Column> columns) {
		if (expression instanceof BinaryExpression bex) {
			Expression leftExpression = bex.getLeftExpression();
			Expression rightExpression = bex.getRightExpression();
			if (leftExpression instanceof BinaryExpression binaryExpression) {
				getColumnsFromExpression(binaryExpression, columns);
			} else if (leftExpression instanceof Column column) {
				columns.add(column);
			}
			if (rightExpression instanceof BinaryExpression binaryExpression) {
				getColumnsFromExpression(binaryExpression, columns);
			} else if (rightExpression instanceof Column column) {
				columns.add(column);
			} else if (leftExpression instanceof Parenthesis parenthesis) {
				Expression subExpression = parenthesis.getExpression();
				getColumnsFromExpression(subExpression, columns);
			}
		} else if (expression instanceof Column column) {
			columns.add(column);
		} else if (expression instanceof Parenthesis parenthesis) {
			Expression subExpression = parenthesis.getExpression();
			getColumnsFromExpression(subExpression, columns);
		}
	}

	static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
	static SimpleDateFormat dateExtract = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	static SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd");
	static SimpleDateFormat time = new SimpleDateFormat("HH:mm:ss");

	public static Object evaluateFunctionsWithType(Object value, Object extraval, String name) {
		switch (name) {
			case "abs":

				if (value instanceof Double dv) {
					return Math.abs(dv);
				} else if (value instanceof Long lv) {
					return Math.abs(lv);
				} else if (value instanceof Float fv) {
					return Math.abs(fv);
				} else if (value instanceof Integer iv) {
					return Math.abs(iv);
				} else {
					return 0;
				}
			case "length":
				// Get the length of string value
				String val = (String) value;
				// return the result to the stack
				return Long.valueOf(val.length());
			case "trim":
				// Get the length of string value
				val = (String) value;
				// return the result to the stack
				return val.trim();
			case "round":

				if (value instanceof Double rdv) {
					return Math.round(rdv);
				} else if (value instanceof Long rlv) {
					return Math.round(rlv);
				} else if (value instanceof Float rfv) {
					return Math.round(rfv);
				} else if (value instanceof Integer riv) {
					return Math.round(riv);
				} else {
					return 0;
				}
			case "ceil":

				if (value instanceof Double cdv) {
					return Math.ceil(cdv);
				} else if (value instanceof Long clv) {
					return Math.ceil(clv);
				} else if (value instanceof Float cfv) {
					return Math.ceil(cfv);
				} else if (value instanceof Integer civ) {
					return Math.ceil(civ);
				} else {
					return 0;
				}
			case "floor":

				if (value instanceof Double fdv) {
					return Math.floor(fdv);
				} else if (value instanceof Long flv) {
					return Math.floor(flv);
				} else if (value instanceof Float ffv) {
					return Math.floor(ffv);
				} else if (value instanceof Integer fiv) {
					return Math.floor(fiv);
				} else {
					return 0;
				}
			case "pow":

				if (value instanceof Double pdv && extraval instanceof Integer powval) {
					return Math.pow(pdv, powval);
				} else if (value instanceof Long plv && extraval instanceof Integer powval) {
					return Math.pow(plv, powval);
				} else if (value instanceof Float pfv && extraval instanceof Integer powval) {
					return Math.pow(pfv, powval);
				} else if (value instanceof Integer piv && extraval instanceof Integer powval) {
					return Math.pow(piv, powval);
				} else if (value instanceof Double pdv && extraval instanceof Double powval) {
					return Math.pow(pdv, powval);
				} else if (value instanceof Long plv && extraval instanceof Double powval) {
					return Math.pow(plv, powval);
				} else if (value instanceof Float pfv && extraval instanceof Double powval) {
					return Math.pow(pfv, powval);
				} else if (value instanceof Integer piv && extraval instanceof Double powval) {
					return Math.pow(piv, powval);
				} else if (value instanceof Double pdv && extraval instanceof Float powval) {
					return Math.pow(pdv, powval);
				} else if (value instanceof Long plv && extraval instanceof Float powval) {
					return Math.pow(plv, powval);
				} else if (value instanceof Float pfv && extraval instanceof Float powval) {
					return Math.pow(pfv, powval);
				} else if (value instanceof Integer piv && extraval instanceof Float powval) {
					return Math.pow(piv, powval);
				} else if (value instanceof Double pdv && extraval instanceof Long powval) {
					return Math.pow(pdv, powval);
				} else if (value instanceof Long plv && extraval instanceof Long powval) {
					return Math.pow(plv, powval);
				} else if (value instanceof Float pfv && extraval instanceof Long powval) {
					return Math.pow(pfv, powval);
				} else if (value instanceof Integer piv && extraval instanceof Long powval) {
					return Math.pow(piv, powval);
				} else {
					return 0;
				}
			case "sqrt":

				if (value instanceof Double sdv) {
					return Math.sqrt(sdv);
				} else if (value instanceof Long slv) {
					return Math.sqrt(slv);
				} else if (value instanceof Float sfv) {
					return Math.sqrt(sfv);
				} else if (value instanceof Integer siv) {
					return Math.sqrt(siv);
				} else {
					return 0;
				}
			case "exp":

				if (value instanceof Double edv) {
					return Math.exp(edv);
				} else if (value instanceof Long elv) {
					return Math.exp(elv);
				} else if (value instanceof Float efv) {
					return Math.exp(efv);
				} else if (value instanceof Integer eiv) {
					return Math.exp(eiv);
				}
			case "loge":

				if (value instanceof Double ldv) {
					return Math.log(ldv);
				} else if (value instanceof Long llv) {
					return Math.log(llv);
				} else if (value instanceof Float lfv) {
					return Math.log(lfv);
				} else if (value instanceof Integer liv) {
					return Math.log(liv);
				} else {
					return 0;
				}
			case "lowercase":

				return ((String) value).toLowerCase();
			case "uppercase":

				return ((String) value).toUpperCase();
			case "base64encode":

				return Base64.getEncoder().encodeToString(((String) value).getBytes());
			case "base64decode":

				return new String(Base64.getDecoder().decode(((String) value).getBytes()));
			case "normalizespaces":

				return StringUtils.normalizeSpace((String) value);
			case "currentisodate":

				return dateFormat.format(new Date(System.currentTimeMillis()));
			case "currenttimemillis":

				return System.currentTimeMillis();
			case "rand":

				return new Random((long) value).nextDouble();
			case "randinteger":

				return new Random((long) value).nextInt((int) extraval);
			case "acos":

				return Math.acos(Double.valueOf(String.valueOf(value)));
			case "asin":

				return Math.asin(Double.valueOf(String.valueOf(value)));
			case "atan":

				return Math.atan(Double.valueOf(String.valueOf(value)));

			case "cos":

				return Math.cos(Double.valueOf(String.valueOf(value)));
			case "sin":

				return Math.sin(Double.valueOf(String.valueOf(value)));
			case "tan":

				return Math.tan(Double.valueOf(String.valueOf(value)));
			case "cosec":

				return 1.0 / Math.sin(Double.valueOf(String.valueOf(value)));
			case "sec":

				return 1.0 / Math.cos(Double.valueOf(String.valueOf(value)));
			case "cot":

				return 1.0 / Math.tan(Double.valueOf(String.valueOf(value)));
			case "cbrt":

				return Math.cbrt(Double.valueOf(String.valueOf(value)));
			case "pii":

				return Math.PI;
			case "radians":

				return Double.valueOf(String.valueOf(value)) / 180 * Math.PI;
			case "degrees":

				return Double.valueOf(String.valueOf(value)) / Math.PI * 180;
		}
		return name;
	}

	/**
	 * Evaluates tuple using operator
	 * 
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
				} else if (leftValue instanceof Double lv && rightValue instanceof Double rv) {
					return lv + rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof Double rv) {
					return lv + rv;
				} else if (leftValue instanceof Double lv && rightValue instanceof Long rv) {
					return lv + rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof Long rv) {
					return lv + rv;
				} else if (leftValue instanceof String lv && rightValue instanceof Long rv) {
					return lv + rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof String rv) {
					return lv + rv;
				} else if (leftValue instanceof Double lv && rightValue instanceof String rv) {
					return lv + rv;
				} else if (leftValue instanceof Integer lv && rightValue instanceof Integer rv) {
					return lv + rv;
				} else if (leftValue instanceof String lv && rightValue instanceof String rv) {
					return lv + rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof Integer rv) {
					return lv + rv;
				} else if (leftValue instanceof Integer lv && rightValue instanceof Long rv) {
					return lv + rv;
				}
			case "-":
				if (leftValue instanceof Double lv && rightValue instanceof Double rv) {
					return lv - rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof Double rv) {
					return lv - rv;
				} else if (leftValue instanceof Double lv && rightValue instanceof Long rv) {
					return lv - rv;
				} else if (leftValue instanceof Integer lv && rightValue instanceof Integer rv) {
					return lv - rv;
				} else if (leftValue instanceof Integer lv && rightValue instanceof Long rv) {
					return lv - rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof Integer rv) {
					return lv - rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof Long rv) {
					return lv - rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof Integer rv) {
					return lv - rv;
				} else if (leftValue instanceof Integer lv && rightValue instanceof Long rv) {
					return lv - rv;
				}
			case "*":
				if (leftValue instanceof Double lv && rightValue instanceof Double rv) {
					return lv * rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof Double rv) {
					return lv * rv;
				} else if (leftValue instanceof Double lv && rightValue instanceof Integer rv) {
					return lv * rv;
				} else if (leftValue instanceof Integer lv && rightValue instanceof Double rv) {
					return lv * rv;
				} else if (leftValue instanceof Double lv && rightValue instanceof Long rv) {
					return lv * rv;
				} else if (leftValue instanceof Integer lv && rightValue instanceof Integer rv) {
					return lv * rv;
				} else if (leftValue instanceof Integer lv && rightValue instanceof Long rv) {
					return lv * rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof Integer rv) {
					return lv * rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof Long rv) {
					return lv * rv;
				}
			case "/":
				if (leftValue instanceof Double lv && rightValue instanceof Double rv) {
					return lv / rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof Double rv) {
					return lv / rv;
				} else if (leftValue instanceof Double lv && rightValue instanceof Integer rv) {
					return lv / rv;
				} else if (leftValue instanceof Integer lv && rightValue instanceof Double rv) {
					return lv / rv;
				} else if (leftValue instanceof Double lv && rightValue instanceof Long rv) {
					return lv / rv;
				} else if (leftValue instanceof Integer lv && rightValue instanceof Integer rv) {
					return lv / (double) rv;
				} else if (leftValue instanceof Integer lv && rightValue instanceof Long rv) {
					return lv / (double) rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof Integer rv) {
					return lv / (double) rv;
				} else if (leftValue instanceof Long lv && rightValue instanceof Long rv) {
					return lv / (double) rv;
				}
			default:
				return 0;
		}

	}

	/**
	 * Get list of columns from list of functions.
	 * 
	 * @param functions
	 * @return list of columns
	 */
	public static List<Expression> getExpressions(Function function) {
		if (nonNull(function.getParameters())) {
			return function.getParameters().getExpressions();
		}
		return null;
	}

	public static Object evaluateValuesByFunctionMin(Object leftValue, Object rightValue) {
		if (leftValue instanceof Double lv && rightValue instanceof Double rv) {
			return Math.min(lv, rv);
		} else if (leftValue instanceof Long lv && rightValue instanceof Long rv) {
			return Math.min(lv, rv);
		} else if (leftValue instanceof Integer lv && rightValue instanceof Integer rv) {
			return Math.min(lv, rv);
		} else if (leftValue instanceof Float lv && rightValue instanceof Float rv) {
			return Math.min(lv, rv);
		} else {
			throw new IllegalArgumentException("Unknown type: " + leftValue + " " + rightValue);
		}
	}

	public static Object evaluateValuesByFunctionMax(Object leftValue, Object rightValue) {
		if (leftValue instanceof Double lv && rightValue instanceof Double rv) {
			return Math.max(lv, rv);
		} else if (leftValue instanceof Long lv && rightValue instanceof Long rv) {
			return Math.max(lv, rv);
		} else if (leftValue instanceof Integer lv && rightValue instanceof Integer rv) {
			return Math.max(lv, rv);
		} else if (leftValue instanceof Float lv && rightValue instanceof Float rv) {
			return Math.max(lv, rv);
		} else {
			throw new IllegalArgumentException("Unknown type: " + leftValue + " " + rightValue);
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
		if (expression instanceof LongValue value) {
			return value.getValue();
		} else if (expression instanceof StringValue value) {
			return value.getValue();
		} else if (expression instanceof DoubleValue value) {
			return Double.valueOf(value.getValue());
		} else if (expression instanceof Addition addition) {
			return evaluateValuesByOperator(getValueString(addition.getLeftExpression(), row),
					getValueString(addition.getRightExpression(), row), "+");
		} else if (expression instanceof Subtraction subtraction) {
			return evaluateValuesByOperator(getValueString(subtraction.getLeftExpression(), row),
					getValueString(subtraction.getRightExpression(), row), "-");
		} else if (expression instanceof Multiplication multiplication) {
			return evaluateValuesByOperator(getValueString(multiplication.getLeftExpression(), row),
					getValueString(multiplication.getRightExpression(), row), "*");
		} else if (expression instanceof Division division) {
			return evaluateValuesByOperator(getValueString(division.getLeftExpression(), row),
					getValueString(division.getRightExpression(), row), "/");
		} else {
			Column column = (Column) expression;
			String columnName = column.getColumnName();
			Object value = row.get(columnName);
			if (value instanceof String stringval) {
				return String.valueOf(stringval);
			} else if (value instanceof Double doubleval) {
				return doubleval;
			} else if (value instanceof Integer intval) {
				return intval;
			} else if (value instanceof Long longval) {
				return longval;
			} else if (value instanceof String stringval && NumberUtils.isParsable(stringval)) {
				return Double.valueOf(stringval);
			}
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
	public static boolean evaluateExpression(Expression expression, Map<String, Object> row) {
		if (expression instanceof Between betweenExpression) {
			Expression leftExpression = betweenExpression.getLeftExpression();
			Expression startExpression = betweenExpression.getBetweenExpressionStart();
			Expression endExpression = betweenExpression.getBetweenExpressionEnd();
			Object leftValue = getValueString(leftExpression, row);
			Object startExpressionValue = getValueString(startExpression, row);
			Object endExpressionValue = getValueString(endExpression, row);
			if (evaluatePredicate(leftValue, startExpressionValue, ">")
					&& evaluatePredicate(leftValue, endExpressionValue, "<")) {
				return true;
			}
			return false;
		} else if (expression instanceof InExpression inExpression) {
			Expression leftExpression = inExpression.getLeftExpression();
			ItemsList itemsList = inExpression.getRightItemsList();

			// Handle the left expression
			Object leftValue = getValueString(leftExpression, row);

			// Handle the IN clause values
			if (itemsList instanceof ExpressionList expressionList) {
				List<Expression> inValues = expressionList.getExpressions();

				// Process each value in the IN clause
				for (Expression expressioninlist : inValues) {
					Object rightValue = getValueString(expressioninlist, row);
					if (evaluatePredicate(leftValue, rightValue, "=")) {
						return true;
					}
				}
			}
			return false;

		} else if (expression instanceof LikeExpression likeExpression) {
			Expression leftExpression = likeExpression.getLeftExpression();
			Expression rightExpression = likeExpression.getRightExpression();

			// Handle the left expression
			String leftValue = (String) getValueString(leftExpression, row);

			// Handle the right expression (LIKE pattern)
			String rightValue = (String) getValueString(rightExpression, row);

			return leftValue.contains(rightValue);

		} else if (expression instanceof BinaryExpression binaryExpression) {
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
		} else if (expression instanceof Parenthesis parenthesis) {
			Expression subExpression = parenthesis.getExpression();
			return evaluateExpression(subExpression, row);
		} else {
			Object value = getValueString(expression, row);
			return Boolean.parseBoolean((String) value);
		}
	}

	public static boolean evaluatePredicate(Object leftvalue, Object rightvalue, String operator) {
		try {
			switch (operator.trim()) {
				case ">":
					return compare(leftvalue, rightvalue, ComparisonOperator.GREATER_THAN);
				case ">=":
					return compare(leftvalue, rightvalue, ComparisonOperator.GREATER_THAN_OR_EQUAL);
				case "<":
					return compare(leftvalue, rightvalue, ComparisonOperator.LESS_THAN);
				case "<=":
					return compare(leftvalue, rightvalue, ComparisonOperator.LESS_THAN_OR_EQUAL);
				case "=":
					return compare(leftvalue, rightvalue, ComparisonOperator.EQUAL);
				case "<>":
					return compare(leftvalue, rightvalue, ComparisonOperator.NOT_EQUAL);
				default:
					throw new UnsupportedOperationException("Unsupported operator: " + operator);
			}
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
		return false;
	}

	/**
	 * Performs the string comparison and returns true or false
	 * 
	 * @param str1
	 * @param str2
	 * @param operator
	 * @return true or false
	 */
	private static boolean performStringComparison(String str1, String str2, ComparisonOperator operator) {
		int comparisonResult = str1.compareTo(str2);

		switch (operator) {
			case EQUAL:
				return comparisonResult == 0;
			case NOT_EQUAL:
				return comparisonResult != 0;
			case GREATER_THAN:
				return comparisonResult > 0;
			case LESS_THAN:
				return comparisonResult < 0;
			case GREATER_THAN_OR_EQUAL:
				return comparisonResult >= 0;
			case LESS_THAN_OR_EQUAL:
				return comparisonResult <= 0;
			default:
				throw new IllegalArgumentException("Unsupported comparison operator.");
		}
	}

	/**
	 * Compare two values using reflection
	 * 
	 * @param obj1
	 * @param obj2
	 * @param operator
	 * @return true if two objects statisfy the condition else false
	 * @throws NoSuchFieldException
	 * @throws IllegalAccessException
	 */
	public static boolean compare(Object obj1, Object obj2, ComparisonOperator operator)
			throws NoSuchFieldException, IllegalAccessException {
		// Get the Class objects for the wrapper classes
		Class<?> wrapperClass1 = obj1.getClass();
		Class<?> wrapperClass2 = obj2.getClass();

		if (wrapperClass1 == String.class || wrapperClass2 == String.class) {
			String str1 = String.valueOf(obj1);
			String str2 = String.valueOf(obj2);
			return performStringComparison(str1, str2, operator);
		}

		// Ensure that both objects are comparable
		if (!(obj1 instanceof Comparable) || !(obj2 instanceof Comparable)) {
			throw new IllegalArgumentException("Both objects must implement the Comparable interface.");
		}
		// Ensure that both objects are instances of wrapper classes
		if (!isWrapperClass(wrapperClass1) || !isWrapperClass(wrapperClass2)) {
			throw new IllegalArgumentException("Both objects must be instances of wrapper classes.");
		}

		// Perform the specified comparison
		int comparisonResult = compareNumericValues((Number) obj1, (Number) obj2);

		switch (operator) {
			case EQUAL:
				return comparisonResult == 0;
			case NOT_EQUAL:
				return comparisonResult != 0;
			case GREATER_THAN:
				return comparisonResult > 0;
			case LESS_THAN:
				return comparisonResult < 0;
			case GREATER_THAN_OR_EQUAL:
				return comparisonResult >= 0;
			case LESS_THAN_OR_EQUAL:
				return comparisonResult <= 0;
			default:
				throw new IllegalArgumentException("Unsupported comparison operator.");
		}
	}

	/**
	 * Compares two primitive wrapper classes.
	 * 
	 * @param num1
	 * @param num2
	 * @return comparison value
	 */
	private static int compareNumericValues(Number num1, Number num2) {
		double value1 = num1.doubleValue();
		double value2 = num2.doubleValue();

		return Double.compare(value1, value2);
	}

	/**
	 * The function checks for the wrapper class
	 * 
	 * @param clazz
	 * @return true if wrapper class
	 */
	private static boolean isWrapperClass(Class<?> clazz) {
		return clazz == Integer.class || clazz == Double.class || clazz == Float.class || clazz == Long.class
				|| clazz == Short.class || clazz == Byte.class || clazz == Character.class || clazz == Boolean.class;
	}

	private enum ComparisonOperator {
		EQUAL, NOT_EQUAL, GREATER_THAN, LESS_THAN, GREATER_THAN_OR_EQUAL, LESS_THAN_OR_EQUAL
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
	public static boolean evaluateExpression(Expression expression, Map<String, Object> row,
			List<String> columnsforeachjoin) {
		if (expression instanceof Between betweenExpression) {
			Expression leftExpression = betweenExpression.getLeftExpression();
			Expression startExpression = betweenExpression.getBetweenExpressionStart();
			Expression endExpression = betweenExpression.getBetweenExpressionEnd();
			Object leftValue = getValueString(leftExpression, row, columnsforeachjoin);
			Object startExpressionValue = getValueString(startExpression, row, columnsforeachjoin);
			Object endExpressionValue = getValueString(endExpression, row, columnsforeachjoin);
			if (evaluatePredicate(leftValue, startExpressionValue, ">")
					&& evaluatePredicate(leftValue, endExpressionValue, "<")) {
				return true;
			}
			return false;
		} else if (expression instanceof InExpression inExpression) {
			Expression leftExpression = inExpression.getLeftExpression();
			ItemsList itemsList = inExpression.getRightItemsList();

			// Handle the left expression
			Object leftValue = getValueString(leftExpression, row, columnsforeachjoin);

			// Handle the IN clause values
			if (itemsList instanceof ExpressionList expressionList) {
				List<Expression> inValues = expressionList.getExpressions();

				// Process each value in the IN clause
				for (Expression expressioninlist : inValues) {
					Object rightValue = getValueString(expressioninlist, row, columnsforeachjoin);
					if (evaluatePredicate(leftValue, rightValue, "=")) {
						return true;
					}
				}
			}
			return false;

		} else if (expression instanceof LikeExpression likeExpression) {
			Expression leftExpression = likeExpression.getLeftExpression();
			Expression rightExpression = likeExpression.getRightExpression();

			// Handle the left expression
			String leftValue = (String) getValueString(leftExpression, row, columnsforeachjoin);

			// Handle the right expression (LIKE pattern)
			String rightValue = (String) getValueString(rightExpression, row, columnsforeachjoin);

			return leftValue.contains(rightValue);

		} else if (expression instanceof BinaryExpression binaryExpression) {
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
		} else if (expression instanceof Parenthesis parenthesis) {
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
	public static boolean evaluateExpressionJoin(Expression expression, String jointable, Map<String, Object> row1,
			Map<String, Object> row2, List<String> leftablecolumns, List<String> righttablecolumns) {
		if (expression instanceof BinaryExpression binaryExpression) {
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
					return compareTo(leftValueO, rightValueO) == 0;
				case "<>":
					leftValueO = getValueString(leftExpression, rowleft, columnsrowleft);
					leftValueO = getValueString(rightExpression, rowright, righttablecolumns);
					return compareTo(leftValueO, leftValueO) != 0;
				default:
					throw new UnsupportedOperationException("Unsupported operator: " + operator);
			}
		} else if (expression instanceof Parenthesis parenthesis) {
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
			Map<String, Set<String>> tablerequiredcolumns, List<String> columnsselect, List<String> functioncols) {
		if (expression instanceof BinaryExpression binaryExpression) {
			Expression leftExpression = binaryExpression.getLeftExpression();
			Expression rightExpression = binaryExpression.getRightExpression();
			getColumnsFromSelectItemsFunctions(leftExpression, tablerequiredcolumns, columnsselect, functioncols);
			getColumnsFromSelectItemsFunctions(rightExpression, tablerequiredcolumns, columnsselect, functioncols);
		}
		if (expression instanceof Function function) {
			if (nonNull(function.getParameters())) {
				List<Expression> expressions = function.getParameters().getExpressions();
				for (Expression exp : expressions) {
					getColumnsFromSelectItemsFunctions(exp, tablerequiredcolumns, columnsselect, functioncols);
				}
			}
		} else if (expression instanceof Parenthesis parenthesis) {
			Expression subExpression = parenthesis.getExpression();
			getColumnsFromSelectItemsFunctions(subExpression, tablerequiredcolumns, columnsselect, functioncols);
		} else if (expression instanceof Column column && nonNull(column.getTable())) {
			Set<String> requiredcolumns = tablerequiredcolumns.get(column.getTable().getName());
			if (isNull(requiredcolumns)) {
				requiredcolumns = new LinkedHashSet<>();
				tablerequiredcolumns.put(column.getTable().getName(), requiredcolumns);
			}
			requiredcolumns.add(column.getColumnName());
			columnsselect.add(column.getColumnName());
			functioncols.add(column.getColumnName());
		} else if (expression instanceof Column column) {
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
	private static Object getValueString(Expression expression, Map<String, Object> row, List<String> columns) {
		if (expression instanceof LongValue value) {
			return Long.valueOf(value.getValue());
		} else if (expression instanceof StringValue value) {
			return value.getValue();
		} else if (expression instanceof DoubleValue value) {
			return Double.valueOf(value.getValue());
		} else if (expression instanceof Addition addition) {
			return evaluateValuesByOperator(getValueString(addition.getLeftExpression(), row),
					getValueString(addition.getRightExpression(), row), "+");
		} else if (expression instanceof Subtraction subtraction) {
			return evaluateValuesByOperator(getValueString(subtraction.getLeftExpression(), row),
					getValueString(subtraction.getRightExpression(), row), "-");
		} else if (expression instanceof Multiplication multiplication) {
			return evaluateValuesByOperator(getValueString(multiplication.getLeftExpression(), row),
					getValueString(multiplication.getRightExpression(), row), "*");
		} else if (expression instanceof Division division) {
			return evaluateValuesByOperator(getValueString(division.getLeftExpression(), row),
					getValueString(division.getRightExpression(), row), "/");
		} else {
			Column column = (Column) expression;
			String columnName = column.getColumnName();
			Object value = row.get(columnName);
			if (value instanceof String stringval) {
				return String.valueOf(stringval);
			} else if (value instanceof Double doubleval) {
				return doubleval;
			} else if (value instanceof Integer intval) {
				return intval;
			} else if (value instanceof Long longval) {
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
	public static void getRequiredColumnsForAllTables(PlainSelect plainSelect,
			Map<String, Set<String>> tablerequiredcolumns, Map<String, List<Expression>> expressionsTable,
			Map<String, List<Expression>> joinTableExpressions) {

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
	public static void getColumnsFromBinaryExpression(Expression expression,
			Map<String, Set<String>> tablerequiredcolumns, Map<String, List<Expression>> expressions,
			Map<String, List<Expression>> joinTableExpressions) {
		if (expression instanceof Parenthesis parenthesis) {
			Expression subExpression = parenthesis.getExpression();
			getColumnsFromBinaryExpression(subExpression, tablerequiredcolumns, expressions, joinTableExpressions);
		} else if (expression instanceof Between betweenExpression) {
			Expression leftExpression = betweenExpression.getLeftExpression();
			Expression startExpression = betweenExpression.getBetweenExpressionStart();
			Expression endExpression = betweenExpression.getBetweenExpressionEnd();
			getColumnsFromBinaryExpression(leftExpression, tablerequiredcolumns, expressions, joinTableExpressions);
			getColumnsFromBinaryExpression(startExpression, tablerequiredcolumns, expressions, joinTableExpressions);
			getColumnsFromBinaryExpression(endExpression, tablerequiredcolumns, expressions, joinTableExpressions);
		} else if (expression instanceof InExpression inExpression) {
			Expression leftExpression = inExpression.getLeftExpression();
			ItemsList itemsList = inExpression.getRightItemsList();

			// Handle the left expression
			getColumnsFromBinaryExpression(leftExpression, tablerequiredcolumns, expressions, joinTableExpressions);

			// Handle the IN clause values
			if (itemsList instanceof ExpressionList expressionList) {
				List<Expression> inValues = expressionList.getExpressions();

				// Process each value in the IN clause
				for (Expression value : inValues) {
					getColumnsFromBinaryExpression(value, tablerequiredcolumns, expressions, joinTableExpressions);
				}
			}

		} else if (expression instanceof LikeExpression likeExpression) {
			Expression leftExpression = likeExpression.getLeftExpression();
			Expression rightExpression = likeExpression.getRightExpression();

			// Handle the left expression
			getColumnsFromBinaryExpression(leftExpression, tablerequiredcolumns, expressions, joinTableExpressions);

			// Handle the right expression (LIKE pattern)
			getColumnsFromBinaryExpression(rightExpression, tablerequiredcolumns, expressions, joinTableExpressions);

		} else if (expression instanceof BinaryExpression binaryExpression) {

			String operator = binaryExpression.getStringExpression();

			Expression leftExpression = binaryExpression.getLeftExpression();

			getColumnsFromBinaryExpression(leftExpression, tablerequiredcolumns, expressions, joinTableExpressions);

			Expression rightExpression = binaryExpression.getRightExpression();

			getColumnsFromBinaryExpression(rightExpression, tablerequiredcolumns, expressions, joinTableExpressions);

			if (leftExpression instanceof Column column1 && !(rightExpression instanceof Column)
					&& ("=".equals(operator) || "<".equals(operator) || "<=".equals(operator) || ">".equals(operator)
					|| ">=".equals(operator) || "<>".equals(operator))) {
				if (nonNull(column1.getTable())) {
					List<Expression> expressionsTable = expressions.get(column1.getTable().getName());
					if (isNull(expressionsTable)) {
						expressionsTable = new Vector<>();
						expressions.put(column1.getTable().getName(), expressionsTable);
					}
					expressionsTable.add(binaryExpression);
				}
			} else if (!(leftExpression instanceof Column) && rightExpression instanceof Column column1
					&& ("=".equals(operator) || "<".equals(operator) || "<=".equals(operator) || ">".equals(operator)
					|| ">=".equals(operator) || "<>".equals(operator))) {
				if (nonNull(column1.getTable())) {
					List<Expression> expressionsTable = expressions.get(column1.getTable().getName());
					if (isNull(expressionsTable)) {
						expressionsTable = new Vector<>();
						expressions.put(column1.getTable().getName(), expressionsTable);
					}
					expressionsTable.add(binaryExpression);
				}
			} else if (leftExpression instanceof Column col1 && rightExpression instanceof Column col2
					&& ("=".equals(operator) || "<".equals(operator) || "<=".equals(operator) || ">".equals(operator)
					|| ">=".equals(operator) || "<>".equals(operator))) {
				List<Expression> expressionsTable = joinTableExpressions
						.get(col1.getTable().getName() + "-" + col2.getTable().getName());
				if (isNull(expressionsTable)) {
					expressionsTable = new Vector<>();
					joinTableExpressions.put(col1.getTable().getName() + "-" + col2.getTable().getName(),
							expressionsTable);
				}
				expressionsTable.add(binaryExpression);
			} else if (leftExpression instanceof BinaryExpression expr && !(rightExpression instanceof Column)
					&& ("=".equals(operator) || "<".equals(operator) || "<=".equals(operator) || ">".equals(operator)
					|| ">=".equals(operator) || "<>".equals(operator))) {
				List<Column> columnsFromExpression = new ArrayList<>();
				getColumnsFromExpression(expr, columnsFromExpression);
				for (Column column1 : columnsFromExpression) {
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
					&& ("=".equals(operator) || "<".equals(operator) || "<=".equals(operator) || ">".equals(operator)
					|| ">=".equals(operator) || "<>".equals(operator))) {
				List<Column> columnsFromExpression = new ArrayList<>();
				getColumnsFromExpression(expr, columnsFromExpression);
				for (Column column1 : columnsFromExpression) {
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
	 * 
	 * @param gbe
	 * @param tablerequiredcolumns
	 */
	public static void getColumnsFromGroupByExpression(GroupByElement gbe,
			Map<String, Set<String>> tablerequiredcolumns) {
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
			for (int expressioncount = 1;expressioncount < leftTableExpressions.size();expressioncount++) {
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
		for (int columnindex = 0;columnindex < columns.size();columnindex++) {
			columnindexmap.put(columns.get(columnindex), (int) columnindex);
		}
		return columnindexmap;
	}

	/**
	 * Converts Object array to Tuple
	 * 
	 * @param obj
	 * @return Tuple
	 */
	public static Tuple convertObjectToTuple(Object[] obj) {
		if (isNull(obj) || obj.length == 0) {
			return Tuple.tuple(DataSamudayaConstants.EMPTY);
		} else if (obj.length == 1) {
			return Tuple.tuple(obj[0]);
		} else if (obj.length == 2) {
			return Tuple.tuple(obj[0], obj[1]);
		} else if (obj.length == 3) {
			return Tuple.tuple(obj[0], obj[1], obj[2]);
		} else if (obj.length == 4) {
			return Tuple.tuple(obj[0], obj[1], obj[2], obj[3]);
		} else if (obj.length == 5) {
			return Tuple.tuple(obj[0], obj[1], obj[2], obj[3], obj[4]);
		} else if (obj.length == 6) {
			return Tuple.tuple(obj[0], obj[1], obj[2], obj[3], obj[4], obj[5]);
		} else if (obj.length == 7) {
			return Tuple.tuple(obj[0], obj[1], obj[2], obj[3], obj[4], obj[5], obj[6]);
		} else if (obj.length == 8) {
			return Tuple.tuple(obj[0], obj[1], obj[2], obj[3], obj[4], obj[5], obj[6], obj[7]);
		} else if (obj.length == 9) {
			return Tuple.tuple(obj[0], obj[1], obj[2], obj[3], obj[4], obj[5], obj[6], obj[7], obj[8]);
		} else if (obj.length == 10) {
			return Tuple.tuple(obj[0], obj[1], obj[2], obj[3], obj[4], obj[5], obj[6], obj[7], obj[8], obj[9]);
		} else if (obj.length == 11) {
			return Tuple.tuple(obj[0], obj[1], obj[2], obj[3], obj[4], obj[5], obj[6], obj[7], obj[8], obj[9], obj[10]);
		} else if (obj.length == 12) {
			return Tuple.tuple(obj[0], obj[1], obj[2], obj[3], obj[4], obj[5], obj[6], obj[7], obj[8], obj[9], obj[10],
					obj[11]);
		} else if (obj.length == 13) {
			return Tuple.tuple(obj[0], obj[1], obj[2], obj[3], obj[4], obj[5], obj[6], obj[7], obj[8], obj[9], obj[10],
					obj[11], obj[12]);
		} else if (obj.length == 14) {
			return Tuple.tuple(obj[0], obj[1], obj[2], obj[3], obj[4], obj[5], obj[6], obj[7], obj[8], obj[9], obj[10],
					obj[11], obj[12], obj[13]);
		} else if (obj.length == 15) {
			return Tuple.tuple(obj[0], obj[1], obj[2], obj[3], obj[4], obj[5], obj[6], obj[7], obj[8], obj[9], obj[10],
					obj[11], obj[12], obj[13], obj[14]);
		} else if (obj.length == 16) {
			return Tuple.tuple(obj[0], obj[1], obj[2], obj[3], obj[4], obj[5], obj[6], obj[7], obj[8], obj[9], obj[10],
					obj[11], obj[12], obj[13], obj[14], obj[15]);
		} else {
			throw new UnsupportedOperationException("Max supported Column is 16");
		}
	}

	/**
	 * Populates map from tuple for
	 * 
	 * @param mapvalues
	 * @param tuple
	 * @param groupby
	 */
	public static void populateMapFromTuple(Map<String, Object> mapvalues, Tuple tuple, List<String> groupby) {

		if (groupby.isEmpty()) {
			return;
		} else if (tuple instanceof Tuple1 tup) {
			mapvalues.put(groupby.get(0), tup.v1());
		} else if (tuple instanceof Tuple2 tup) {
			mapvalues.put(groupby.get(0), tup.v1());
			mapvalues.put(groupby.get(1), tup.v2());
		} else if (tuple instanceof Tuple3 tup) {
			mapvalues.put(groupby.get(0), tup.v1());
			mapvalues.put(groupby.get(1), tup.v2());
			mapvalues.put(groupby.get(2), tup.v3());
		} else if (tuple instanceof Tuple4 tup) {
			mapvalues.put(groupby.get(0), tup.v1());
			mapvalues.put(groupby.get(1), tup.v2());
			mapvalues.put(groupby.get(2), tup.v3());
			mapvalues.put(groupby.get(3), tup.v4());
		} else if (tuple instanceof Tuple5 tup) {
			mapvalues.put(groupby.get(0), tup.v1());
			mapvalues.put(groupby.get(1), tup.v2());
			mapvalues.put(groupby.get(2), tup.v3());
			mapvalues.put(groupby.get(3), tup.v4());
			mapvalues.put(groupby.get(4), tup.v5());
		} else if (tuple instanceof Tuple6 tup) {
			mapvalues.put(groupby.get(0), tup.v1());
			mapvalues.put(groupby.get(1), tup.v2());
			mapvalues.put(groupby.get(2), tup.v3());
			mapvalues.put(groupby.get(3), tup.v4());
			mapvalues.put(groupby.get(4), tup.v5());
			mapvalues.put(groupby.get(5), tup.v6());
		} else if (tuple instanceof Tuple7 tup) {
			mapvalues.put(groupby.get(0), tup.v1());
			mapvalues.put(groupby.get(1), tup.v2());
			mapvalues.put(groupby.get(2), tup.v3());
			mapvalues.put(groupby.get(3), tup.v4());
			mapvalues.put(groupby.get(4), tup.v5());
			mapvalues.put(groupby.get(5), tup.v6());
			mapvalues.put(groupby.get(6), tup.v7());
		} else if (tuple instanceof Tuple8 tup) {
			mapvalues.put(groupby.get(0), tup.v1());
			mapvalues.put(groupby.get(1), tup.v2());
			mapvalues.put(groupby.get(2), tup.v3());
			mapvalues.put(groupby.get(3), tup.v4());
			mapvalues.put(groupby.get(4), tup.v5());
			mapvalues.put(groupby.get(5), tup.v6());
			mapvalues.put(groupby.get(6), tup.v7());
			mapvalues.put(groupby.get(7), tup.v8());
		} else if (tuple instanceof Tuple9 tup) {
			mapvalues.put(groupby.get(0), tup.v1());
			mapvalues.put(groupby.get(1), tup.v2());
			mapvalues.put(groupby.get(2), tup.v3());
			mapvalues.put(groupby.get(3), tup.v4());
			mapvalues.put(groupby.get(4), tup.v5());
			mapvalues.put(groupby.get(5), tup.v6());
			mapvalues.put(groupby.get(6), tup.v7());
			mapvalues.put(groupby.get(7), tup.v8());
			mapvalues.put(groupby.get(8), tup.v9());
		} else if (tuple instanceof Tuple10 tup) {
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
		} else if (tuple instanceof Tuple11 tup) {
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
		} else if (tuple instanceof Tuple12 tup) {
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
		} else if (tuple instanceof Tuple13 tup) {
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
		} else if (tuple instanceof Tuple14 tup) {
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
		} else if (tuple instanceof Tuple15 tup) {
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
		} else if (tuple instanceof Tuple16 tup) {
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
		} else {
			throw new UnsupportedOperationException("Max supported Column is 16");
		}
	}

	public static Long getCountFromTuple(Tuple tuple) {
		if (tuple instanceof Tuple1 tup) {
			return (Long) tup.v1;
		} else if (tuple instanceof Tuple2 tup) {
			return (Long) tup.v2;
		} else if (tuple instanceof Tuple3 tup) {
			return (Long) tup.v3;
		} else if (tuple instanceof Tuple4 tup) {
			return (Long) tup.v4;
		} else if (tuple instanceof Tuple5 tup) {
			return (Long) tup.v5;
		} else if (tuple instanceof Tuple6 tup) {
			return (Long) tup.v6;
		} else if (tuple instanceof Tuple7 tup) {
			return (Long) tup.v7;
		} else if (tuple instanceof Tuple8 tup) {
			return (Long) tup.v8;
		} else if (tuple instanceof Tuple9 tup) {
			return (Long) tup.v9;
		} else if (tuple instanceof Tuple10 tup) {
			return (Long) tup.v10;
		} else if (tuple instanceof Tuple11 tup) {
			return (Long) tup.v11;
		} else if (tuple instanceof Tuple12 tup) {
			return (Long) tup.v12;
		} else if (tuple instanceof Tuple13 tup) {
			return (Long) tup.v13;
		} else if (tuple instanceof Tuple14 tup) {
			return (Long) tup.v14;
		} else if (tuple instanceof Tuple15 tup) {
			return (Long) tup.v15;
		} else if (tuple instanceof Tuple16 tup) {
			return (Long) tup.v16;
		} else {
			throw new UnsupportedOperationException("Max supported Column is 16");
		}
	}

	/**
	 * Populate Map values from function.
	 * 
	 * @param mapvalues
	 * @param tuple
	 * @param functions
	 * @param functionalias
	 * @throws Exception
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 */
	public static void populateMapFromFunctions(Map<String, Object> mapvalues, Tuple tuple, List<Function> functions,
			Map<Function, String> functionalias) {
		try {
			Class<?> cls = tuple.getClass();
			for (int funcindex = 0, valueindex = 1;funcindex < functions.size();funcindex++) {
				Function func = functions.get(funcindex);
				String funname = func.getName();
				if (funname.toLowerCase().startsWith("avg")) {
					java.lang.reflect.Method method = cls.getMethod("v" + valueindex);
					Object valuesum = method.invoke(tuple);
					valueindex++;
					method = cls.getMethod("v" + valueindex);
					Object valuecount = method.invoke(tuple);
					mapvalues.put(getAliasForFunction(func, functionalias),
							evaluateValuesByOperator(valuesum, valuecount, "/"));
				} else {
					java.lang.reflect.Method method = cls.getMethod("v" + valueindex);
					Object value = method.invoke(tuple);
					mapvalues.put(getAliasForFunction(func, functionalias), value);
				}
				valueindex++;
			}
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
	}

	/**
	 * This method returns function alias for a given function from map
	 * 
	 * @param function
	 * @param functionalias
	 * @return function alias
	 */
	public static String getAliasForFunction(Function function, Map<Function, String> functionalias) {
		String aliasfunction = functionalias.get(function);
		String alias = nonNull(aliasfunction) ? aliasfunction : function.toString();
		return alias;
	}

	/**
	 * Evaluate Tuple for functions
	 * 
	 * @param tuple1
	 * @param tuple2
	 * @param aggfunctions
	 * @return Tuple
	 * @throws Exception
	 * @throws InvocationTargetException
	 * @throws IllegalArgumentException
	 */
	public static Tuple evaluateTuple(Tuple tuple1, Tuple tuple2, List<Function> aggfunctions) {
		try {
			// Get the class of the Tuple
			Class<?> tupleClass = tuple1.getClass();
			// Create a new instance of the Tuple
			Tuple result = (Tuple) tupleClass.getConstructor(tuple2.getClass()).newInstance(tuple1);
			// Get all the fields of the Tuple class
			java.lang.reflect.Field[] fields = tuple1.getClass().getFields();
			int index = 0;
			boolean avgindex = false;
			Function func = aggfunctions.get(index);
			// Iterate over the fields and perform summation
			for (java.lang.reflect.Field field : fields) {
				// Make the field accessible, as it might be private
				field.setAccessible(true);

				// Get the values of the fields from both tuples
				Object value1 = field.get(tuple1);
				Object value2 = field.get(tuple2);
				field.set(result, evaluateFunction(value1, value2, func));
				// Set the sum of the values in the result tuple
				if (index + 1 < aggfunctions.size()) {
					if (avgindex) {
						func = aggfunctions.get(index);
						avgindex = false;
						index++;
					} else if (!"avg".equalsIgnoreCase(aggfunctions.get(index).getName())) {
						func = aggfunctions.get(index + 1);
						index++;
						avgindex = false;
					} else {
						func = aggfunctions.get(index);
						avgindex = true;
					}
				}
			}

			return result;
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
		return null;
	}

	/**
	 * Evaluate values based on functions
	 * 
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
	 * 
	 * @param leftValue
	 * @param rightValue
	 * @param appendstring
	 * @return concatenated string
	 */
	public static Object evaluateGroupConcat(Object leftValue, Object rightValue, String appendstring) {
		if (leftValue instanceof String lv && rightValue instanceof Double rv) {
			return lv + appendstring + rv;
		} else if (leftValue instanceof Double lv && rightValue instanceof Double rv) {
			return lv + appendstring + rv;
		} else if (leftValue instanceof Long lv && rightValue instanceof Double rv) {
			return lv + appendstring + rv;
		} else if (leftValue instanceof Double lv && rightValue instanceof Long rv) {
			return lv + appendstring + rv;
		} else if (leftValue instanceof Long lv && rightValue instanceof Long rv) {
			return lv + appendstring + rv;
		} else if (leftValue instanceof String lv && rightValue instanceof Long rv) {
			return lv + appendstring + rv;
		} else if (leftValue instanceof Long lv && rightValue instanceof String rv) {
			return lv + appendstring + rv;
		} else if (leftValue instanceof Double lv && rightValue instanceof String rv) {
			return lv + appendstring + rv;
		} else if (leftValue instanceof Integer lv && rightValue instanceof Integer rv) {
			return lv + appendstring + rv;
		} else if (leftValue instanceof String lv && rightValue instanceof String rv) {
			return lv + appendstring + rv;
		} else if (leftValue instanceof Long lv && rightValue instanceof Integer rv) {
			return lv + appendstring + rv;
		} else if (leftValue instanceof Integer lv && rightValue instanceof Long rv) {
			return lv + appendstring + rv;
		}
		return leftValue + appendstring + rightValue;
	}

	/**
	 * Validates and optimizes sql query
	 * 
	 * @param tablecolumnsmap
	 * @param tablecolumntypesmap
	 * @param sql
	 * @param db
	 * @return
	 * @return optimized query
	 * @throws Exception
	 */
	public static RelNode validateSql(ConcurrentMap<String, List<String>> tablecolumnsmap,
			ConcurrentMap<String, List<SqlTypeName>> tablecolumntypesmap, String sql, String db,
			AtomicBoolean isDistinct) throws Exception {
		Set<String> tablesfromconfig = tablecolumnsmap.keySet();
		SimpleSchema.Builder builder = SimpleSchema.newBuilder(db);
		for (String table : tablesfromconfig) {
			List<String> columns = tablecolumnsmap.get(table);
			List<SqlTypeName> sqltypes = tablecolumntypesmap.get(table);
			builder.addTable(getSimpleTable(table, columns.toArray(new String[columns.size()]),
					sqltypes.toArray(new SqlTypeName[sqltypes.size()])));
		}
		SimpleSchema schema = builder.build();
		Optimizer optimizer = Optimizer.create(schema);
		SqlNode sqlTree = optimizer.parse(sql);
		sqlTree = optimizer.validate(sqlTree);
		if (sqlTree.getKind() == SqlKind.SELECT) {
			SqlSelect selectNode = (SqlSelect) sqlTree;
			isDistinct.set(selectNode.isDistinct());
		}
		RelNode relTree = optimizer.convert(sqlTree);
		RuleSet rules = RuleSets.ofList(CoreRules.FILTER_TO_CALC, CoreRules.PROJECT_TO_CALC, CoreRules.FILTER_MERGE,
				CoreRules.FILTER_CALC_MERGE, CoreRules.PROJECT_CALC_MERGE,
				CoreRules.AGGREGATE_JOIN_TRANSPOSE,
				CoreRules.PROJECT_MERGE,
				CoreRules.FILTER_INTO_JOIN,
				CoreRules.AGGREGATE_JOIN_TRANSPOSE_EXTENDED,
				CoreRules.AGGREGATE_PROJECT_MERGE,
				CoreRules.PROJECT_FILTER_VALUES_MERGE,
				CoreRules.PROJECT_FILTER_TRANSPOSE,
				CoreRules.JOIN_PROJECT_BOTH_TRANSPOSE,
				EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
				EnumerableRules.ENUMERABLE_PROJECT_RULE, EnumerableRules.ENUMERABLE_FILTER_RULE,
				EnumerableRules.ENUMERABLE_AGGREGATE_RULE, EnumerableRules.ENUMERABLE_SORT_RULE,
				EnumerableRules.ENUMERABLE_SORTED_AGGREGATE_RULE, EnumerableRules.ENUMERABLE_JOIN_RULE,
				EnumerableRules.ENUMERABLE_UNION_RULE, EnumerableRules.ENUMERABLE_INTERSECT_RULE);
		
		relTree = trimUnusedFields(relTree);
		
		RelNode relnode = optimizer.optimize(relTree, relTree.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
		traverseRelNode(relnode, 0, new PrintWriter(System.out, true));
		return relnode;
	}
	
	/**
	 * The functions returns relationship node trimming unused fields
	 * @param relNode
	 * @return relnode with unused fields
	 */
	public static RelNode trimUnusedFields(RelNode relNode) {
	    final List<RelOptTable> relOptTables = RelOptUtil.findAllTables(relNode);
	    RelOptSchema relOptSchema = null;
	    if (relOptTables.size() != 0) {
	      relOptSchema = relOptTables.get(0).getRelOptSchema();
	    }
	    final RelBuilder relBuilder = RelFactories.LOGICAL_BUILDER.create(
	        relNode.getCluster(), relOptSchema);
	    final RelFieldTrimmer relFieldTrimmer = new RelFieldTrimmer(null, relBuilder);
	    final RelNode rel = relFieldTrimmer.trim(relNode);
	    return rel;
	  
	}
	
	/**
	 * The function returns Optimized RelNode Filter Into TableScan
	 * 
	 * @param tablecolumnsmap
	 * @param tablecolumntypesmap
	 * @param sql
	 * @param db
	 * @param isDistinct
	 * @return Optimized RelNode Filter Into TableScan
	 * @throws Exception
	 */
	public static RelNode getSQLFilter(ConcurrentMap<String, List<String>> tablecolumnsmap,
			ConcurrentMap<String, List<SqlTypeName>> tablecolumntypesmap, String sql, String db,
			AtomicBoolean isDistinct) throws Exception {
		Set<String> tablesfromconfig = tablecolumnsmap.keySet();
		SimpleSchema.Builder builder = SimpleSchema.newBuilder(db);
		for (String table : tablesfromconfig) {
			List<String> columns = tablecolumnsmap.get(table);
			List<SqlTypeName> sqltypes = tablecolumntypesmap.get(table);
			builder.addTable(getSimpleTable(table, columns.toArray(new String[columns.size()]),
					sqltypes.toArray(new SqlTypeName[sqltypes.size()])));
		}
		SimpleSchema schema = builder.build();
		Optimizer optimizer = Optimizer.create(schema);
		SqlNode sqlTree = optimizer.parse(sql);
		sqlTree = optimizer.validate(sqlTree);
		if (sqlTree.getKind() == SqlKind.SELECT) {
			SqlSelect selectNode = (SqlSelect) sqlTree;
			isDistinct.set(selectNode.isDistinct());
		}
		RelNode relTree = optimizer.convert(sqlTree);
		RuleSet rules = RuleSets.ofList(CoreRules.FILTER_TO_CALC, CoreRules.PROJECT_TO_CALC, CoreRules.FILTER_MERGE,
				CoreRules.FILTER_CALC_MERGE, CoreRules.PROJECT_CALC_MERGE, CoreRules.AGGREGATE_PROJECT_MERGE,
				CoreRules.AGGREGATE_JOIN_TRANSPOSE, CoreRules.AGGREGATE_PROJECT_MERGE,
				CoreRules.PROJECT_AGGREGATE_MERGE, CoreRules.PROJECT_MERGE, CoreRules.FILTER_INTO_JOIN,
				CoreRules.FILTER_PROJECT_TRANSPOSE, EnumerableRules.ENUMERABLE_TABLE_SCAN_RULE,
				EnumerableRules.ENUMERABLE_PROJECT_RULE, EnumerableRules.ENUMERABLE_FILTER_RULE,
				EnumerableRules.ENUMERABLE_AGGREGATE_RULE, EnumerableRules.ENUMERABLE_SORT_RULE,
				EnumerableRules.ENUMERABLE_SORTED_AGGREGATE_RULE, EnumerableRules.ENUMERABLE_JOIN_RULE,
				EnumerableRules.ENUMERABLE_UNION_RULE, EnumerableRules.ENUMERABLE_INTERSECT_RULE);

		return optimizer.optimize(relTree, relTree.getTraitSet().plus(EnumerableConvention.INSTANCE), rules);
	}

	/**
	 * Get simple table given the tablename, fields and types
	 * 
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
	 * 
	 * @param statement
	 * @param tables
	 */
	public static void getAllTables(Statement statement, List<String> tables) {
		net.sf.jsqlparser.util.TablesNamesFinder tablesNamesFinder = new net.sf.jsqlparser.util.TablesNamesFinder();
		tables.addAll(tablesNamesFinder.getTableList(statement));
	}

	/**
	 * Get orc file to store columnar data
	 * 
	 * @return path
	 */
	protected static String getORCFilePath(String randomuuid) {
		String tmpdir = isNull(DataSamudayaProperties.get()) ? System.getProperty(DataSamudayaConstants.TMPDIR)
				: DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TMPDIR,
				System.getProperty(DataSamudayaConstants.TMPDIR));
		new File(tmpdir + DataSamudayaConstants.FORWARD_SLASH + FileSystemSupport.MDS).mkdirs();
		return tmpdir + DataSamudayaConstants.FORWARD_SLASH + FileSystemSupport.MDS
				+ DataSamudayaConstants.FORWARD_SLASH + randomuuid + DataSamudayaConstants.ORCFILE_EXT;
	}

	/**
	 * Get the orcs crc with path
	 * 
	 * @return crcpath
	 */
	protected static String getORCCRCFilePath(String uuid) {
		String tmpdir = isNull(DataSamudayaProperties.get()) ? System.getProperty(DataSamudayaConstants.TMPDIR)
				: DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TMPDIR,
				System.getProperty(DataSamudayaConstants.TMPDIR));
		new File(tmpdir + DataSamudayaConstants.FORWARD_SLASH + FileSystemSupport.MDS).mkdirs();
		return tmpdir + DataSamudayaConstants.FORWARD_SLASH + FileSystemSupport.MDS
				+ DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.DOT + uuid
				+ DataSamudayaConstants.ORCFILE_EXT + DataSamudayaConstants.CRCFILE_EXT;
	}

	/**
	 * Creates ORC file for given CSV Record and returns path
	 * 
	 * @param airlineheader
	 * @param headertypes
	 * @param records
	 * @return filepath
	 * @throws Exception
	 */
	public static String createORCFile(List<String> headers, List<SqlTypeName> headertypes, Stream<CSVRecord> records)
			throws Exception {
		Configuration configuration = new Configuration();
		TypeDescription schema = TypeDescription.fromString(convertFieldsToString(headers, headertypes));

		// Create ORC WriterOptions
		WriterOptions options = OrcFile.writerOptions(configuration).setSchema(schema).compress(CompressionKind.LZ4)
				.blockPadding(true).blockSize(128 * DataSamudayaConstants.MB);
		String fileuuid = UUID.randomUUID().toString();
		String orcfilepath = getORCFilePath(fileuuid);
		// Create an ORC file writer
		try (Writer writer = OrcFile.createWriter(new Path(orcfilepath), options);) {
			VectorizedRowBatch batch = schema.createRowBatch();
			records.forEach(csvrecord -> {
				try {
					// Create a row batch and populate it with data
					for (int index = 0;index < headers.size();index++) {
						batch.cols[index].noNulls = true;
						batch.cols[index].isNull[batch.size] = false;
						writeValueToVector(batch.size, batch.cols[index], csvrecord.get(headers.get(index)));
					}
					if (batch.size == batch.getMaxSize() - 1) {
						batch.size++;
						writer.addRowBatch(batch);
						batch.reset();
					} else {
						batch.size++;
					}
					if (batch.size != 0) {
						writer.addRowBatch(batch);
						batch.reset();
					}
				} catch (Exception ex) {
					log.error(DataSamudayaConstants.EMPTY, ex);
				}
			});
			new File(orcfilepath).deleteOnExit();
			new File(getORCCRCFilePath(fileuuid)).deleteOnExit();
			return orcfilepath;
		} finally {

		}
	}

	/**
	 * Convert header and types to string in orc format
	 * 
	 * @param airlineheader
	 * @param headertypes
	 * @return String in struct format
	 */
	public static String convertFieldsToString(List<String> airlineheader, List<SqlTypeName> headertypes) {
		StringBuilder builder = new StringBuilder();
		builder.append("struct<");
		for (int index = 0;index < airlineheader.size();index++) {
			builder.append(airlineheader.get(index));
			builder.append(":");
			builder.append(convertSqlTypesToString(headertypes.get(index)));
			if (index < airlineheader.size() - 1) {
				builder.append(",");
			}
		}
		builder.append(">");
		return builder.toString();

	}

	/**
	 * Converts SQL Types to string
	 * 
	 * @param sqltypename
	 * @return string
	 */
	public static String convertSqlTypesToString(SqlTypeName sqltypename) {
		if (sqltypename == SqlTypeName.VARCHAR) {
			return "string";
		} else if (sqltypename == SqlTypeName.INTEGER) {
			return "int";
		} else if (sqltypename == SqlTypeName.DOUBLE) {
			return "double";
		}
		return "string";
	}

	/**
	 * Writes value to value vector for index
	 * 
	 * @param index
	 * @param cv
	 * @param value
	 */
	public static void writeValueToVector(int index, ColumnVector cv, Object value) {
		if (cv instanceof LongColumnVector lcv) {
			if (NumberUtils.isCreatable((String) value)) {
				lcv.vector[index] = Long.valueOf((String) value);
			} else {
				lcv.vector[index] = 0l;
			}
		} else if (cv instanceof BytesColumnVector bcv) {
			byte[] values = ((String) value).getBytes();
			bcv.setRef(index, values, 0, values.length);
		}
	}

	/**
	 * Loads the record reader object
	 * 
	 * @param orcfilepath
	 * @return orc rrr object
	 * @throws Exception
	 */
	public static OrcReaderRecordReader getOrcStreamRecords(String orcfilepath, String[] allcolumns,
			List<String> requiredcolumns, List<SqlTypeName> sqltypes) throws Exception {
		Configuration configuration = new Configuration();
		Reader reader = OrcFile.createReader(new Path(orcfilepath), OrcFile.readerOptions(configuration));
		RecordReader rows = reader.rows();
		VectorizedRowBatch batch = reader.getSchema().createRowBatch();
		Map<String, Integer> colindex = getColumnIndex(allcolumns);
		TypeDescription schema = TypeDescription.fromString(
				convertFieldsToString(requiredcolumns, getRequiredColumnTypes(colindex, requiredcolumns, sqltypes)));
		Stream<Map<String, Object>> mapStream = StreamSupport
				.stream(new ORCRecordSpliterator(rows, schema, batch, colindex), false);
		OrcReaderRecordReader orrr = new OrcReaderRecordReader(reader, rows, mapStream);
		return orrr;
	}

	/**
	 * This function returns required column types
	 * 
	 * @param colindex
	 * @param requiredcolumns
	 * @param sqltypes
	 * @return get required column types
	 */
	public static List<SqlTypeName> getRequiredColumnTypes(Map<String, Integer> colindex, List<String> requiredcolumns,
			List<SqlTypeName> sqltypes) {
		List<SqlTypeName> sqltypesrequiredcolumns = new ArrayList<>();
		if (CollectionUtils.isNotEmpty(requiredcolumns)) {
			for (String reqcols : requiredcolumns) {
				sqltypesrequiredcolumns.add(sqltypes.get(colindex.get(reqcols).intValue()));
			}
		}
		return sqltypesrequiredcolumns;
	}

	/**
	 * This function returns column with index in map
	 * 
	 * @param tablecolumns
	 * @return column with index
	 */
	public static Map<String, Integer> getColumnIndex(String[] tablecolumns) {
		Map<String, Integer> roottablecolumnindexmap = new ConcurrentHashMap<>();
		if (nonNull(tablecolumns)) {
			for (int originalcolumnindex = 0;originalcolumnindex < tablecolumns.length;originalcolumnindex++) {
				roottablecolumnindexmap.put(tablecolumns[originalcolumnindex], Integer.valueOf(originalcolumnindex));
			}
		}
		return roottablecolumnindexmap;
	}

	/**
	 * Converts sqltypes and colums to column map
	 * 
	 * @param sqltypes
	 * @param cols
	 * @return column types map
	 */
	public static Map<String, SqlTypeName> getColumnTypesByColumn(List<SqlTypeName> sqltypes, List<String> cols) {
		Map<String, SqlTypeName> colsqltypenamemap = new ConcurrentHashMap<>();
		for (int index = 0;index < cols.size();index++) {
			colsqltypenamemap.put(cols.get(index), sqltypes.get(index));
		}
		return colsqltypenamemap;
	}

	/**
	 * 
	 * @param value
	 * @param type
	 * @return
	 */
	public static void setYosegiObjectByValue(String value, SqlTypeName type, Map<String, Object> mapvalues,
			String col) {
		try {
			if (type == SqlTypeName.INTEGER) {
				if (NumberUtils.isCreatable((String) value)) {
					mapvalues.put(col, new IntegerObj(Integer.valueOf(value)));
					mapvalues.put(col + DataSamudayaConstants.SQLCOUNTFORAVG, new BooleanObj(true));
				} else {
					mapvalues.put(col, new IntegerObj(Integer.valueOf(0)));
					mapvalues.put(col + DataSamudayaConstants.SQLCOUNTFORAVG, new BooleanObj(false));
				}
			} else if (type == SqlTypeName.BIGINT) {
				if (NumberUtils.isCreatable((String) value)) {
					mapvalues.put(col, new LongObj(Long.valueOf(value)));
					mapvalues.put(col + DataSamudayaConstants.SQLCOUNTFORAVG, new BooleanObj(true));
				} else {
					mapvalues.put(col, new LongObj(Long.valueOf(0l)));
					mapvalues.put(col + DataSamudayaConstants.SQLCOUNTFORAVG, new BooleanObj(false));
				}
			} else if (type == SqlTypeName.VARCHAR) {
				mapvalues.put(col, new StringObj(value));
			} else if (type == SqlTypeName.FLOAT) {
				if (NumberUtils.isCreatable((String) value)) {
					mapvalues.put(col, new FloatObj(Float.valueOf(value)));
					mapvalues.put(col + DataSamudayaConstants.SQLCOUNTFORAVG, new BooleanObj(true));
				} else {
					mapvalues.put(col, new FloatObj(Float.valueOf(0.0f)));
					mapvalues.put(col + DataSamudayaConstants.SQLCOUNTFORAVG, new BooleanObj(false));
				}
			} else if (type == SqlTypeName.DOUBLE) {
				if (NumberUtils.isCreatable((String) value)) {
					mapvalues.put(col, new DoubleObj(Double.valueOf(value)));
					mapvalues.put(col + DataSamudayaConstants.SQLCOUNTFORAVG, new BooleanObj(true));
				} else {
					mapvalues.put(col, new DoubleObj(Double.valueOf(0.0d)));
					mapvalues.put(col + DataSamudayaConstants.SQLCOUNTFORAVG, new BooleanObj(false));
				}
			} else if (type == SqlTypeName.BOOLEAN) {
				mapvalues.put(col, new BooleanObj(Boolean.valueOf(value)));
				mapvalues.put(col + DataSamudayaConstants.SQLCOUNTFORAVG, new BooleanObj(true));
			} else {
				mapvalues.put(col, new StringObj(value));
				mapvalues.put(col + DataSamudayaConstants.SQLCOUNTFORAVG, new BooleanObj(true));
			}
		} catch (Exception ex) {
			mapvalues.put(col, new StringObj(DataSamudayaConstants.EMPTY));
		}
	}

	/**
	 * The method populates the object and toconsider valueobject for the given
	 * index
	 * 
	 * @param value
	 * @param type
	 * @param objvalues
	 * @param valuestoconsider
	 * @param index
	 */
	public static void getValueByIndex(String value, SqlTypeName type, Object[] objvalues, Object[] valuestoconsider,
			int index) {
		try {
			if (type == SqlTypeName.INTEGER) {
				if (NumberUtils.isCreatable((String) value)) {
					objvalues[index] = Integer.valueOf(value);
					valuestoconsider[index] = true;
				} else {
					objvalues[index] = 0;
					valuestoconsider[index] = false;
				}
			} else if (type == SqlTypeName.BIGINT) {
				if (NumberUtils.isCreatable((String) value)) {
					objvalues[index] = Long.valueOf(value);
					valuestoconsider[index] = true;
				} else {
					objvalues[index] = 0l;
					valuestoconsider[index] = false;
				}
			} else if (type == SqlTypeName.VARCHAR) {
				objvalues[index] = value;
				valuestoconsider[index] = true;
			} else if (type == SqlTypeName.FLOAT) {
				if (NumberUtils.isCreatable((String) value)) {
					objvalues[index] = Float.valueOf(value);
					valuestoconsider[index] = true;
				} else {
					objvalues[index] = 0.0f;
					valuestoconsider[index] = false;
				}
			} else if (type == SqlTypeName.DOUBLE) {
				if (NumberUtils.isCreatable((String) value)) {
					objvalues[index] = Double.valueOf(value);
					valuestoconsider[index] = true;
				} else {
					objvalues[index] = 0.0d;
					valuestoconsider[index] = false;
				}
			} else if (type == SqlTypeName.BOOLEAN) {
				objvalues[index] = Boolean.valueOf(value);
				valuestoconsider[index] = true;
			} else {
				objvalues[index] = value;
				valuestoconsider[index] = true;
			}
		} catch (Exception ex) {
			objvalues[index] = DataSamudayaConstants.EMPTY;
			valuestoconsider[index] = false;
		}
	}

	/**
	 * Converts yosegi bytes to stream of records
	 * 
	 * @param yosegibytes
	 * @param reqcols
	 * @param allcols
	 * @param sqltypes
	 * @return stream of map
	 * @throws Exception
	 */
	public static Stream<Object[]> getYosegiStreamRecords(byte[] yosegibytes, List<Integer> reqcols,
			List<String> allcols, List<SqlTypeName> sqltypes) throws Exception {
		InputStream in = new ByteArrayInputStream(yosegibytes);
		YosegiSchemaReader reader = new YosegiSchemaReader();
		reader.setNewStream(in, Long.valueOf(yosegibytes.length), new jp.co.yahoo.yosegi.config.Configuration());
		Map<String, SqlTypeName> sqltypesallcols = getColumnTypesByColumn(sqltypes, allcols);
		return StreamSupport.stream(new YosegiRecordSpliterator(reader, reqcols, allcols, sqltypesallcols), false);

	}

	/**
	 * This function populates map from primitiveobject and
	 * 
	 * @param map
	 * @param col
	 * @param po
	 */
	public static void getValueFromYosegiObject(Object[] valueobjects, Object[] toconsidervalueobjects, String col,
			Map<String, Object> mapforavg, int index) {
		PrimitiveObject po = (PrimitiveObject) mapforavg.get(col);
		try {
			if (po instanceof IntegerObj iobj) {
				valueobjects[index] = iobj.getInt();
				toconsidervalueobjects[index] = ((BooleanObj) mapforavg.get(col + DataSamudayaConstants.SQLCOUNTFORAVG))
						.getBoolean();
			} else if (po instanceof LongObj lobj) {
				valueobjects[index] = lobj.getLong();
				toconsidervalueobjects[index] = ((BooleanObj) mapforavg.get(col + DataSamudayaConstants.SQLCOUNTFORAVG))
						.getBoolean();
			} else if (po instanceof FloatObj fobj) {
				valueobjects[index] = fobj.getFloat();
				toconsidervalueobjects[index] = ((BooleanObj) mapforavg.get(col + DataSamudayaConstants.SQLCOUNTFORAVG))
						.getBoolean();
			} else if (po instanceof DoubleObj dobj) {
				valueobjects[index] = dobj.getDouble();
				toconsidervalueobjects[index] = ((BooleanObj) mapforavg.get(col + DataSamudayaConstants.SQLCOUNTFORAVG))
						.getBoolean();
			} else if (po instanceof BooleanObj bobj) {
				valueobjects[index] = bobj.getBoolean();
				toconsidervalueobjects[index] = true;
			} else if (po instanceof StringObj sobj) {
				valueobjects[index] = sobj.getString();
				toconsidervalueobjects[index] = true;
			}
		} catch (Exception ex) {
			if (po instanceof StringObj sobj) {
				valueobjects[index] = "";
				toconsidervalueobjects[index] = false;
			}
		}
	}

	/**
	 * Evaluates expression
	 * 
	 * @param node
	 * @param values
	 * @return predicate as true or false
	 */
	public static boolean evaluateExpression(RexNode node, Object[] values) {
		if (node.isA(SqlKind.AND)) {
			// For AND nodes, recursively evaluate left and right children
			RexCall call = (RexCall) node;
			boolean initvalue = true;
			for (int operandindex = 0;operandindex < call.operands.size();operandindex++) {
				initvalue = initvalue && evaluateExpression(call.operands.get(operandindex), values);
				if (!initvalue) {
					break;
				}
			}
			return initvalue;
		} else if (node.isA(SqlKind.OR)) {
			// For OR nodes, recursively evaluate left and right children
			RexCall call = (RexCall) node;
			boolean initvalue = false;
			for (int operandindex = 0;operandindex < call.operands.size();operandindex++) {
				initvalue = initvalue || evaluateExpression(call.operands.get(operandindex), values);
				if (initvalue) {
					break;
				}
			}
			return initvalue;
		} else if (node.isA(SqlKind.EQUALS)) {
			// For EQUALS nodes, evaluate left and right children
			RexCall call = (RexCall) node;
			Object value1 = getValueObject(call.operands.get(0), values);
			Object value2 = getValueObject(call.operands.get(1), values);
			return evaluatePredicate(value1, value2, "=");
		} else if (node.isA(SqlKind.NOT_EQUALS)) {
			// For EQUALS nodes, evaluate left and right children
			RexCall call = (RexCall) node;
			Object value1 = getValueObject(call.operands.get(0), values);
			Object value2 = getValueObject(call.operands.get(1), values);
			return evaluatePredicate(value1, value2, "<>");
		} else if (node.isA(SqlKind.GREATER_THAN)) {
			// For EQUALS nodes, evaluate left and right children
			RexCall call = (RexCall) node;
			Object value1 = getValueObject(call.operands.get(0), values);
			Object value2 = getValueObject(call.operands.get(1), values);
			return evaluatePredicate(value1, value2, ">");
		} else if (node.isA(SqlKind.GREATER_THAN_OR_EQUAL)) {
			// For EQUALS nodes, evaluate left and right children
			RexCall call = (RexCall) node;
			Object value1 = getValueObject(call.operands.get(0), values);
			Object value2 = getValueObject(call.operands.get(1), values);
			return evaluatePredicate(value1, value2, ">=");
		} else if (node.isA(SqlKind.LESS_THAN)) {
			// For EQUALS nodes, evaluate left and right children
			RexCall call = (RexCall) node;
			Object value1 = getValueObject(call.operands.get(0), values);
			Object value2 = getValueObject(call.operands.get(1), values);
			return evaluatePredicate(value1, value2, "<");
		} else if (node.isA(SqlKind.LESS_THAN_OR_EQUAL)) {
			// For EQUALS nodes, evaluate left and right children
			RexCall call = (RexCall) node;
			Object value1 = getValueObject(call.operands.get(0), values);
			Object value2 = getValueObject(call.operands.get(1), values);
			return evaluatePredicate(value1, value2, "<=");
		} else if (node.isA(SqlKind.LIKE)) {
			// For EQUALS nodes, evaluate left and right children
			RexCall call = (RexCall) node;
			Object value1 = getValueObject(call.operands.get(0), values);
			Object value2 = getValueObject(call.operands.get(1), values);
			return value1.equals(value2);
		} else if (node.isA(SqlKind.SEARCH)) {
			// For EQUALS nodes, evaluate left and right children
			RexCall call = (RexCall) node;
			Object value1 = getValueObject(call.operands.get(0), values);
			Sarg value2 = (Sarg) getValueObject(call.operands.get(1), values);
			Set searchvalues = new LinkedHashSet<>();
			if(CollectionUtils.isNotEmpty(value2.rangeSet.asRanges()) && value2.rangeSet.asRanges().size()==1) {
				Set range = value2.rangeSet.asRanges();
				Range r = (Range) range.iterator().next();
				Object valuelowendpoint = getSearchValue(value1, r.lowerEndpoint());
				Object valueupperendpoint = getSearchValue(value1, r.upperEndpoint());
				if(valuelowendpoint != valueupperendpoint) {
					return evaluatePredicate(value1, valuelowendpoint, ">=") && evaluatePredicate(value1, valueupperendpoint, "<=");
				}
				return evaluatePredicate(value1, valuelowendpoint, "=");
				
			} else {
				Ord.forEach(value2.rangeSet.asRanges(), (r, i) -> {
					Object valuelowendpoint = ((Range) r).lowerEndpoint();
					searchvalues.add(getSearchValue(value1, valuelowendpoint));
				});
				return searchvalues.contains(value1);
			}			
		} else {
			return false;
		}
	}

	/**
	 * Converts Search value1 to corresponding types of value2 
	 * @param value1
	 * @param value2
	 * @return converted value
	 */
	public static Object getSearchValue(Object value1,Object value2) {
		if(value2 instanceof BigDecimal bdecimal) {
			if(value1 instanceof Long) {
				return (bdecimal.longValue());
			} else if(value1 instanceof Float) {
				return (bdecimal.floatValue());
			} else if(value1 instanceof Double) {
				return (bdecimal.doubleValue());
			} else if(value1 instanceof Integer) {
				return (bdecimal.intValue());
			}
		} else if(value2 instanceof Long lvalue) {
			return (lvalue);
		} else if(value1 instanceof Float flvalue) {
			return (flvalue);
		} else if(value1 instanceof Double dvalue) {
			return (dvalue);
		} else if(value1 instanceof Integer ivalue) {
			return (ivalue);
		}
		return value2;
	}
	
	
	/**
	 * Evaluate whether to consider for aggregate
	 * 
	 * @param rexnode
	 * @param boolvalues
	 * @return true or false
	 */
	public static boolean toEvaluateRexNode(RexNode rexnode, Object[] boolvalues) {
		if (rexnode.isA(SqlKind.PLUS) || rexnode.isA(SqlKind.MINUS) || rexnode.isA(SqlKind.TIMES)
				|| rexnode.isA(SqlKind.DIVIDE)) {
			// For addition nodes, recursively evaluate left and right children
			RexCall call = (RexCall) rexnode;
			return toEvaluateRexNode(call.operands.get(0), boolvalues)
					&& toEvaluateRexNode(call.operands.get(1), boolvalues);
		} else if (rexnode.isA(SqlKind.LITERAL)) {
			return true;
		} else if (rexnode instanceof RexInputRef inputRef) {
			// Handle column references
			if (boolvalues.length < inputRef.getIndex()) {
				return true;
			}
			return (Boolean) boolvalues[inputRef.getIndex()];
		} else if (rexnode instanceof RexCall call) {
			String name = call.getOperator().getName().toLowerCase();
			if ("pow".equals(name)) {
				return toEvaluateRexNode(call.operands.get(0), boolvalues)
						&& toEvaluateRexNode(call.operands.get(1), boolvalues);
			} else if ("substring".equals(name)) {
				boolean resultvalue = toEvaluateRexNode(call.operands.get(0), boolvalues)
						&& toEvaluateRexNode(call.operands.get(1), boolvalues);
				resultvalue = call.operands.size() > 2
						? resultvalue && toEvaluateRexNode(call.operands.get(2), boolvalues)
						: resultvalue;
				return resultvalue;
			} else {
				for (RexNode opernode : call.operands) {
					return toEvaluateRexNode(opernode, boolvalues);
				}
				return true;
			}
		} else {
			return false;
		}
	}

	/**
	 * Gets value Object from RexNode and values
	 * 
	 * @param node
	 * @param values
	 * @return returns the values of RexNode
	 */
	public static Object getValueObject(RexNode node, Object[] values) {
		if (node.isA(SqlKind.PLUS)) {
			// For addition nodes, recursively evaluate left and right children
			RexCall call = (RexCall) node;
			return evaluateValuesByOperator(getValueObject(call.operands.get(0), values),
					getValueObject(call.operands.get(1), values), "+");
		} else if (node.isA(SqlKind.MINUS)) {
			// For subtraction nodes, recursively evaluate left and right
			// children
			RexCall call = (RexCall) node;
			return evaluateValuesByOperator(getValueObject(call.operands.get(0), values),
					getValueObject(call.operands.get(1), values), "-");
		} else if (node.isA(SqlKind.TIMES)) {
			// For multiplication nodes, recursively evaluate left and right
			// children
			RexCall call = (RexCall) node;
			return evaluateValuesByOperator(getValueObject(call.operands.get(0), values),
					getValueObject(call.operands.get(1), values), "*");
		} else if (node.isA(SqlKind.DIVIDE)) {
			// For division nodes, recursively evaluate left and right children
			RexCall call = (RexCall) node;
			return evaluateValuesByOperator(getValueObject(call.operands.get(0), values),
					getValueObject(call.operands.get(1), values), "/");
		} else if (node.isA(SqlKind.LITERAL)) {
			// For literals, return their value
			RexLiteral literal = (RexLiteral) node;
			if(literal.getTypeName() == SqlTypeName.SARG) {
				return getValue(literal, literal.getTypeName());
			}
			return getValue(literal, literal.getType().getSqlTypeName());
		} else if (node.isA(SqlKind.FUNCTION)) {
			// For functions, return their value
			return evaluateRexNode(node, values);
		} else if (node instanceof RexInputRef inputRef) {
			// Handle column references
			return values[inputRef.getIndex()];
		} else {
			return null;
		}
	}

	public static Object getValue(RexLiteral value, SqlTypeName type) {
		try {
			
			if (type == SqlTypeName.SARG) {
				return value.getValueAs(Sarg.class);
			} else if (type == SqlTypeName.INTEGER) {
				return value.getValueAs(Integer.class);
			} else if (type == SqlTypeName.BIGINT) {
				return value.getValueAs(Long.class);
			} else if (type == SqlTypeName.VARCHAR) {
				return value.getValueAs(String.class);
			} else if (type == SqlTypeName.CHAR) {
				return value.getValueAs(String.class);
			} else if (type == SqlTypeName.FLOAT) {
				return value.getValueAs(Float.class);
			} else if (type == SqlTypeName.DOUBLE) {
				return value.getValueAs(Double.class);
			} else if (type == SqlTypeName.DECIMAL) {
				return value.getValueAs(Double.class);
			} else if (type == SqlTypeName.BOOLEAN) {
				return value.getValueAs(Boolean.class);
			} else if (type == SqlTypeName.SYMBOL) {
				return ((SqlTrimFunction.Flag) value.getValue4()).name();
			} else {
				return value.getValueAs(String.class);
			}
		} catch (Exception ex) {
			if (type == SqlTypeName.INTEGER) {
				return Integer.valueOf(0);
			} else if (type == SqlTypeName.BIGINT) {
				return Long.valueOf(0);
			} else if (type == SqlTypeName.VARCHAR) {
				return String.valueOf(DataSamudayaConstants.EMPTY);
			} else if (type == SqlTypeName.FLOAT) {
				return Float.valueOf(0.0f);
			} else if (type == SqlTypeName.DOUBLE) {
				return Double.valueOf(0.0d);
			} else {
				return String.valueOf(DataSamudayaConstants.EMPTY);
			}
		}
	}

	/**
	 * 
	 * @param aggfunctions
	 * @param tuple1
	 * @param tuple2
	 * @return evaluated function
	 */
	public static Tuple evaluateTuple(List<String> aggfunctions, Tuple tuple1, Tuple tuple2) {
		try {
			// Get the class of the Tuple
			Class<?> tupleClass = tuple1.getClass();
			// Create a new instance of the Tuple
			Tuple result = (Tuple) tupleClass.getConstructor(tuple2.getClass()).newInstance(tuple1);
			// Get all the fields of the Tuple class
			java.lang.reflect.Field[] fields = tuple1.getClass().getFields();
			int index = 0;
			boolean avgindex = false;
			String func = aggfunctions.get(index);
			// Iterate over the fields and perform summation
			for (java.lang.reflect.Field field : fields) {
				// Make the field accessible, as it might be private
				field.setAccessible(true);

				// Get the values of the fields from both tuples
				Object value1 = field.get(tuple1);
				Object value2 = field.get(tuple2);
				field.set(result, evaluateFunction(func, value1, value2));
				// Set the sum of the values in the result tuple
				if (index + 1 < aggfunctions.size()) {
					if (avgindex) {
						func = aggfunctions.get(index);
						avgindex = false;
						index++;
					} else if (!"avg".equalsIgnoreCase(aggfunctions.get(index))) {
						func = aggfunctions.get(index + 1);
						index++;
						avgindex = false;
					} else {
						func = aggfunctions.get(index);
						avgindex = true;
					}
				}
			}

			return result;
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
		return null;
	}

	/**
	 * Evaluates function By function name
	 * 
	 * @param func
	 * @param leftValue
	 * @param rightValue
	 * @return evaluated values
	 */
	public static Object evaluateFunction(String functionname, Object leftValue, Object rightValue) {
		if (functionname.toLowerCase().contains("count") || functionname.toLowerCase().contains("sum") || functionname.toLowerCase().contains("avg")) {
			return evaluateValuesByOperator(leftValue, rightValue, "+");
		} else if (functionname.toLowerCase().contains("min")) {
			return SQLUtils.evaluateValuesByFunctionMin(leftValue, rightValue);
		} else if (functionname.toLowerCase().contains("max")) {
			return SQLUtils.evaluateValuesByFunctionMax(leftValue, rightValue);
		}
		return null;
	}

	/**
	 * Gets Object from tuple
	 * 
	 * @param tuple
	 * @return values array
	 */
	public static Object[] populateObjectFromTuple(Tuple tuple) {
		if (tuple instanceof Tuple1 tup && tup.v1().equals(DataSamudayaConstants.EMPTY)) {
			return null;
		} else if (tuple instanceof Tuple1 tup) {
			Object[] mapvalues = new Object[1];
			mapvalues[0] = tup.v1();
			return mapvalues;
		} else if (tuple instanceof Tuple2 tup) {
			Object[] mapvalues = new Object[2];
			mapvalues[0] = tup.v1();
			mapvalues[1] = tup.v2();
			return mapvalues;
		} else if (tuple instanceof Tuple3 tup) {
			Object[] mapvalues = new Object[3];
			mapvalues[0] = tup.v1();
			mapvalues[1] = tup.v2();
			mapvalues[2] = tup.v3();
			return mapvalues;
		} else if (tuple instanceof Tuple4 tup) {
			Object[] mapvalues = new Object[4];
			mapvalues[0] = tup.v1();
			mapvalues[1] = tup.v2();
			mapvalues[2] = tup.v3();
			mapvalues[3] = tup.v4();
			return mapvalues;
		} else if (tuple instanceof Tuple5 tup) {
			Object[] mapvalues = new Object[5];
			mapvalues[0] = tup.v1();
			mapvalues[1] = tup.v2();
			mapvalues[2] = tup.v3();
			mapvalues[3] = tup.v4();
			mapvalues[4] = tup.v5();
			return mapvalues;
		} else if (tuple instanceof Tuple6 tup) {
			Object[] mapvalues = new Object[6];
			mapvalues[0] = tup.v1();
			mapvalues[1] = tup.v2();
			mapvalues[2] = tup.v3();
			mapvalues[3] = tup.v4();
			mapvalues[4] = tup.v5();
			mapvalues[5] = tup.v6();
			return mapvalues;
		} else if (tuple instanceof Tuple7 tup) {
			Object[] mapvalues = new Object[7];
			mapvalues[0] = tup.v1();
			mapvalues[1] = tup.v2();
			mapvalues[2] = tup.v3();
			mapvalues[3] = tup.v4();
			mapvalues[4] = tup.v5();
			mapvalues[5] = tup.v6();
			mapvalues[6] = tup.v7();
			return mapvalues;
		} else if (tuple instanceof Tuple8 tup) {
			Object[] mapvalues = new Object[8];
			mapvalues[0] = tup.v1();
			mapvalues[1] = tup.v2();
			mapvalues[2] = tup.v3();
			mapvalues[3] = tup.v4();
			mapvalues[4] = tup.v5();
			mapvalues[5] = tup.v6();
			mapvalues[6] = tup.v7();
			mapvalues[7] = tup.v8();
			return mapvalues;
		} else if (tuple instanceof Tuple9 tup) {
			Object[] mapvalues = new Object[9];
			mapvalues[0] = tup.v1();
			mapvalues[1] = tup.v2();
			mapvalues[2] = tup.v3();
			mapvalues[3] = tup.v4();
			mapvalues[4] = tup.v5();
			mapvalues[5] = tup.v6();
			mapvalues[6] = tup.v7();
			mapvalues[7] = tup.v8();
			mapvalues[8] = tup.v9();
			return mapvalues;
		} else if (tuple instanceof Tuple10 tup) {
			Object[] mapvalues = new Object[10];
			mapvalues[0] = tup.v1();
			mapvalues[1] = tup.v2();
			mapvalues[2] = tup.v3();
			mapvalues[3] = tup.v4();
			mapvalues[4] = tup.v5();
			mapvalues[5] = tup.v6();
			mapvalues[6] = tup.v7();
			mapvalues[7] = tup.v8();
			mapvalues[8] = tup.v9();
			mapvalues[9] = tup.v10();
			return mapvalues;
		} else if (tuple instanceof Tuple11 tup) {
			Object[] mapvalues = new Object[11];
			mapvalues[0] = tup.v1();
			mapvalues[1] = tup.v2();
			mapvalues[2] = tup.v3();
			mapvalues[3] = tup.v4();
			mapvalues[4] = tup.v5();
			mapvalues[5] = tup.v6();
			mapvalues[6] = tup.v7();
			mapvalues[7] = tup.v8();
			mapvalues[8] = tup.v9();
			mapvalues[9] = tup.v10();
			mapvalues[10] = tup.v11();
			return mapvalues;
		} else if (tuple instanceof Tuple12 tup) {
			Object[] mapvalues = new Object[12];
			mapvalues[0] = tup.v1();
			mapvalues[1] = tup.v2();
			mapvalues[2] = tup.v3();
			mapvalues[3] = tup.v4();
			mapvalues[4] = tup.v5();
			mapvalues[5] = tup.v6();
			mapvalues[6] = tup.v7();
			mapvalues[7] = tup.v8();
			mapvalues[8] = tup.v9();
			mapvalues[9] = tup.v10();
			mapvalues[10] = tup.v11();
			mapvalues[11] = tup.v12();
			return mapvalues;
		} else if (tuple instanceof Tuple13 tup) {
			Object[] mapvalues = new Object[13];
			mapvalues[0] = tup.v1();
			mapvalues[1] = tup.v2();
			mapvalues[2] = tup.v3();
			mapvalues[3] = tup.v4();
			mapvalues[4] = tup.v5();
			mapvalues[5] = tup.v6();
			mapvalues[6] = tup.v7();
			mapvalues[7] = tup.v8();
			mapvalues[8] = tup.v9();
			mapvalues[9] = tup.v10();
			mapvalues[10] = tup.v11();
			mapvalues[11] = tup.v12();
			mapvalues[12] = tup.v13();
			return mapvalues;
		} else if (tuple instanceof Tuple14 tup) {
			Object[] mapvalues = new Object[14];
			mapvalues[0] = tup.v1();
			mapvalues[1] = tup.v2();
			mapvalues[2] = tup.v3();
			mapvalues[3] = tup.v4();
			mapvalues[4] = tup.v5();
			mapvalues[5] = tup.v6();
			mapvalues[6] = tup.v7();
			mapvalues[7] = tup.v8();
			mapvalues[8] = tup.v9();
			mapvalues[9] = tup.v10();
			mapvalues[10] = tup.v11();
			mapvalues[11] = tup.v12();
			mapvalues[12] = tup.v13();
			mapvalues[13] = tup.v14();
			return mapvalues;
		} else if (tuple instanceof Tuple15 tup) {
			Object[] mapvalues = new Object[15];
			mapvalues[0] = tup.v1();
			mapvalues[1] = tup.v2();
			mapvalues[2] = tup.v3();
			mapvalues[3] = tup.v4();
			mapvalues[4] = tup.v5();
			mapvalues[5] = tup.v6();
			mapvalues[6] = tup.v7();
			mapvalues[7] = tup.v8();
			mapvalues[8] = tup.v9();
			mapvalues[9] = tup.v10();
			mapvalues[10] = tup.v11();
			mapvalues[11] = tup.v12();
			mapvalues[12] = tup.v13();
			mapvalues[13] = tup.v14();
			mapvalues[14] = tup.v15();
			return mapvalues;
		} else if (tuple instanceof Tuple16 tup) {
			Object[] mapvalues = new Object[16];
			mapvalues[0] = tup.v1();
			mapvalues[1] = tup.v2();
			mapvalues[2] = tup.v3();
			mapvalues[3] = tup.v4();
			mapvalues[4] = tup.v5();
			mapvalues[5] = tup.v6();
			mapvalues[6] = tup.v7();
			mapvalues[7] = tup.v8();
			mapvalues[8] = tup.v9();
			mapvalues[9] = tup.v10();
			mapvalues[10] = tup.v11();
			mapvalues[11] = tup.v12();
			mapvalues[12] = tup.v13();
			mapvalues[13] = tup.v14();
			mapvalues[14] = tup.v15();
			mapvalues[15] = tup.v16();
			return mapvalues;
		} else {
			throw new UnsupportedOperationException("Max supported Column is 16");
		}
	}

	/**
	 * The function returns array of group by column indexes
	 * 
	 * @param aggregate
	 * @return column indexes
	 */
	public static int[] getGroupByColumnIndexes(EnumerableAggregateBase aggregate) {
		// Extract the BitSet representing the Group By columns
		ImmutableBitSet groupSet = aggregate.getGroupSet();

		// Convert the BitSet to an array of integers
		int[] groupByColumnIndexes = new int[groupSet.cardinality()];
		int index = 0;
		for (int i = groupSet.nextSetBit(0);i >= 0;i = groupSet.nextSetBit(i + 1)) {
			groupByColumnIndexes[index++] = i;
		}

		return groupByColumnIndexes;
	}

	public static Object[] populateObjectFromFunctions(Tuple tuple, List<String> functions) {
		try {
			Class<?> cls = tuple.getClass();
			List<Object> processedvalues = new ArrayList<>();
			for (int funcindex = 0, valueindex = 1;funcindex < functions.size();funcindex++) {
				String funname = functions.get(funcindex);
				if (funname.toLowerCase().startsWith("avg")) {
					java.lang.reflect.Method method = cls.getMethod("v" + valueindex);
					Object valuesum = method.invoke(tuple);
					valueindex++;
					method = cls.getMethod("v" + valueindex);
					Object valuecount = method.invoke(tuple);
					processedvalues.add(evaluateValuesByOperator(valuesum, valuecount, "/"));
				} else {
					java.lang.reflect.Method method = cls.getMethod("v" + valueindex);
					Object value = method.invoke(tuple);
					processedvalues.add(value);
				}
				valueindex++;
			}
			return processedvalues.toArray(new Object[0]);
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
		return null;
	}

	/**
	 * The functions checks whether the RelNode has decendants.
	 * 
	 * @param relnode
	 * @param descendants
	 * @return true or false
	 */
	public static boolean hasDescendants(RelNode relnode, Map<RelNode, Boolean> descendants) {
		return descendants.get(relnode);
	}

	/**
	 * This function evaluates expression.
	 * 
	 * @param node
	 * @param values1
	 * @param values2
	 * @return
	 */
	public static boolean evaluateExpression(RexNode node, Object[] values1, Object[] values2) {
		if (node.isA(SqlKind.AND)) {
			// For AND nodes, recursively evaluate left and right children
			RexCall call = (RexCall) node;
			return evaluateExpression(call.operands.get(0), values1, values2)
					&& evaluateExpression(call.operands.get(1), values1, values2);
		} else if (node.isA(SqlKind.OR)) {
			// For OR nodes, recursively evaluate left and right children
			RexCall call = (RexCall) node;
			return evaluateExpression(call.operands.get(0), values1, values2)
					|| evaluateExpression(call.operands.get(1), values1, values2);
		} else if (node.isA(SqlKind.EQUALS)) {
			// For EQUALS nodes, evaluate left and right children
			RexCall call = (RexCall) node;
			Object value1 = getValueObject(call.operands.get(0), values1, values2);
			Object value2 = getValueObject(call.operands.get(1), values1, values2);
			return evaluatePredicate(value1, value2, "=");
		} else if (node.isA(SqlKind.NOT_EQUALS)) {
			// For EQUALS nodes, evaluate left and right children
			RexCall call = (RexCall) node;
			Object value1 = getValueObject(call.operands.get(0), values1, values2);
			Object value2 = getValueObject(call.operands.get(1), values1, values2);
			return evaluatePredicate(value1, value2, "<>");
		} else if (node.isA(SqlKind.GREATER_THAN)) {
			// For EQUALS nodes, evaluate left and right children
			RexCall call = (RexCall) node;
			Object value1 = getValueObject(call.operands.get(0), values1, values2);
			Object value2 = getValueObject(call.operands.get(1), values1, values2);
			return evaluatePredicate(value1, value2, ">");
		} else if (node.isA(SqlKind.GREATER_THAN_OR_EQUAL)) {
			// For EQUALS nodes, evaluate left and right children
			RexCall call = (RexCall) node;
			Object value1 = getValueObject(call.operands.get(0), values1, values2);
			Object value2 = getValueObject(call.operands.get(1), values1, values2);
			return evaluatePredicate(value1, value2, ">=");
		} else if (node.isA(SqlKind.LESS_THAN)) {
			// For EQUALS nodes, evaluate left and right children
			RexCall call = (RexCall) node;
			Object value1 = getValueObject(call.operands.get(0), values1, values2);
			Object value2 = getValueObject(call.operands.get(1), values1, values2);
			return evaluatePredicate(value1, value2, "<");
		} else if (node.isA(SqlKind.LESS_THAN_OR_EQUAL)) {
			// For EQUALS nodes, evaluate left and right children
			RexCall call = (RexCall) node;
			Object value1 = getValueObject(call.operands.get(0), values1, values2);
			Object value2 = getValueObject(call.operands.get(1), values1, values2);
			return evaluatePredicate(value1, value2, "<=");
		} else if (node instanceof RexLiteral rlit) {
			// Boolean Value
			return (boolean) getValue((RexLiteral) rlit, rlit.getType().getSqlTypeName());
		} else {
			return false;
		}
	}

	/**
	 * The function returns the values from value object for evaluation
	 * 
	 * @param node
	 * @param values1
	 * @param values2
	 * @return values from value objects
	 */
	public static Object getValueObject(RexNode node, Object[] values1, Object[] values2) {
		if (node.isA(SqlKind.PLUS)) {
			// For addition nodes, recursively evaluate left and right children
			RexCall call = (RexCall) node;
			return evaluateValuesByOperator(getValueObject(call.operands.get(0), values1, values2),
					getValueObject(call.operands.get(1), values1, values2), "+");
		} else if (node.isA(SqlKind.MINUS)) {
			// For subtraction nodes, recursively evaluate left and right
			// children
			RexCall call = (RexCall) node;
			return evaluateValuesByOperator(getValueObject(call.operands.get(0), values1, values2),
					getValueObject(call.operands.get(1), values1, values2), "-");
		} else if (node.isA(SqlKind.TIMES)) {
			// For multiplication nodes, recursively evaluate left and right
			// children
			RexCall call = (RexCall) node;
			return evaluateValuesByOperator(getValueObject(call.operands.get(0), values1, values2),
					getValueObject(call.operands.get(1), values1, values2), "*");
		} else if (node.isA(SqlKind.DIVIDE)) {
			// For division nodes, recursively evaluate left and right children
			RexCall call = (RexCall) node;
			return evaluateValuesByOperator(getValueObject(call.operands.get(0), values1, values2),
					getValueObject(call.operands.get(1), values1, values2), "/");
		} else if (node.isA(SqlKind.LITERAL)) {
			// For literals, return their value
			RexLiteral literal = (RexLiteral) node;
			return literal.getValue2();
		} else if (node instanceof RexInputRef inputRef) {
			// Handle column references
			return inputRef.getIndex() < values1.length ? values1[inputRef.getIndex()]
					: values2[inputRef.getIndex() - values1.length];
		} else {
			return null;
		}
	}

	/**
	 * Evaluates function RexNode with given values
	 * 
	 * @param node
	 * @param values
	 * @return value processed
	 */
	public static Object evaluateRexNode(RexNode node, Object[] values) {
		if (node.isA(SqlKind.FUNCTION) || node.isA(SqlKind.CASE)) {
			RexCall call = (RexCall) node;
			RexNode expfunc = call.getOperands().size() > 0 ? call.getOperands().get(0) : null;
			String name = call.getOperator().getName().toLowerCase();
			switch (name) {
				case "abs":

					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values), null, "abs");
				case "length", "char_length", "character_length":
					// Get the length of string value
					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values), null, "length");
				case "round":
					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values), null, "round");
				case "ceil":
					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values), null, "ceil");
				case "floor":
					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values), null, "floor");
				case "power":
					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values),
							evaluateRexNode(call.getOperands().get(1), values), "pow");
				case "pow":
					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values),
							evaluateRexNode(call.getOperands().get(1), values), "pow");
				case "sqrt":
					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values), null, "sqrt");
				case "exp":
					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values), null, "exp");
				case "loge":
					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values), null, "loge");
				case "lowercase", "lower", "lcase":
					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values), null, "lowercase");
				case "uppercase", "upper", "ucase":
					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values), null, "uppercase");
				case "base64encode":
					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values), null, "base64encode");
				case "base64decode":
					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values), null, "base64decode");
				case "normalizespaces":

					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values), null, "normalizespaces");
				case "currentisodate":

					return evaluateFunctionsWithType(null, null, "currentisodate");
				case "current_timemillis":

					return evaluateFunctionsWithType(null, null, "currenttimemillis");
				case "rand":

					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values), null, "rand");
				case "rand_integer":

					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values),
							evaluateRexNode(call.getOperands().get(1), values), "randinteger");
				case "acos":

					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values), null, "acos");
				case "asin":

					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values), null, "asin");
				case "atan":

					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values), null, "atan");
				case "cos":

					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values), null, "cos");
				case "sin":

					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values), null, "sin");
				case "tan":

					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values), null, "tan");
				case "cosec":

					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values), null, "cosec");
				case "sec":

					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values), null, "sec");
				case "cot":

					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values), null, "cot");
				case "cbrt":

					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values), null, "cbrt");
				case "pii":

					return evaluateFunctionsWithType(null, null, "pii");
				case "degrees":

					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values), null, "degrees");
				case "radians":

					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values), null, "radians");
				case "trimstr":

					return evaluateFunctionsWithType(evaluateRexNode(expfunc, values), null, "trim");
				case "substring":
					RexLiteral pos = (RexLiteral) call.getOperands().get(1);
					RexLiteral length = (RexLiteral) (call.getOperands().size() > 2 ? call.getOperands().get(2) : null);
					String val = (String) evaluateRexNode(expfunc, values);
					if (nonNull(length)) {
						return val.substring((Integer) getValue(pos, SqlTypeName.INTEGER),
								Math.min(((String) val).length(), (Integer) getValue(pos, SqlTypeName.INTEGER)
										+ (Integer) getValue(length, SqlTypeName.INTEGER)));
					}
					return val.substring((Integer) getValue(pos, SqlTypeName.INTEGER));
				case "overlay":
					pos = (RexLiteral) call.getOperands().get(2);
					length = (RexLiteral) (call.getOperands().size() > 3 ? call.getOperands().get(3) : null);
					String val1 = (String) evaluateRexNode(call.getOperands().get(0), values);
					String val2 = (String) evaluateRexNode(call.getOperands().get(1), values);
					if (nonNull(length)) {
						return val1
								.replaceAll(
										val1.substring(
												(Integer) getValue(pos, SqlTypeName.INTEGER), Math
														.min(((String) val2).length(),
																(Integer) getValue(pos, SqlTypeName.INTEGER)
																		+ (Integer) getValue(length, SqlTypeName.INTEGER))),
										val2);
					}
					return val1.replaceAll(val1.substring((Integer) getValue(pos, SqlTypeName.INTEGER)), val2);
				case "locate":
					val1 = (String) evaluateRexNode(call.getOperands().get(0), values);
					val2 = (String) evaluateRexNode(call.getOperands().get(1), values);
					pos = (RexLiteral) (call.getOperands().size() > 2 ? call.getOperands().get(2) : null);
					if (nonNull(pos)) {
						int positiontosearch = Math.min((Integer) getValue(pos, SqlTypeName.INTEGER), val2.length());
						return positiontosearch + val2.substring(positiontosearch).indexOf(val1);
					}
					return val2.indexOf(val1);
				case "cast":
					return evaluateRexNode(expfunc, values);
				case "group_concat":
					RexNode rexnode1 = call.getOperands().get(0);
					RexNode rexnode2 = call.getOperands().get(1);
					return (String) evaluateRexNode(rexnode1, values) + evaluateRexNode(rexnode2, values);
				case "concat":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);
					return (String) evaluateRexNode(rexnode1, values) + evaluateRexNode(rexnode2, values);
				case "position":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);
					RexNode rexnode3 = call.getOperands().size() > 2 ? call.getOperands().get(2) : null;
					if (nonNull(rexnode3)) {
						return ((String) evaluateRexNode(rexnode2, values)).indexOf(
								(String) evaluateRexNode(rexnode1, values), (Integer) evaluateRexNode(rexnode3, values));
					}
					return ((String) evaluateRexNode(rexnode2, values)).indexOf((String) evaluateRexNode(rexnode1, values));
				case "initcap":
					val = (String) evaluateRexNode(call.getOperands().get(0), values);
					return val.length() > 1 ? StringUtils.upperCase("" + val.charAt(0)) + val.substring(1)
							: val.length() == 1 ? StringUtils.upperCase("" + val.charAt(0)) : val;
				case "ascii":
					val = (String) evaluateRexNode(call.getOperands().get(0), values);
					return (int) val.charAt(0);
				case "charac":
					Integer asciicode = Integer.valueOf(String.valueOf(evaluateRexNode(call.getOperands().get(0), values)));
					return (char) (asciicode % 256);
				case "insertstr":
					String value1 = (String) evaluateRexNode(call.getOperands().get(0), values);
					Integer postoinsert = Integer
							.valueOf(String.valueOf(evaluateRexNode(call.getOperands().get(2), values)));
					Integer lengthtoinsert = Integer
							.valueOf(String.valueOf(evaluateRexNode(call.getOperands().get(3), values)));
					String value2 = (String) evaluateRexNode(call.getOperands().get(1), values);
					value2 = value2.substring(0, Math.min(lengthtoinsert, value2.length()));
					return value1.substring(0, Math.min(value1.length(), postoinsert)) + value2
							+ value1.substring(Math.min(value1.length(), postoinsert), value1.length());
				case "leftchars":
					value1 = (String) evaluateRexNode(call.getOperands().get(0), values);
					Integer lengthtoextract = Integer
							.valueOf(String.valueOf(evaluateRexNode(call.getOperands().get(1), values)));
					return value1.substring(0, Math.min(lengthtoextract, value1.length()));
				case "rightchars":
					value1 = (String) evaluateRexNode(call.getOperands().get(0), values);
					lengthtoextract = Integer.valueOf(String.valueOf(evaluateRexNode(call.getOperands().get(1), values)));
					return value1.substring(value1.length() - Math.min(lengthtoextract, value1.length()));
				case "reverse":
					value1 = (String) evaluateRexNode(call.getOperands().get(0), values);
					return StringUtils.reverse(value1);
				case "trim":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);
					rexnode3 = call.getOperands().get(2);
					String leadtrailboth = (String) evaluateRexNode(rexnode1, values);
					String str1 = (String) evaluateRexNode(rexnode2, values);
					String str2 = (String) evaluateRexNode(rexnode3, values);
					if ("leading".equalsIgnoreCase(leadtrailboth)) {
						while (str2.startsWith(str1)) {
							str2 = str2.substring(str1.length());
						}
						return str2;
					}
					if ("trailing".equalsIgnoreCase(leadtrailboth)) {
						while (str2.endsWith(str1)) {
							str2 = str2.substring(0, str2.length() - str1.length());
						}
						return str2;
					}
					while (str2.startsWith(str1)) {
						str2 = str2.substring(str1.length());
					}
					while (str2.endsWith(str1)) {
						str2 = str2.substring(0, str2.length() - str1.length());
					}
					return str2;

				case "ltrim":
					rexnode1 = call.getOperands().get(0);
					str1 = (String) evaluateRexNode(rexnode1, values);
					return StringUtils.stripStart(str1, null);
				case "rtrim":
					rexnode1 = call.getOperands().get(0);
					str1 = (String) evaluateRexNode(rexnode1, values);
					return StringUtils.stripEnd(str1, null);
				case "curdate":
					return date.format(new Date(System.currentTimeMillis()));
				case "curtime":
					return time.format(new Date(System.currentTimeMillis()));
				case "now":
					return dateExtract.format(new Date(System.currentTimeMillis()));
				case "year":
					rexnode1 = call.getOperands().get(0);
					str1 = (String) evaluateRexNode(rexnode1, values);
					Calendar calendar = new GregorianCalendar();
					try {
						calendar.setTime(dateExtract.parse(str1));
					} catch (Exception e) {
						log.error(DataSamudayaConstants.EMPTY, e);
					}
					return calendar.get(Calendar.YEAR);
				case "month":
					rexnode1 = call.getOperands().get(0);
					str1 = (String) evaluateRexNode(rexnode1, values);
					calendar = new GregorianCalendar();
					try {
						calendar.setTime(dateExtract.parse(str1));
					} catch (Exception e) {
						log.error(DataSamudayaConstants.EMPTY, e);
					}
					return calendar.get(Calendar.MONTH);
				case "day":
					rexnode1 = call.getOperands().get(0);
					str1 = (String) evaluateRexNode(rexnode1, values);
					calendar = new GregorianCalendar();
					try {
						calendar.setTime(dateExtract.parse(str1));
					} catch (Exception e) {
						log.error(DataSamudayaConstants.EMPTY, e);
					}
					return calendar.get(Calendar.DAY_OF_MONTH);
				case "case":
					List<RexNode> rexnodes = call.getOperands();
					for (int numnodes = 0;numnodes < rexnodes.size();numnodes += 2) {
						if (evaluateExpression(rexnodes.get(numnodes), values)) {
							return evaluateRexNode(rexnodes.get(numnodes + 1), values);
						} else if (numnodes + 2 == rexnodes.size() - 1) {
							return evaluateRexNode(rexnodes.get(numnodes + 2), values);
						}
					}
					return "";
				case "indexof":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);
					str1 = (String) evaluateRexNode(rexnode1, values);
					str2 = (String) evaluateRexNode(rexnode2, values);
					return StringUtils.indexOf(str1, str2);
				case "indexofstartpos":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);
					rexnode3 = call.getOperands().get(2);
					str1 = (String) evaluateRexNode(rexnode1, values);
					str2 = (String) evaluateRexNode(rexnode2, values);
					int startpos = (Integer) evaluateRexNode(rexnode3, values);
					return StringUtils.indexOf(str1, str2, startpos);
				case "indexofany":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);
					str1 = (String) evaluateRexNode(rexnode1, values);
					str2 = (String) evaluateRexNode(rexnode2, values);
					return StringUtils.indexOfAny(str1, str2);
				case "indexofanybut":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);
					str1 = (String) evaluateRexNode(rexnode1, values);
					str2 = (String) evaluateRexNode(rexnode2, values);
					return StringUtils.indexOfAnyBut(str1, str2);
				case "indexofdiff":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);
					str1 = (String) evaluateRexNode(rexnode1, values);
					str2 = (String) evaluateRexNode(rexnode2, values);
					return StringUtils.indexOfDifference(str1, str2);
				case "indexofignorecase":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);
					str1 = (String) evaluateRexNode(rexnode1, values);
					str2 = (String) evaluateRexNode(rexnode2, values);
					return StringUtils.indexOfIgnoreCase(str1, str2);
				case "indexofignorecasestartpos":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);
					rexnode3 = call.getOperands().get(2);
					str1 = (String) evaluateRexNode(rexnode1, values);
					str2 = (String) evaluateRexNode(rexnode2, values);
					startpos = (Integer) evaluateRexNode(rexnode3, values);
					return StringUtils.indexOfIgnoreCase(str1, str2, startpos);
				case "lastindexof":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);
					str1 = (String) evaluateRexNode(rexnode1, values);
					str2 = (String) evaluateRexNode(rexnode2, values);
					return StringUtils.lastIndexOf(str1, str2);
				case "lastindexofany":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);
					str1 = (String) evaluateRexNode(rexnode1, values);
					str2 = (String) evaluateRexNode(rexnode2, values);
					return StringUtils.lastIndexOfAny(str1, str2);
				case "lastindexofstartpos":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);
					rexnode3 = call.getOperands().get(2);
					str1 = (String) evaluateRexNode(rexnode1, values);
					str2 = (String) evaluateRexNode(rexnode2, values);
					startpos = (Integer) evaluateRexNode(rexnode3, values);
					return StringUtils.lastIndexOf(str1, str2, startpos);
				case "leftpad":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);
					str1 = (String) evaluateRexNode(rexnode1, values);
					int number = (Integer) evaluateRexNode(rexnode2, values);
					return StringUtils.leftPad(str1, number);
				case "leftpadstring":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);
					rexnode3 = call.getOperands().get(2);
					str1 = (String) evaluateRexNode(rexnode1, values);
					str2 = (String) evaluateRexNode(rexnode2, values);
					number = (Integer) evaluateRexNode(rexnode3, values);
					return StringUtils.leftPad(str1, number, str2);
				case "remove":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);					
					str1 = (String) evaluateRexNode(rexnode1, values);
					str2 = (String) evaluateRexNode(rexnode2, values);
					return StringUtils.remove(str1, str2);
				case "removeend":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);					
					str1 = (String) evaluateRexNode(rexnode1, values);
					str2 = (String) evaluateRexNode(rexnode2, values);
					return StringUtils.removeEnd(str1, str2);
				case "removeendignorecase":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);					
					str1 = (String) evaluateRexNode(rexnode1, values);
					str2 = (String) evaluateRexNode(rexnode2, values);
					return StringUtils.removeEndIgnoreCase(str1, str2);
				case "removeignorecase":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);					
					str1 = (String) evaluateRexNode(rexnode1, values);
					str2 = (String) evaluateRexNode(rexnode2, values);
					return StringUtils.removeIgnoreCase(str1, str2);
				case "removestart":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);					
					str1 = (String) evaluateRexNode(rexnode1, values);
					str2 = (String) evaluateRexNode(rexnode2, values);
					return StringUtils.removeStart(str1, str2);
				case "removestartignorecase":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);					
					str1 = (String) evaluateRexNode(rexnode1, values);
					str2 = (String) evaluateRexNode(rexnode2, values);
					return StringUtils.removeStartIgnoreCase(str1, str2);
				case "repeat":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);					
					str1 = (String) evaluateRexNode(rexnode1, values);
					number = (int) evaluateRexNode(rexnode2, values);
					return StringUtils.repeat(str1, number);
				case "repeatseparator":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);
					rexnode3 = call.getOperands().get(2);
					str1 = (String) evaluateRexNode(rexnode1, values);
					str2 = (String) evaluateRexNode(rexnode2, values);
					number = (int) evaluateRexNode(rexnode3, values);
					return StringUtils.repeat(str1, str2, number);
				case "chop":
					rexnode1 = call.getOperands().get(0);
					str1 = (String) evaluateRexNode(rexnode1, values);
					return StringUtils.chop(str1);
				case "getdigits":
					rexnode1 = call.getOperands().get(0);
					str1 = (String) evaluateRexNode(rexnode1, values);
					return StringUtils.getDigits(str1);
				case "rightpad":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);
					str1 = (String) evaluateRexNode(rexnode1, values);
					number = (Integer) evaluateRexNode(rexnode2, values);
					return StringUtils.rightPad(str1, number);
				case "rightpadstring":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);
					rexnode3 = call.getOperands().get(2);
					str1 = (String) evaluateRexNode(rexnode1, values);
					str2 = (String) evaluateRexNode(rexnode2, values);
					number = (Integer) evaluateRexNode(rexnode3, values);
					return StringUtils.rightPad(str1, number, str2);
				case "rotate":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);
					str1 = (String) evaluateRexNode(rexnode1, values);
					number = (Integer) evaluateRexNode(rexnode2, values);
					return StringUtils.rotate(str1, number);
				case "wrap":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);
					str1 = (String) evaluateRexNode(rexnode1, values);
					str2 = (String) evaluateRexNode(rexnode2, values);
					return StringUtils.wrap(str1, str2);
				case "wrapifmissing":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);
					str1 = (String) evaluateRexNode(rexnode1, values);
					str2 = (String) evaluateRexNode(rexnode2, values);
					return StringUtils.wrapIfMissing(str1, str2);
				case "unwrap":
					rexnode1 = call.getOperands().get(0);
					rexnode2 = call.getOperands().get(1);
					str1 = (String) evaluateRexNode(rexnode1, values);
					str2 = (String) evaluateRexNode(rexnode2, values);
					return StringUtils.unwrap(str1, str2);
				case "uncapitalize":
					rexnode1 = call.getOperands().get(0);
					str1 = (String) evaluateRexNode(rexnode1, values);
					return StringUtils.uncapitalize(str1);
			}
		} else if (node instanceof RexCall call && call.getOperator() instanceof SqlFloorFunction) {
			return evaluateFunctionsWithType(evaluateRexNode(call.getOperands().get(0), values), null,
					call.getOperator().getName().toLowerCase());
		} else if (node instanceof RexLiteral) {
			return getValue((RexLiteral) node, ((RexLiteral) node).getType().getSqlTypeName());
		} else {
			return getValueObject(node, values);
		}
		return null;
	}

	/**
	 * This function gets the greatest type in RexNode expression
	 * 
	 * @param rexNode
	 * @return greatest type
	 */
	public static SqlTypeName findGreatestType(RexNode rexNode) {
		SqlTypeName greatestType = null;

		// Traverse the tree and compare types
		switch (rexNode.getKind()) {
			case INPUT_REF:
			case LITERAL:
				greatestType = rexNode.getType().getSqlTypeName();
				break;

			case PLUS:
			case MINUS:
			case TIMES:
			// Add more cases for other node types as needed

		default:
				if (CollectionUtils.isEmpty(((RexCall) rexNode).getOperands())) {
					if (((RexCall) rexNode).getOperator() instanceof SqlFunction sqlfunc) {
						if (sqlfunc.getName().startsWith("currentisodate")) {
							return SqlTypeName.CHAR;
						} else if (sqlfunc.getName().startsWith("current_timemillis")) {
							return rexNode.getType().getSqlTypeName();
						} else if (sqlfunc.getName().startsWith("curdate")) {
							return rexNode.getType().getSqlTypeName();
						} else if (sqlfunc.getName().startsWith("curtime")) {
							return rexNode.getType().getSqlTypeName();
						} else if (sqlfunc.getName().startsWith("now")) {
							return rexNode.getType().getSqlTypeName();
						} else if (sqlfunc.getName().startsWith("pii")) {
							return rexNode.getType().getSqlTypeName();
						}
					}
				}
				// Traverse the children
				for (RexNode operand : ((RexCall) rexNode).getOperands()) {
					SqlTypeName operandType = findGreatestType(operand);

					// Compare types
					if (greatestType == null || operandType.compareTo(greatestType) > 0) {
						greatestType = operandType;
					}
				}
		}

		return greatestType;
	}

	/**
	 * The function generates zero for literal
	 * 
	 * @param sqlTypeName
	 * @return zero based on type
	 */
	public static Object generateZeroLiteral(SqlTypeName sqlTypeName) {
		switch (sqlTypeName) {
			case TINYINT:
			case SMALLINT:
			case INTEGER:
				return 0;
			case BIGINT:
				return 0L;

			case DECIMAL:
			case FLOAT:
				return 0.0;

			case REAL:
			case DOUBLE:
				return 0.0d;

			case CHAR:
			case VARCHAR:
				return "";

			// Add more cases for other data types as needed

			default:
				throw new UnsupportedOperationException("Unsupported SqlTypeName: " + sqlTypeName);
		}
	}

	/**
	 * The function returns the current tasks operated with the actor selection url
	 * or executes task
	 * 
	 * @param system
	 * @param obj
	 * @param jobidstageidjobstagemap
	 * @param hdfs
	 * @param inmemorycache
	 * @param jobidstageidtaskidcompletedmap
	 * @param hostport
	 * @param cluster
	 * @param teid
	 * @param actors
	 * @param blockspartitionfilterskipmap
	 * @return
	 */
	public static Task getAkkaActor(ActorSystem system, Object obj, Map<String, JobStage> jobidstageidjobstagemap,
			FileSystem hdfs, Cache inmemorycache, Map<String, Boolean> jobidstageidtaskidcompletedmap,
			String hostport, akka.cluster.sharding.typed.javadsl.ClusterSharding clustersharding, String teid,
			Map<String, EntityTypeKey> eref, Map<String, Map<RexNode, AtomicBoolean>> blockspartitionfilterskipmap,
			String shardid, ExecutorService fjpool) {
		try {
			if (obj instanceof GetTaskActor taskactor) {
				String jobid = taskactor.getTask().getJobid();
				String jobstageid = taskactor.getTask().getJobid() + taskactor.getTask().getStageid();
				JobStage js = jobidstageidjobstagemap.get(jobstageid);
				taskactor.getTask().setTeid(teid);
				if (js.getStage().tasks.get(0) instanceof CsvOptionsSQL cosql) {
					log.debug("Creating Actor for task {} using system {} " + taskactor.getTask() + system);
					EntityTypeKey<Command> entityKey = ProcessMapperByBlocksLocation
							.createTypeKey(jobstageid + taskactor.getTask().getTaskid());					
					if(taskactor.isTostartdummy()) {
						startDummyActor(system, clustersharding, entityKey, eref, hostport, shardid);
						return taskactor.getTask();
					} else {
						setShardLocation(system, shardid, entityKey, hostport);
						ActorRef<ShardingEnvelope<Command>> shardRegion = ClusterSharding.get(system).init(Entity
								.of(entityKey,
										ctx -> ProcessMapperByBlocksLocation.create(ctx.getEntityId(), hdfs, inmemorycache,
												jobidstageidtaskidcompletedmap, taskactor.getTask(),
												blockspartitionfilterskipmap, fjpool))
								.withAllocationStrategy(ExternalShardAllocationStrategy.create(system, entityKey.name())));
						eref.put(getEntityUrl(hostport, shardid, entityKey.name()), entityKey);
						taskactor.getTask().setActorselection(getEntityUrl(hostport, shardid, entityKey.name()));
						return taskactor.getTask();
					}
				} else if (js.getStage().tasks.get(0) instanceof ShuffleStage shuffle) {					
					EntityTypeKey<Command> entityKey = ProcessShuffle
							.createTypeKey(jobstageid + taskactor.getTask().getTaskid());
					log.debug("Creating Actor for task {} using system {}", taskactor.getTask(), system);
					if(taskactor.isTostartdummy()) {
						startDummyActor(system, clustersharding, entityKey, eref, hostport, shardid);
						return taskactor.getTask();
					} else {
						setShardLocation(system, shardid, entityKey, hostport);
						List<RecipientRef> childactors = new ArrayList<>();
						getChildActors(system, clustersharding, childactors, eref, taskactor.getChildtaskactors(), hostport, shardid);
						ActorRef<ShardingEnvelope<Command>> shardRegion = ClusterSharding.get(system).init(Entity
								.of(entityKey,
										ctx -> ProcessShuffle.create(ctx.getEntityId(), jobidstageidtaskidcompletedmap,
												taskactor.getTask(), childactors, fjpool))
								
								.withAllocationStrategy(ExternalShardAllocationStrategy.create(system, entityKey.name())));
						eref.put(getEntityUrl(hostport, shardid, entityKey.name()), entityKey);
						taskactor.getTask().setActorselection(getEntityUrl(hostport, shardid, entityKey.name()));
					return taskactor.getTask();
					}
				} else if (js.getStage().tasks.get(0) instanceof DistributedDistinct dd) {					
					log.debug("Creating Actor for task {} using system {}", taskactor.getTask(), system);
					EntityTypeKey<Command> entityKey = ProcessDistributedDistinct
							.createTypeKey(jobstageid + taskactor.getTask().getTaskid());					
					if(taskactor.isTostartdummy()) {
						startDummyActor(system, clustersharding, entityKey, eref, hostport, shardid);
						return taskactor.getTask();
					} else {
						setShardLocation(system, shardid, entityKey, hostport);
						List<RecipientRef> childactors = new ArrayList<>();
						getChildActors(system, clustersharding, childactors, eref, taskactor.getChildtaskactors(), hostport, shardid);
						ActorRef<ShardingEnvelope<Command>> shardRegion = ClusterSharding.get(system).init(Entity.of(entityKey,
								ctx -> ProcessDistributedDistinct.create(ctx.getEntityId(), jobidstageidtaskidcompletedmap,
										taskactor.getTask(), childactors, taskactor.getTerminatingparentcount(), fjpool))
								
								.withAllocationStrategy(ExternalShardAllocationStrategy.create(system, entityKey.name())));	
						eref.put(getEntityUrl(hostport, shardid, entityKey.name()), entityKey);
						taskactor.getTask().setActorselection(getEntityUrl(hostport, shardid, entityKey.name()));
					}
					return taskactor.getTask();
				} else if (js.getStage().tasks.get(0) instanceof UnionFunction
						|| js.getStage().tasks.get(0) instanceof IntersectionFunction) {					
					final Class<?> cls;
					if (js.getStage().tasks.get(0) instanceof UnionFunction) {						
						EntityTypeKey<Command> entityKey = ProcessUnion
								.createTypeKey(jobstageid + taskactor.getTask().getTaskid());						
						if(taskactor.isTostartdummy()) {
							startDummyActor(system, clustersharding, entityKey, eref, hostport, shardid);
							return taskactor.getTask();
						} else {
							setShardLocation(system, shardid, entityKey, hostport);
							List<RecipientRef> childactors = new ArrayList<>();
							getChildActors(system, clustersharding, childactors, eref, taskactor.getChildtaskactors(), hostport, shardid);
							ActorRef<ShardingEnvelope<Command>> shardRegion = ClusterSharding.get(system).init(Entity
									.of(entityKey,
											ctx -> ProcessUnion.create(ctx.getEntityId(), js, inmemorycache,
													jobidstageidtaskidcompletedmap, taskactor.getTask(), childactors,
													taskactor.getTerminatingparentcount(), fjpool))
									.withAllocationStrategy(
									ExternalShardAllocationStrategy.create(system, entityKey.name())));
							eref.put(getEntityUrl(hostport, shardid, entityKey.name()), entityKey);
							taskactor.getTask().setActorselection(getEntityUrl(hostport, shardid, entityKey.name()));
						}
					} else {
						EntityTypeKey<Command> entityKey = ProcessIntersection
								.createTypeKey(jobstageid + taskactor.getTask().getTaskid());
						if(taskactor.isTostartdummy()) {
							startDummyActor(system, clustersharding, entityKey, eref, hostport, shardid);
							return taskactor.getTask();
						} else {
							setShardLocation(system, shardid, entityKey, hostport);
							List<RecipientRef> childactors = new ArrayList<>();
							getChildActors(system, clustersharding, childactors, eref, taskactor.getChildtaskactors(), hostport, shardid);
							ActorRef<ShardingEnvelope<Command>> shardRegion = ClusterSharding.get(system).init(Entity
									.of(entityKey,
											ctx -> ProcessIntersection.create(ctx.getEntityId(), js, inmemorycache,
													jobidstageidtaskidcompletedmap, taskactor.getTask(), childactors,
													taskactor.getTerminatingparentcount(), fjpool))
									.withAllocationStrategy(
									ExternalShardAllocationStrategy.create(system, entityKey.name())));
							eref.put(getEntityUrl(hostport, shardid, entityKey.name()), entityKey);
							taskactor.getTask().setActorselection(getEntityUrl(hostport, shardid, entityKey.name()));
						}
					}
					log.debug("Creating Actor for task {} using system {}", taskactor.getTask(), system);
					return taskactor.getTask();
				} else if (js.getStage().tasks.get(0) instanceof ReduceByKeyFunction rbkf) {
					log.debug("Creating Actor for task {} using system {}", taskactor.getTask(), system);
					EntityTypeKey<Command> entityKey = ProcessReduce
							.createTypeKey(jobstageid + taskactor.getTask().getTaskid());					
					if(taskactor.isTostartdummy()) {
						startDummyActor(system, clustersharding, entityKey, eref, hostport, shardid);
						return taskactor.getTask();
					} else {
						setShardLocation(system, shardid, entityKey, hostport);
						List<RecipientRef> childactors = new ArrayList<>();
						getChildActors(system, clustersharding, childactors, eref, taskactor.getChildtaskactors(), hostport, shardid);
						ActorRef<ShardingEnvelope<Command>> shardRegion = ClusterSharding.get(system).init(Entity.of(entityKey,
								ctx -> ProcessReduce.create(ctx.getEntityId(), jobidstageidjobstagemap.get(jobstageid),
										hdfs, inmemorycache, jobidstageidtaskidcompletedmap, taskactor.getTask(),
										childactors, taskactor.getTerminatingparentcount(), fjpool))
								
								.withAllocationStrategy(ExternalShardAllocationStrategy.create(system, entityKey.name())));
						eref.put(getEntityUrl(hostport, shardid, entityKey.name()), entityKey);
						taskactor.getTask().setActorselection(getEntityUrl(hostport, shardid, entityKey.name()));
					}
					return taskactor.getTask();
				} else if (js.getStage().getTasks().get(0) instanceof Coalesce coalesce) {
					EntityTypeKey<Command> entityKey = ProcessCoalesce
							.createTypeKey(jobstageid + taskactor.getTask().getTaskid());					
					if(taskactor.isTostartdummy()) {
						startDummyActor(system, clustersharding, entityKey, eref, hostport, shardid);
						return taskactor.getTask();
					} else {
						setShardLocation(system, shardid, entityKey, hostport);
						if (CollectionUtils.isNotEmpty(taskactor.getChildtaskactors())) {
							List<RecipientRef> childactors = new ArrayList<>();
							getChildActors(system, clustersharding, childactors, eref, taskactor.getChildtaskactors(), hostport, shardid);
							ActorRef<ShardingEnvelope<Command>> shardRegion = ClusterSharding.get(system).init(Entity
									.of(entityKey,
											ctx -> ProcessCoalesce.create(ctx.getEntityId(), coalesce, childactors,
													taskactor.getTerminatingparentcount(), jobidstageidtaskidcompletedmap,
													inmemorycache, taskactor.getTask(), fjpool))
									.withAllocationStrategy(
									ExternalShardAllocationStrategy.create(system, entityKey.name())));
							eref.put(getEntityUrl(hostport, shardid, entityKey.name()), entityKey);
							log.debug("Creating Actor for task {} using system {}", taskactor.getTask(), system);
						} else {
							ActorRef<ShardingEnvelope<Command>> shardRegion = ClusterSharding.get(system).init(Entity
									.of(entityKey,
											ctx -> ProcessCoalesce.create(ctx.getEntityId(), coalesce, null,
													taskactor.getTerminatingparentcount(), jobidstageidtaskidcompletedmap,
													inmemorycache, taskactor.getTask(), fjpool))
									.withAllocationStrategy(
									ExternalShardAllocationStrategy.create(system, entityKey.name())));
							eref.put(getEntityUrl(hostport, shardid, entityKey.name()), entityKey);
						}
					}
					taskactor.getTask().setActorselection(getEntityUrl(hostport, shardid, entityKey.name()));
					return taskactor.getTask();
				} else if (js.getStage().getTasks().get(0) instanceof ProcessDistributedSort ds) {
					EntityTypeKey<Command> entityKey = ProcessDistributedSort
							.createTypeKey(jobstageid + taskactor.getTask().getTaskid());					
					if(taskactor.isTostartdummy()) {
						startDummyActor(system, clustersharding, entityKey, eref, hostport, shardid);
						return taskactor.getTask();
					} else {
						setShardLocation(system, shardid, entityKey, hostport);
						if (CollectionUtils.isNotEmpty(taskactor.getChildtaskactors())) {
							List<RecipientRef> childactors = new ArrayList<>();
							getChildActors(system, clustersharding, childactors, eref, taskactor.getChildtaskactors(), hostport, shardid);
							ActorRef<ShardingEnvelope<Command>> shardRegion = ClusterSharding.get(system).init(Entity
									.of(entityKey,
											ctx -> ProcessDistributedSort.create(ctx.getEntityId(),
													jobidstageidjobstagemap.get(jobstageid), inmemorycache,
													jobidstageidtaskidcompletedmap, taskactor.getTask(), childactors,
													taskactor.getTerminatingparentcount(), fjpool))
									.withAllocationStrategy(
									ExternalShardAllocationStrategy.create(system, entityKey.name())));
							eref.put(getEntityUrl(hostport, shardid, entityKey.name()), entityKey);
							log.debug("Creating Actor for task {} using system {}", taskactor.getTask(), system);
	
						} else {
							ActorRef<ShardingEnvelope<Command>> shardRegion = ClusterSharding.get(system).init(Entity
									.of(entityKey,
											ctx -> ProcessDistributedSort.create(ctx.getEntityId(),
													jobidstageidjobstagemap.get(jobstageid), inmemorycache,
													jobidstageidtaskidcompletedmap, taskactor.getTask(), null,
													taskactor.getTerminatingparentcount(), fjpool))
									.withAllocationStrategy(
									ExternalShardAllocationStrategy.create(system, entityKey.name())));
							eref.put(getEntityUrl(hostport, shardid, entityKey.name()), entityKey);
						}
					}
					taskactor.getTask().setActorselection(getEntityUrl(hostport, shardid, entityKey.name()));
					return taskactor.getTask();
				} else if (js.getStage().getTasks().get(0) instanceof com.github.datasamudaya.common.functions.Join join) {
					EntityTypeKey<Command> entityKey = ProcessInnerJoin
							.createTypeKey(jobstageid + taskactor.getTask().getTaskid());					
					if(taskactor.isTostartdummy()) {
						startDummyActor(system, clustersharding, entityKey, eref, hostport, shardid);
						return taskactor.getTask();
					} else {
						setShardLocation(system, shardid, entityKey, hostport);
						if (CollectionUtils.isNotEmpty(taskactor.getChildtaskactors())) {
							List<RecipientRef> childactors = new ArrayList<>();
							getChildActors(system, clustersharding, childactors, eref, taskactor.getChildtaskactors(), hostport, shardid);
							ActorRef<ShardingEnvelope<Command>> shardRegion = ClusterSharding.get(system).init(Entity
									.of(entityKey,
											ctx -> ProcessInnerJoin.create(ctx.getEntityId(), join, childactors,
													taskactor.getTerminatingparentcount(), jobidstageidtaskidcompletedmap,
													inmemorycache, taskactor.getTask(), fjpool))
									.withAllocationStrategy(
									ExternalShardAllocationStrategy.create(system, entityKey.name())));
							eref.put(getEntityUrl(hostport, shardid, entityKey.name()), entityKey);
						} else {
							ActorRef<ShardingEnvelope<Command>> shardRegion = ClusterSharding.get(system).init(Entity
									.of(entityKey,
											ctx -> ProcessInnerJoin.create(ctx.getEntityId(), join, null,
													taskactor.getTerminatingparentcount(), jobidstageidtaskidcompletedmap,
													inmemorycache, taskactor.getTask(), fjpool))
									.withAllocationStrategy(
									ExternalShardAllocationStrategy.create(system, entityKey.name())));
							eref.put(getEntityUrl(hostport, shardid, entityKey.name()), entityKey);
						}
					}
					taskactor.getTask().setActorselection(getEntityUrl(hostport, shardid, entityKey.name()));
					return taskactor.getTask();
				} else if (js.getStage().getTasks().get(0) instanceof RightJoin rightjoin) {
					EntityTypeKey<Command> entityKey = ProcessRightOuterJoin
							.createTypeKey(jobstageid + taskactor.getTask().getTaskid());					
					if(taskactor.isTostartdummy()) {
						startDummyActor(system, clustersharding, entityKey, eref, hostport, shardid);
						return taskactor.getTask();
					} else {
						setShardLocation(system, shardid, entityKey, hostport);
						if (CollectionUtils.isNotEmpty(taskactor.getChildtaskactors())) {
							List<RecipientRef> childactors = new ArrayList<>();
							getChildActors(system, clustersharding, childactors, eref, taskactor.getChildtaskactors(), hostport, shardid);
							ActorRef<ShardingEnvelope<Command>> shardRegion = ClusterSharding.get(system).init(Entity
									.of(entityKey,
											ctx -> ProcessRightOuterJoin.create(ctx.getEntityId(), rightjoin, childactors,
													taskactor.getTerminatingparentcount(), jobidstageidtaskidcompletedmap,
													inmemorycache, taskactor.getTask(), fjpool))
									.withAllocationStrategy(
									ExternalShardAllocationStrategy.create(system, entityKey.name())));
							eref.put(getEntityUrl(hostport, shardid, entityKey.name()), entityKey);
						} else {
							log.debug("Creating Actor for task {} using system {}", taskactor.getTask(), system);
							ActorRef<ShardingEnvelope<Command>> shardRegion = ClusterSharding.get(system).init(Entity
									.of(entityKey,
											ctx -> ProcessRightOuterJoin.create(ctx.getEntityId(), rightjoin, null,
													taskactor.getTerminatingparentcount(), jobidstageidtaskidcompletedmap,
													inmemorycache, taskactor.getTask(), fjpool))
									.withAllocationStrategy(
									ExternalShardAllocationStrategy.create(system, entityKey.name())));
							eref.put(getEntityUrl(hostport, shardid, entityKey.name()), entityKey);
						}
					}
					taskactor.getTask().setActorselection(getEntityUrl(hostport, shardid, entityKey.name()));
					return taskactor.getTask();
				} else if (js.getStage().getTasks().get(0) instanceof LeftJoin leftjoin) {
					EntityTypeKey<Command> entityKey = ProcessLeftOuterJoin
							.createTypeKey(jobstageid + taskactor.getTask().getTaskid());					
					if(taskactor.isTostartdummy()) {
						startDummyActor(system, clustersharding, entityKey, eref, hostport, shardid);
						return taskactor.getTask();
					} else {
						setShardLocation(system, shardid, entityKey, hostport);
						if (CollectionUtils.isNotEmpty(taskactor.getChildtaskactors())) {
							List<RecipientRef> childactors = new ArrayList<>();
							getChildActors(system, clustersharding, childactors, eref, taskactor.getChildtaskactors(), hostport, shardid);
							ActorRef<ShardingEnvelope<Command>> shardRegion = ClusterSharding.get(system).init(Entity
									.of(entityKey,
											ctx -> ProcessLeftOuterJoin.create(ctx.getEntityId(), leftjoin, childactors,
													taskactor.getTerminatingparentcount(), jobidstageidtaskidcompletedmap,
													inmemorycache, taskactor.getTask(), fjpool))
									.withAllocationStrategy(
									ExternalShardAllocationStrategy.create(system, entityKey.name())));
							eref.put(getEntityUrl(hostport, shardid, entityKey.name()), entityKey);
						} else {
							ActorRef<ShardingEnvelope<Command>> shardRegion = ClusterSharding.get(system).init(Entity
									.of(entityKey,
											ctx -> ProcessLeftOuterJoin.create(ctx.getEntityId(), leftjoin, null,
													taskactor.getTerminatingparentcount(), jobidstageidtaskidcompletedmap,
													inmemorycache, taskactor.getTask(), fjpool))
									.withAllocationStrategy(
									ExternalShardAllocationStrategy.create(system, entityKey.name())));
							eref.put(getEntityUrl(hostport, shardid, entityKey.name()), entityKey);
	
						}
					}
					taskactor.getTask().setActorselection(getEntityUrl(hostport, shardid, entityKey.name()));
					return taskactor.getTask();
				} else if (js.getStage().getTasks().get(0) instanceof FullOuterJoin) {
					EntityTypeKey<Command> entityKey = ProcessFullOuterJoin
							.createTypeKey(jobstageid + taskactor.getTask().getTaskid());					
					if(taskactor.isTostartdummy()) {
						startDummyActor(system, clustersharding, entityKey, eref, hostport, shardid);
						return taskactor.getTask();
					} else {
						setShardLocation(system, shardid, entityKey, hostport);
						if (CollectionUtils.isNotEmpty(taskactor.getChildtaskactors())) {
							List<RecipientRef> childactors = new ArrayList<>();
							getChildActors(system, clustersharding, childactors, eref, taskactor.getChildtaskactors(), hostport, shardid);
							ActorRef<ShardingEnvelope<Command>> shardRegion = ClusterSharding.get(system).init(Entity
									.of(entityKey,
											ctx -> ProcessFullOuterJoin.create(ctx.getEntityId(), childactors,
													taskactor.getTerminatingparentcount(), jobidstageidtaskidcompletedmap,
													inmemorycache, taskactor.getTask(), fjpool))
									.withAllocationStrategy(
									ExternalShardAllocationStrategy.create(system, entityKey.name())));
							eref.put(getEntityUrl(hostport, shardid, entityKey.name()), entityKey);
						} else {
							ActorRef<ShardingEnvelope<Command>> shardRegion = ClusterSharding.get(system).init(Entity
									.of(entityKey,
											ctx -> ProcessFullOuterJoin.create(ctx.getEntityId(), null,
													taskactor.getTerminatingparentcount(), jobidstageidtaskidcompletedmap,
													inmemorycache, taskactor.getTask(), fjpool))
									.withAllocationStrategy(
									ExternalShardAllocationStrategy.create(system, entityKey.name())));
							eref.put(getEntityUrl(hostport, shardid, entityKey.name()), entityKey);
	
						}
					taskactor.getTask().setActorselection(getEntityUrl(hostport, shardid, entityKey.name()));
					return taskactor.getTask();
					}
				} else {
					if(taskactor.isTostartdummy()) {
						EntityTypeKey<Command> entityKey = ProcessMapperByStream
								.createTypeKey(jobstageid + taskactor.getTask().getTaskid());						
						startDummyActor(system, clustersharding, entityKey, eref, hostport, shardid);
						return taskactor.getTask();
					} else {
						List<RecipientRef> childactors = new ArrayList<>();
						int totalfilepartspernode = Integer.parseInt(
								DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TOTALFILEPARTSPEREXEC,
										DataSamudayaConstants.TOTALFILEPARTSPEREXEC_DEFAULT));
						int indexfilepartpernode = 0;
						if (CollectionUtils.isNotEmpty(taskactor.getChildtaskactors())) {
							log.debug("Mapper By Stream ChildActors {}", taskactor.getChildtaskactors());
							getChildActors(system, clustersharding, childactors, eref, taskactor.getChildtaskactors(), hostport, shardid);
						}
						final Map<Integer, EntityRef> actorselections = new ConcurrentHashMap<>();
						if (CollectionUtils.isNotEmpty(taskactor.getTask().getShufflechildactors())) {
	
							log.debug("Mapper By Stream ShuffleChildActors {}",
									taskactor.getTask().getShufflechildactors());
							for (Task actortask : taskactor.getTask().getShufflechildactors()) {
								log.debug("Mapper By Stream Actors Selected {}", actortask.getActorselection());
								EntityTypeKey entityKeyactorsel = eref.get(actortask.getActorselection());
								if (isNull(entityKeyactorsel)) {
									String[] hpshardidentitykeyname = extractAkkaActorHostPortShardIdEntityTypeKey(actortask.getActorselection());
									entityKeyactorsel = EntityTypeKey.create(Command.class, hpshardidentitykeyname[2]);
									ClusterSharding.get(system).init(Entity.of(entityKeyactorsel, ctx -> Behaviors.same())
											.withAllocationStrategy(ExternalShardAllocationStrategy.create(system, entityKeyactorsel.name())));
									eref.put(getEntityUrl(hpshardidentitykeyname[0], hpshardidentitykeyname[1], entityKeyactorsel.name()), entityKeyactorsel);
								}
								EntityRef actorselection = ClusterSharding.get(system)
										.entityRefFor(eref.get(actortask.getActorselection()), entityKeyactorsel.name());
								for (int filepartcount = 0;filepartcount < totalfilepartspernode;filepartcount++) {
									actorselections.put(filepartcount + indexfilepartpernode, actorselection);
								}
								log.debug("Mapper By Stream FilePartitions Selected {}",
										taskactor.getTask().getFilepartitionsid());
								indexfilepartpernode += totalfilepartspernode;
							}
						}
						log.debug("Mapper By Stream Actor Creation Started {}...", childactors);
						log.debug("Creating Actor for task {} using system {}", taskactor.getTask(), system);
						EntityTypeKey<Command> entityKey = ProcessMapperByStream
								.createTypeKey(jobstageid + taskactor.getTask().getTaskid());
						setShardLocation(system, shardid, entityKey, hostport);
						ActorRef<ShardingEnvelope<Command>> shardRegion = ClusterSharding.get(system).init(Entity
								.of(entityKey,
										ctx -> ProcessMapperByStream.create(ctx.getEntityId(), js, hdfs, inmemorycache,
												jobidstageidtaskidcompletedmap, taskactor.getTask(), childactors,
												taskactor.getTask().getFilepartitionsid(), actorselections,
												taskactor.getTerminatingparentcount(), fjpool))
								.withAllocationStrategy(ExternalShardAllocationStrategy.create(system, entityKey.name())));
						eref.put(getEntityUrl(hostport, shardid, entityKey.name()), entityKey);
						log.debug("Mapper By Stream Actor Creation Ended...");
						taskactor.getTask().setActorselection(getEntityUrl(hostport, shardid, entityKey.name()));
						return taskactor.getTask();
					}
				}
			} else if (obj instanceof ExecuteTaskActor exectaskactor) {
				String jobstageid = exectaskactor.getTask().getJobid() + exectaskactor.getTask().getStageid();
				exectaskactor.getTask().setTeid(teid);
				int totalfilepartspernode = Integer
						.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TOTALFILEPARTSPEREXEC,
								DataSamudayaConstants.TOTALFILEPARTSPEREXEC_DEFAULT));
				int indexfilepartpernode = 0;
				log.debug("Executing Task " + exectaskactor.getTask() + " " + hostport);
				List<RecipientRef> childactors = new ArrayList<>();
				if (CollectionUtils.isNotEmpty(exectaskactor.getChildtaskactors())) {
					log.debug("Before Child Actors" + childactors);
					getChildActors(system, clustersharding, childactors, eref, exectaskactor.getChildtaskactors(), hostport, shardid);
					log.debug("Child Actors" + childactors);
				}
				Map<Integer, EntityRef> actorselections = null;
				if (CollectionUtils.isNotEmpty(exectaskactor.getTask().getShufflechildactors())) {
					actorselections = new ConcurrentHashMap<>();
					for (Task actortask : exectaskactor.getTask().getShufflechildactors()) {
						EntityTypeKey entityKeyactorsel = eref.get(actortask.getActorselection());
						if (isNull(entityKeyactorsel)) {
							String[] hpshardidentitykeyname = extractAkkaActorHostPortShardIdEntityTypeKey(
									actortask.getActorselection());
							entityKeyactorsel = EntityTypeKey.create(Command.class, hpshardidentitykeyname[2]);							
							ClusterSharding.get(system).init(Entity.of(entityKeyactorsel, ctx -> Behaviors.same())
									.withAllocationStrategy(ExternalShardAllocationStrategy.create(system, entityKeyactorsel.name())));
							eref.put(getEntityUrl(hpshardidentitykeyname[0], hpshardidentitykeyname[1],
									entityKeyactorsel.name()), entityKeyactorsel);
						}						
						EntityRef actorselection = ClusterSharding.get(system).entityRefFor(entityKeyactorsel,
								entityKeyactorsel.name());
						for (int filepartcount = 0;filepartcount < totalfilepartspernode;filepartcount++) {
							actorselections.put(filepartcount + indexfilepartpernode, actorselection);
						}
						indexfilepartpernode += totalfilepartspernode;
					}
				}
				log.debug("Before Child Actors" + actorselections + " " + eref);
				ProcessMapperByBlocksLocation.BlocksLocationRecord blr = new ProcessMapperByBlocksLocation.BlocksLocationRecord(
						(BlocksLocation) exectaskactor.getTask().getInput()[0],
						exectaskactor.getTask().getFilepartitionsid(), childactors, actorselections,
						jobidstageidjobstagemap.get(jobstageid));
				String[] hpshardidentitytypekey = extractAkkaActorHostPortShardIdEntityTypeKey(
						exectaskactor.getTask().getActorselection());
				final EntityRef mapreducetask = ClusterSharding.get(system)
						.entityRefFor(eref.get(exectaskactor.getTask().getActorselection()), hpshardidentitytypekey[2]);
				log.debug("Cluster Seed Nodes " + Cluster.get(system).state().members());
				log.debug("Map Reduce Task " + mapreducetask.getEntityId() + mapreducetask.getTypeKey() + blr);
				log.debug("Processing Blocks {} actors {}" + exectaskactor.getTask().getInput()
						+ exectaskactor.getTask().getActorselection());
				var path = Utils.getIntermediateInputStreamTask(exectaskactor.getTask());
				log.debug("Job Stage Completed Map" + jobidstageidtaskidcompletedmap);
				mapreducetask.tell(blr);
				while (isNull(jobidstageidtaskidcompletedmap.get(path)) || !jobidstageidtaskidcompletedmap.get(path)) {
					try {
						Thread.sleep(1000);
						log.debug("Job Stage Completed Map ..." + jobidstageidtaskidcompletedmap);
					} catch (Exception e) {
						log.error(DataSamudayaConstants.EMPTY, e);
					}
				}
				Task tasktoexecute = exectaskactor.getTask();
				log.debug("Task Executed {} with status {}" + tasktoexecute + jobidstageidtaskidcompletedmap);
				tasktoexecute.taskstatus = TaskStatus.COMPLETED;
				tasktoexecute.tasktype = TaskType.EXECUTEUSERTASK;
				return tasktoexecute;
			}
		} catch (Exception ex) {
			log.error("{}", ex);
			throw ex;
		}
		return null;
	}

	/**
	 * The method create dummy entity in current actor system cluster sharding
	 * @param entityTypeKey
	 * @param eref
	 */
	public static void startDummyActor(ActorSystem actorsystem, 
			ClusterSharding clustersharding, 
			EntityTypeKey entityTypeKey, 
			Map<String, EntityTypeKey> eref, String hostport, String shardid) {
		ClusterSharding.get(actorsystem).init(Entity.of(entityTypeKey, ctx -> Behaviors.same())
				.withAllocationStrategy(ExternalShardAllocationStrategy.create(actorsystem, entityTypeKey.name())));
		eref.put(getEntityUrl(hostport, shardid, entityTypeKey.name()), entityTypeKey);
	}
	
	/**
	 * THe method gathers child EntityRef objects
	 * 
	 * @param jobid
	 * @param system
	 * @param clustersharding
	 * @param childactors
	 * @param eref
	 * @param taskactor
	 */
	public static void getChildActors(ActorSystem actorsystem, ClusterSharding clustersharding, List<RecipientRef> childactors,
			Map<String, EntityTypeKey> eref, List<String> taskactors, String hostport, String shardid) {
		for (String actorselectionurl : taskactors) {
			EntityTypeKey entityKeyactorsel = eref.get(actorselectionurl);
			if (isNull(entityKeyactorsel)) {
				String[] hpshardidentitykeyname = extractAkkaActorHostPortShardIdEntityTypeKey(actorselectionurl);
				entityKeyactorsel = EntityTypeKey.create(Command.class, hpshardidentitykeyname[2]);
				ClusterSharding.get(actorsystem).init(Entity.of(entityKeyactorsel, ctx -> Behaviors.same())
						.withAllocationStrategy(ExternalShardAllocationStrategy.create(actorsystem, entityKeyactorsel.name())));
				eref.put(getEntityUrl(hpshardidentitykeyname[0], hpshardidentitykeyname[1], entityKeyactorsel.name()), entityKeyactorsel);
			}
			EntityRef actorselection = ClusterSharding.get(actorsystem).entityRefFor(entityKeyactorsel, entityKeyactorsel.name());
			childactors.add(actorselection);
		}
	}

	/**
	 * The function extracts host port of actor system shardid and entityid
	 * 
	 * @param entityurl
	 * @return entity url splitted in array
	 */
	public static String[] extractAkkaActorHostPortShardIdEntityTypeKey(String entityurl) {
		return entityurl.split(DataSamudayaConstants.FORWARD_SLASH);
	}

	/**
	 * The function returns entityurl in string format
	 * 
	 * @param hostport
	 * @param jobid
	 * @param entitykey
	 * @return entity url
	 */
	public static String getEntityUrl(String hostport, String shardid, String entitykey) {
		return hostport + DataSamudayaConstants.FORWARD_SLASH + shardid + DataSamudayaConstants.FORWARD_SLASH
				+ entitykey;
	}

	/**
	 * The method sets shard location for a given typekey with additional
	 * constraints
	 * 
	 * @param system
	 * @param shardid
	 * @param typeKey
	 * @param hostport
	 */
	public static void setShardLocation(ActorSystem system, String shardid, EntityTypeKey typeKey, String hostport) {
		log.debug("Set Shard Location Actor System " + system + " " + shardid + " " + typeKey + hostport);
		ExternalShardAllocationClient client = ExternalShardAllocation.get(system).getClient(typeKey.name());
		String[] hp = hostport.split(DataSamudayaConstants.COLON);
		client.setShardLocation(shardid, new Address(DataSamudayaConstants.AKKA_URL_SCHEME,
				DataSamudayaConstants.ACTORUSERNAME, hp[0], Integer.parseInt(hp[1])));
	}
	
	/**
	 * The function extracts join keys for left and right table for join condition
	 * @param condition
	 * @param leftFieldCount
	 * @return JoinKeysSQL object 
	 */
	public static JoinKeysSQL extractJoinKeys(RexNode condition, int leftFieldCount) {
		JoinKeysSQL joinKeys = new JoinKeysSQL();
        condition.accept(new RexVisitorImpl<Void>(true) {
            @Override
            public Void visitCall(RexCall call) {
                if (call.getKind() == SqlKind.EQUALS) {
                    // Check if the equality condition is between two input references
                    RexNode leftOperand = call.getOperands().get(0);
                    RexNode rightOperand = call.getOperands().get(1);

                    if (leftOperand instanceof RexInputRef && rightOperand instanceof RexInputRef) {
                        int leftIndex = ((RexInputRef) leftOperand).getIndex();
                        int rightIndex = ((RexInputRef) rightOperand).getIndex();

                        // Determine if the left and right indices belong to the left and right inputs
                        if (leftIndex < leftFieldCount && rightIndex >= leftFieldCount) {
                            // Left key is from the left input, right key is from the right input
                            joinKeys.getLeftKeys().add(leftIndex);
                            joinKeys.getRightKeys().add(rightIndex - leftFieldCount);
                        } else if (rightIndex < leftFieldCount && leftIndex >= leftFieldCount) {
                            // Left key is from the right input, right key is from the left input
                            joinKeys.getLeftKeys().add(rightIndex);
                            joinKeys.getRightKeys().add(leftIndex - leftFieldCount);
                        }
                    }
                }
                return super.visitCall(call);
            }
        });        
        return joinKeys;
    }
	
	/**
	 * The function returns value of join keys from object array
	 * @param objarr
	 * @param joinkey
	 * @return the value of join keys
	 */
	public static Object[] extractMapKeysFromJoinKeys(Object[] objarr, List<Integer> joinkey) {		
		Object[] joinKeysArray = new Object[joinkey.size()];
		for (int i = 0; i < joinkey.size(); i++) {
			joinKeysArray[i] = objarr[joinkey.get(i)];
		}
		return joinKeysArray;
	}
	
	/**
	 * Merges two object array in to single
	 * @param <T>
	 * @param a
	 * @param b
	 * @return merged object
	 */
	public static <T> Object[] concatenate(T[] a, T[] b)
	{
		return Stream.of(a, b)
				.flatMap(Stream::of)
				.toArray();
	}
	
	/**
	 * The method which traverses the optimized plan
	 * @param relNode
	 * @param depth
	 */
	public static void traverseRelNode(RelNode relNode, int depth, PrintWriter out) {
		// Print information about the current node
		printNodeInfo(relNode, depth, out);

		// Traverse child nodes
		List<RelNode> childNodes = relNode.getInputs();
		for (RelNode child : childNodes) {
			traverseRelNode(child, depth + 1, out);
		}
	}
	
	/**
	 * Prints the RelNode information with indent
	 * @param relNode
	 * @param depth
	 * @param out
	 */
	private static void printNodeInfo(RelNode relNode, int depth, PrintWriter out) {
		// Print information about the current node
		out.println(getIndent(depth) + "Node ID: " + relNode.getId());
		out.println(getIndent(depth) + "Node Description: " + getDescription(relNode));
	}
	
	/**
	 * The function provides the indented plan based on depth
	 * @param depth
	 * @return indented plan based on depth
	 */
	private static String getIndent(int depth) {
		// Create an indentation string based on the depth
		StringBuilder indent = new StringBuilder();
		for (int i = 0;i < depth;i++) {
			indent.append("  ");
		}
		return indent.toString();
	}

	/**
	 * The function returns the description of RelNode
	 * @param relNode
	 * @return description of relnode
	 */
	private static String getDescription(RelNode relNode) {
		return relNode.toString();
	}
	
}
