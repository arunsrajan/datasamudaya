/*
 * Copyright 2021 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.datasamudaya.stream;

import java.util.Arrays;
import java.util.function.IntUnaryOperator;
import java.util.function.ToIntFunction;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.Test;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.functions.AggregateFunction;
import com.github.datasamudaya.common.functions.AggregateReduceFunction;
import com.github.datasamudaya.common.functions.CalculateCount;
import com.github.datasamudaya.common.functions.CoalesceFunction;
import com.github.datasamudaya.common.functions.CountByKeyFunction;
import com.github.datasamudaya.common.functions.CountByValueFunction;
import com.github.datasamudaya.common.functions.DoubleFlatMapFunction;
import com.github.datasamudaya.common.functions.FlatMapFunction;
import com.github.datasamudaya.common.functions.FoldByKey;
import com.github.datasamudaya.common.functions.GroupByKeyFunction;
import com.github.datasamudaya.common.functions.IntersectionFunction;
import com.github.datasamudaya.common.functions.JoinPredicate;
import com.github.datasamudaya.common.functions.LeftOuterJoinPredicate;
import com.github.datasamudaya.common.functions.LongFlatMapFunction;
import com.github.datasamudaya.common.functions.MapFunction;
import com.github.datasamudaya.common.functions.MapToPairFunction;
import com.github.datasamudaya.common.functions.PeekConsumer;
import com.github.datasamudaya.common.functions.PredicateSerializable;
import com.github.datasamudaya.common.functions.ReduceByKeyFunction;
import com.github.datasamudaya.common.functions.RightOuterJoinPredicate;
import com.github.datasamudaya.common.functions.SortedComparator;
import com.github.datasamudaya.common.functions.TupleFlatMapFunction;
import com.github.datasamudaya.common.functions.UnionFunction;
import com.github.datasamudaya.stream.CsvOptions;
import com.github.datasamudaya.stream.PipelineIntStreamCollect;
import com.github.datasamudaya.stream.PipelineUtils;
import com.github.datasamudaya.stream.SampleSupplierInteger;
import com.github.datasamudaya.stream.SampleSupplierPartition;

import junit.framework.TestCase;

public class StreamPipelineUtilsTest extends TestCase {

	@Test
	public void testPredicateSerializable() {

		PredicateSerializable<String> predicateSerializable = value -> "value".equals(value);
		Object task = predicateSerializable;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.PREDICATESERIALIZABLE, printableTask);
	}

	@Test
	public void testMapPairFunction() {

		MapToPairFunction<String, Tuple2<String, String>> mapPairFunction = value -> new Tuple2<String, String>(value, value);
		Object task = mapPairFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.MAPPAIRFUNCTION, printableTask);
	}


	@Test
	public void testMapFunction() {
		MapFunction<String, String> mapFunction = value -> value + "100";
		Object task = mapFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.MAPFUNCTION, printableTask);
	}

	@Test
	public void testReduceFunction() {

		ReduceByKeyFunction<String> reduceFunction = (value1, value2) -> value1 + " " + value2;
		Object task = reduceFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.REDUCEBYKEYFUNCTION, printableTask);
	}

	@Test
	public void testFlatMapFunction() {

		FlatMapFunction<String, String> flatMapFunction = val -> Arrays.asList(val).stream();
		Object task = flatMapFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.FLATMAPFUNCTION, printableTask);
	}


	@Test
	public void testJoinTuplePredicate() {

		JoinPredicate<String, String> joinPredicate = (val1, val2) -> val1.equals(val2);
		Object task = joinPredicate;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.JOINTUPLEPREDICATE, printableTask);
	}

	@Test
	public void testLeftOuterJoinPredicate() {

		LeftOuterJoinPredicate<String, String> joinPredicate = (val1, val2) -> val1.equals(val2);
		Object task = joinPredicate;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.LEFTOUTERJOINTUPLEPREDICATE, printableTask);
	}


	@Test
	public void testRightOuterJoinPredicate() {

		RightOuterJoinPredicate<String, String> joinPredicate = (val1, val2) -> val1.equals(val2);
		Object task = joinPredicate;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.RIGHTOUTERJOINTUPLEPREDICATE, printableTask);
	}


	@Test
	public void testGroupByKeyFunction() {

		GroupByKeyFunction groupByKeyFunction = new GroupByKeyFunction();
		Object task = groupByKeyFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.GROUPBYKEYFUNCTION, printableTask);
	}


	@Test
	public void testAggregateReduceFunction() {

		AggregateReduceFunction<String, String, String> aggregateReduceFunction = (val1, val2) -> val1 + val2;
		Object task = aggregateReduceFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.AGGREGATEREDUCEFUNCTION, printableTask);
	}


	@Test
	public void testAggregateFunction() {

		AggregateFunction<String, String, String> aggregateFunction = (val1, val2) -> val1 + val2;
		Object task = aggregateFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.AGGREGATEFUNCTION, printableTask);
	}

	@Test
	public void testSampleSupplierInteger() {

		SampleSupplierInteger sampleSupplier = new SampleSupplierInteger(100);
		Object task = sampleSupplier;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.SAMPLESUPPLIERINTEGER, printableTask);
	}

	@Test
	public void testSampleSupplierPartition() {

		SampleSupplierPartition sampleSupplier = new SampleSupplierPartition(100);
		Object task = sampleSupplier;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.SAMPLESUPPLIERPARTITION, printableTask);
	}


	@Test
	public void testUnionFunction() {

		UnionFunction unionFunction = new UnionFunction();
		Object task = unionFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.UNIONFUNCTION, printableTask);
	}

	@Test
	public void testIntersectionFunction() {

		IntersectionFunction intersectionFunction = new IntersectionFunction();
		Object task = intersectionFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.INTERSECTIONFUNCTION, printableTask);
	}

	@Test
	public void testTupleFlatMapFunction() {

		TupleFlatMapFunction<String, Tuple2<String, String>> tupleFlatMapFunction = val -> Arrays.asList(new Tuple2<String, String>(val, val));
		Object task = tupleFlatMapFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.PAIRFLATMAPFUNCTION, printableTask);
	}


	@Test
	public void testLongFlatMapFunction() {

		LongFlatMapFunction<String> longFlatMapFunction = val -> Arrays.asList(Long.parseLong(val)).stream();
		Object task = longFlatMapFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.LONGFLATMAPFUNCTION, printableTask);
	}

	@Test
	public void testDoubleFlatMapFunction() {

		DoubleFlatMapFunction<String> doubleFlatMapFunction = val -> Arrays.asList(Double.parseDouble(val)).stream();
		Object task = doubleFlatMapFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.DOUBLEFLATMAPFUNCTION, printableTask);
	}


	@Test
	public void testCoalesceFunction() {

		CoalesceFunction<String> coalesceFunction = (val1, val2) -> val1 + val2;
		Object task = coalesceFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.COALESCEFUNCTION, printableTask);
	}

	@Test
	public void testCsvOptions() {

		CsvOptions csvOptions = new CsvOptions(new String[]{"Month", "DayOfMonth"});
		Object task = csvOptions;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.CSVOPTIONS, printableTask);
	}


	@Test
	public void testPeekConsumer() {

		PeekConsumer<String> peekConsumer = System.out::println;
		Object task = peekConsumer;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.PEEKCONSUMER, printableTask);
	}


	@Test
	public void testSorted() {

		SortedComparator<String> sorted = (Val1, val2) -> Val1.compareTo(val2);
		Object task = sorted;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.SORTEDCOMPARATOR, printableTask);
	}


	@Test
	public void testCalculateCount() {

		CalculateCount calculateCount = new CalculateCount();
		Object task = calculateCount;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.CALCULATECOUNT, printableTask);
	}

	@Test
	public void testMapToInt() {

		ToIntFunction<String> toIntFunction = val -> Integer.parseInt(val);
		Object task = toIntFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.MAPTOINT, printableTask);
	}


	@Test
	public void testPipelineIntStreamCollect() {

		PipelineIntStreamCollect pipelineIntStreamCollect = new PipelineIntStreamCollect(null, null, null);
		Object task = pipelineIntStreamCollect;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.PIPELINEINTSTREAMCOLLECT, printableTask);
	}


	@Test
	public void testIntUnaryOperator() {

		IntUnaryOperator intUnaryOperator = val -> val++;
		Object task = intUnaryOperator;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.INTUNARYOPERATOR, printableTask);
	}


	@Test
	public void testCountByKeyFunction() {

		CountByKeyFunction countByKeyFunction = new CountByKeyFunction();
		Object task = countByKeyFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.COUNTBYKEYFUNCTION, printableTask);
	}

	@Test
	public void testCountByValueFunction() {

		CountByValueFunction countByValueFunction = new CountByValueFunction();
		Object task = countByValueFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.COUNTBYVALUEFUNCTION, printableTask);
	}


	@Test
	public void testFoldByKeyFunction() {

		FoldByKey foldByKeyFunction = new FoldByKey(100l, (val1, val2) -> val1, true);
		Object task = foldByKeyFunction;
		String printableTask = PipelineUtils.getFunctions(task);
		assertEquals(DataSamudayaConstants.FOLDBYKEY, printableTask);
	}
}
