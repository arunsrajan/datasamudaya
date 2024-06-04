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

import java.util.ArrayList;
import java.util.List;
import java.util.function.IntUnaryOperator;
import java.util.function.ToIntFunction;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.Dummy;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.functions.AggregateFunction;
import com.github.datasamudaya.common.functions.AggregateReduceFunction;
import com.github.datasamudaya.common.functions.CalculateCount;
import com.github.datasamudaya.common.functions.Coalesce;
import com.github.datasamudaya.common.functions.CoalesceFunction;
import com.github.datasamudaya.common.functions.CountByKeyFunction;
import com.github.datasamudaya.common.functions.CountByValueFunction;
import com.github.datasamudaya.common.functions.Distinct;
import com.github.datasamudaya.common.functions.DistributedDistinct;
import com.github.datasamudaya.common.functions.DistributedSort;
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
import com.github.datasamudaya.common.functions.MapValuesFunction;
import com.github.datasamudaya.common.functions.Max;
import com.github.datasamudaya.common.functions.Min;
import com.github.datasamudaya.common.functions.PeekConsumer;
import com.github.datasamudaya.common.functions.PredicateSerializable;
import com.github.datasamudaya.common.functions.ReduceByKeyFunction;
import com.github.datasamudaya.common.functions.ReduceByKeyFunctionValues;
import com.github.datasamudaya.common.functions.ReduceFunction;
import com.github.datasamudaya.common.functions.RightOuterJoinPredicate;
import com.github.datasamudaya.common.functions.ShuffleStage;
import com.github.datasamudaya.common.functions.SortedComparator;
import com.github.datasamudaya.common.functions.StandardDeviation;
import com.github.datasamudaya.common.functions.Sum;
import com.github.datasamudaya.common.functions.SummaryStatistics;
import com.github.datasamudaya.common.functions.TupleFlatMapFunction;
import com.github.datasamudaya.common.functions.UnionFunction;

/**
 * 
 * @author arun
 * The class MassiveDataPipelineUtils is helper class for Pipeline.
 */
public class PipelineUtils {
	private PipelineUtils() {
	}

	/**
	 * The function returns the function name based on the type of 
	 * function provided as parameter to the getFunctions. 
	 * @param task
	 * @return function name.
	 */
	public static String getFunctions(Object obj) {
		if (obj instanceof PredicateSerializable) {
			return DataSamudayaConstants.PREDICATESERIALIZABLE;
		} else if (obj instanceof MapToPairFunction) {
			return DataSamudayaConstants.MAPPAIRFUNCTION;
		} else if (obj instanceof MapFunction) {
			return DataSamudayaConstants.MAPFUNCTION;
		} else if (obj instanceof ReduceFunction) {
			return DataSamudayaConstants.REDUCEFUNCTION;
		} else if (obj instanceof ReduceByKeyFunction) {
			return DataSamudayaConstants.REDUCEBYKEYFUNCTION;
		} else if (obj instanceof FlatMapFunction) {
			return DataSamudayaConstants.FLATMAPFUNCTION;
		} else if (obj instanceof JoinPredicate) {
			return DataSamudayaConstants.JOINTUPLEPREDICATE;
		} else if (obj instanceof LeftOuterJoinPredicate) {
			return DataSamudayaConstants.LEFTOUTERJOINTUPLEPREDICATE;
		} else if (obj instanceof RightOuterJoinPredicate) {
			return DataSamudayaConstants.RIGHTOUTERJOINTUPLEPREDICATE;
		} else if (obj instanceof GroupByKeyFunction) {
			return DataSamudayaConstants.GROUPBYKEYFUNCTION;
		} else if (obj instanceof AggregateReduceFunction) {
			return DataSamudayaConstants.AGGREGATEREDUCEFUNCTION;
		} else if (obj instanceof AggregateFunction) {
			return DataSamudayaConstants.AGGREGATEFUNCTION;
		} else if (obj instanceof SampleSupplierInteger) {
			return DataSamudayaConstants.SAMPLESUPPLIERINTEGER;
		} else if (obj instanceof SampleSupplierPartition) {
			return DataSamudayaConstants.SAMPLESUPPLIERPARTITION;
		} else if (obj instanceof UnionFunction) {
			return DataSamudayaConstants.UNIONFUNCTION;
		} else if (obj instanceof IntersectionFunction) {
			return DataSamudayaConstants.INTERSECTIONFUNCTION;
		} else if (obj instanceof TupleFlatMapFunction) {
			return DataSamudayaConstants.PAIRFLATMAPFUNCTION;
		} else if (obj instanceof LongFlatMapFunction) {
			return DataSamudayaConstants.LONGFLATMAPFUNCTION;
		} else if (obj instanceof DoubleFlatMapFunction) {
			return DataSamudayaConstants.DOUBLEFLATMAPFUNCTION;
		} else if (obj instanceof CoalesceFunction) {
			return DataSamudayaConstants.COALESCEFUNCTION;
		} else if (obj instanceof CsvOptions) {
			return DataSamudayaConstants.CSVOPTIONS;
		} else if (obj instanceof PeekConsumer) {
			return DataSamudayaConstants.PEEKCONSUMER;
		} else if (obj instanceof SortedComparator) {
			return DataSamudayaConstants.SORTEDCOMPARATOR;
		} else if (obj instanceof CalculateCount) {
			return DataSamudayaConstants.CALCULATECOUNT;
		} else if (obj instanceof ToIntFunction) {
			return DataSamudayaConstants.MAPTOINT;
		} else if (obj instanceof PipelineIntStreamCollect) {
			return DataSamudayaConstants.PIPELINEINTSTREAMCOLLECT;
		} else if (obj instanceof IntUnaryOperator) {
			return DataSamudayaConstants.INTUNARYOPERATOR;
		} else if (obj instanceof CountByKeyFunction) {
			return DataSamudayaConstants.COUNTBYKEYFUNCTION;
		} else if (obj instanceof CountByValueFunction) {
			return DataSamudayaConstants.COUNTBYVALUEFUNCTION;
		} else if (obj instanceof FoldByKey) {
			return DataSamudayaConstants.FOLDBYKEY;
		} else if (obj instanceof Json) {
			return DataSamudayaConstants.JSON;
		} else if (obj instanceof Coalesce) {
			return DataSamudayaConstants.COALESCE;
		} else if (obj instanceof SummaryStatistics) {
			return DataSamudayaConstants.SUMMARYSTATISTICS;
		} else if (obj instanceof Sum) {
			return DataSamudayaConstants.SUM;
		} else if (obj instanceof Max) {
			return DataSamudayaConstants.MAX;
		} else if (obj instanceof Min) {
			return DataSamudayaConstants.MIN;
		} else if (obj instanceof StandardDeviation) {
			return DataSamudayaConstants.STANDARDDEVIATION;
		} else if (obj instanceof MapValuesFunction) {
			return DataSamudayaConstants.MAPVALUESFUNCTION;
		} else if (obj instanceof ReduceByKeyFunctionValues) {
			return DataSamudayaConstants.REDUCEFUNCTIONVALUES;
		} else if (obj instanceof Dummy) {
			return DataSamudayaConstants.DUMMY;
		} else if (obj instanceof Distinct) {
			return DataSamudayaConstants.DISTINCT;
		} else if (obj instanceof ShuffleStage) {
			return DataSamudayaConstants.SHUFFLE;
		} else if (obj instanceof DistributedSort) {
			return DataSamudayaConstants.DISTRIBUTEDSORT;
		} else if (obj instanceof DistributedDistinct) {
			return DataSamudayaConstants.DISTRIBUTEDDISTINCT;
		}
		return DataSamudayaConstants.EMPTY;
	}

	/**
	 * The function returns the list of function name in list.
	 * @param tasks
	 * @return list of functions name.
	 */
	public static List<String> getFunctions(List<Task> tasks) {
		var functions = new ArrayList<String>();
		for (var task : tasks) {
			functions.add(PipelineUtils.getFunctions(task));
		}
		return functions;
	}
}
