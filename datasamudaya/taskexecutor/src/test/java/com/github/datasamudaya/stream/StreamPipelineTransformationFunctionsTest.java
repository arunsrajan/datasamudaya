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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.function.ToIntFunction;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.Test;

import com.github.datasamudaya.common.DAGEdge;
import com.github.datasamudaya.common.Job;
import com.github.datasamudaya.common.JobMetrics;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.functions.Distinct;
import com.github.datasamudaya.common.functions.IntersectionFunction;
import com.github.datasamudaya.common.functions.KeyByFunction;
import com.github.datasamudaya.common.functions.LeftOuterJoinPredicate;
import com.github.datasamudaya.common.functions.MapFunction;
import com.github.datasamudaya.common.functions.MapToPairFunction;
import com.github.datasamudaya.common.functions.PeekConsumer;
import com.github.datasamudaya.common.functions.PredicateSerializable;
import com.github.datasamudaya.common.functions.RightOuterJoinPredicate;
import com.github.datasamudaya.common.functions.SToIntFunction;
import com.github.datasamudaya.common.functions.SortedComparator;
import com.github.datasamudaya.common.functions.UnionFunction;

public class StreamPipelineTransformationFunctionsTest extends StreamPipelineBaseTestCommon {
	PipelineConfig pipelineconfig = new PipelineConfig();

	@Test
	public void testMassiveDataPipelineMap() throws Exception {
		StreamPipeline<String> mdp = new StreamPipeline<String>();
		MapFunction<String, String> mapfunction = data -> data;
		StreamPipeline<String> map = mdp.map(mapfunction);
		assertTrue(map.parents.get(0) == mdp);
		assertTrue(mdp.childs.get(0) == map);
		assertTrue(map.task == mapfunction);
		assertTrue(map.root == mdp);
	}

	@Test
	public void testMassiveDataPipelineMapFormDAGAbsFunc() throws Exception {
		StreamPipeline<String> mdp = new StreamPipeline<String>();
		MapFunction<String, String> mapfunction = data -> data;
		StreamPipeline<String> mdpchild = mdp.map(mapfunction);
		mdp.graph.addVertex(mdp);
		mdp.formDAGAbstractFunction(mdp, mdp.childs);
		assertEquals(2, mdp.graph.vertexSet().size());
		assertEquals(1, mdp.graph.edgeSet().size());
		assertTrue(mdp.graph.vertexSet().contains(mdp));
		assertTrue(mdp.graph.vertexSet().contains(mdpchild));
		DAGEdge dagedge = mdp.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdp);
		assertTrue(dagedge.getTarget() == mdpchild);
	}

	@Test
	public void testMassiveDataPipelineFilter() throws Exception {
		StreamPipeline<String> mdp = new StreamPipeline<String>();
		PredicateSerializable<String> filterfunction = data -> data.equals(data);
		StreamPipeline<String> filter = mdp.filter(filterfunction);
		assertTrue(filter.parents.get(0) == mdp);
		assertTrue(mdp.childs.get(0) == filter);
		assertTrue(filter.task == filterfunction);
		assertTrue(filter.root == mdp);
	}

	@Test
	public void testMassiveDataPipelineFilterFormDAGAbsFunc() throws Exception {
		StreamPipeline<String> mdp = new StreamPipeline<String>();
		PredicateSerializable<String> filterfunction = data -> data.equals(data);
		StreamPipeline<String> mdpchild = mdp.filter(filterfunction);
		mdp.graph.addVertex(mdp);
		mdp.formDAGAbstractFunction(mdp, mdp.childs);
		assertEquals(2, mdp.graph.vertexSet().size());
		assertEquals(1, mdp.graph.edgeSet().size());
		assertTrue(mdp.graph.vertexSet().contains(mdp));
		assertTrue(mdp.graph.vertexSet().contains(mdpchild));
		DAGEdge dagedge = mdp.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdp);
		assertTrue(dagedge.getTarget() == mdpchild);
	}

	@Test
	public void testMassiveDataPipelinePeekConsumer() throws Exception {
		StreamPipeline<String> mdp = new StreamPipeline<String>();
		PeekConsumer<String> peekconsumer = System.out::println;
		StreamPipeline<String> peek = mdp.peek(peekconsumer);
		assertTrue(peek.parents.get(0) == mdp);
		assertTrue(mdp.childs.get(0) == peek);
		assertTrue(peek.task == peekconsumer);
		assertTrue(peek.root == mdp);
	}

	@Test
	public void testMassiveDataPipelinePeekConsumerFormDAGAbsFunc() throws Exception {
		StreamPipeline<String> mdp = new StreamPipeline<String>();
		PeekConsumer<String> peekconsumer = System.out::println;
		StreamPipeline<String> mdpchild = mdp.peek(peekconsumer);
		mdp.graph.addVertex(mdp);
		mdp.formDAGAbstractFunction(mdp, mdp.childs);
		assertEquals(2, mdp.graph.vertexSet().size());
		assertEquals(1, mdp.graph.edgeSet().size());
		assertTrue(mdp.graph.vertexSet().contains(mdp));
		assertTrue(mdp.graph.vertexSet().contains(mdpchild));
		DAGEdge dagedge = mdp.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdp);
		assertTrue(dagedge.getTarget() == mdpchild);
	}

	@Test
	public void testMassiveDataPipelineUnion() throws Exception {
		StreamPipeline<String> mdpunion1 = new StreamPipeline<String>();
		StreamPipeline<String> mdpunion2 = new StreamPipeline<String>();
		StreamPipeline<String> union = mdpunion1.union(mdpunion2);
		assertTrue(union.parents.get(0) == mdpunion1);
		assertTrue(mdpunion1.childs.get(0) == union);
		assertTrue(union.parents.get(1) == mdpunion2);
		assertTrue(mdpunion2.childs.get(0) == union);
		assertTrue(union.task instanceof UnionFunction);
	}

	@Test
	public void testMassiveDataPipelineUnionFormDAGAbsFunc() throws Exception {
		StreamPipeline<String> mdpleft = new StreamPipeline<String>();
		StreamPipeline<String> mdpright = new StreamPipeline<String>();
		StreamPipeline<String> mdpchild = mdpleft.union(mdpright);
		mdpleft.graph.addVertex(mdpleft);
		mdpleft.formDAGAbstractFunction(mdpleft, mdpleft.childs);
		assertEquals(2, mdpleft.graph.vertexSet().size());
		assertEquals(1, mdpleft.graph.edgeSet().size());
		assertTrue(mdpleft.graph.vertexSet().contains(mdpleft));
		assertTrue(mdpleft.graph.vertexSet().contains(mdpchild));
		DAGEdge dagedge = mdpleft.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdpleft);
		assertTrue(dagedge.getTarget() == mdpchild);

		mdpright.graph.addVertex(mdpright);
		mdpright.formDAGAbstractFunction(mdpright, mdpright.childs);
		assertEquals(2, mdpright.graph.vertexSet().size());
		assertEquals(1, mdpright.graph.edgeSet().size());
		assertTrue(mdpright.graph.vertexSet().contains(mdpright));
		assertTrue(mdpright.graph.vertexSet().contains(mdpchild));
		dagedge = mdpright.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdpright);
		assertTrue(dagedge.getTarget() == mdpchild);
	}

	@Test
	public void testMassiveDataPipelineIntersection() throws Exception {
		StreamPipeline<String> mdpintersection1 = new StreamPipeline<String>();
		StreamPipeline<String> mdpintersection2 = new StreamPipeline<String>();
		StreamPipeline<String> intersection = mdpintersection1.intersection(mdpintersection2);
		assertTrue(intersection.parents.get(0) == mdpintersection1);
		assertTrue(mdpintersection1.childs.get(0) == intersection);
		assertTrue(intersection.parents.get(1) == mdpintersection2);
		assertTrue(mdpintersection2.childs.get(0) == intersection);
		assertTrue(intersection.task instanceof IntersectionFunction);
	}

	@Test
	public void testMassiveDataPipelineIntersectionFormDAGAbsFunc() throws Exception {
		StreamPipeline<String> mdpleft = new StreamPipeline<String>();
		StreamPipeline<String> mdpright = new StreamPipeline<String>();
		StreamPipeline<String> mdpchild = mdpleft.intersection(mdpright);
		mdpleft.graph.addVertex(mdpleft);
		mdpleft.formDAGAbstractFunction(mdpleft, mdpleft.childs);
		assertEquals(2, mdpleft.graph.vertexSet().size());
		assertEquals(1, mdpleft.graph.edgeSet().size());
		assertTrue(mdpleft.graph.vertexSet().contains(mdpleft));
		assertTrue(mdpleft.graph.vertexSet().contains(mdpchild));
		DAGEdge dagedge = mdpleft.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdpleft);
		assertTrue(dagedge.getTarget() == mdpchild);

		mdpright.graph.addVertex(mdpright);
		mdpright.formDAGAbstractFunction(mdpright, mdpright.childs);
		assertEquals(2, mdpright.graph.vertexSet().size());
		assertEquals(1, mdpright.graph.edgeSet().size());
		assertTrue(mdpright.graph.vertexSet().contains(mdpright));
		assertTrue(mdpright.graph.vertexSet().contains(mdpchild));
		dagedge = mdpright.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdpright);
		assertTrue(dagedge.getTarget() == mdpchild);
	}

	@Test
	public void testMassiveDataPipelineMapPair() throws Exception {
		StreamPipeline<String> mdp = new StreamPipeline<String>();
		MapToPairFunction<String, Tuple2<String, String>> mappairfunction = data -> new Tuple2<>(data, data);
		MapPair<String, String> mappair = mdp.mapToPair(mappairfunction);
		assertTrue(mappair.parents.get(0) == mdp);
		assertTrue(mdp.childs.get(0) == mappair);
		assertTrue(mappair.task == mappairfunction);
	}

	@Test
	public void testMassiveDataPipelineMapPairFormDAGAbsFunc() throws Exception {
		StreamPipeline<String> mdp = new StreamPipeline<String>();
		MapToPairFunction<String, Tuple2<String, String>> mappairfunction = data -> new Tuple2<>(data, data);
		MapPair<String, String> mdpchild = mdp.mapToPair(mappairfunction);
		mdp.graph.addVertex(mdp);
		mdp.formDAGAbstractFunction(mdp, mdp.childs);
		assertEquals(2, mdp.graph.vertexSet().size());
		assertEquals(1, mdp.graph.edgeSet().size());
		assertTrue(mdp.graph.vertexSet().contains(mdp));
		assertTrue(mdp.graph.vertexSet().contains(mdpchild));
		DAGEdge dagedge = mdp.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdp);
		assertTrue(dagedge.getTarget() == mdpchild);
	}

	@Test
	public void testMassiveDataPipelineSample() throws Exception {
		StreamPipeline<String> mdp = new StreamPipeline<String>();
		Integer samplenumber = 10;
		StreamPipeline<String> sample = mdp.sample(samplenumber);
		assertTrue(sample.parents.get(0) == mdp);
		assertTrue(mdp.childs.get(0) == sample);
		assertTrue(sample.task instanceof SampleSupplierInteger);
		SampleSupplierInteger samplesuppinteger = (SampleSupplierInteger) sample.task;
		assertTrue(samplesuppinteger.getAsInt() == samplenumber);
	}

	@Test
	public void testMassiveDataPipelineSampleFormDAGAbsFunc() throws Exception {
		StreamPipeline<String> mdp = new StreamPipeline<String>();
		Integer samplenumber = 10;
		StreamPipeline<String> mdpchild = mdp.sample(samplenumber);
		mdp.graph.addVertex(mdp);
		mdp.formDAGAbstractFunction(mdp, mdp.childs);
		assertEquals(2, mdp.graph.vertexSet().size());
		assertEquals(1, mdp.graph.edgeSet().size());
		assertTrue(mdp.graph.vertexSet().contains(mdp));
		assertTrue(mdp.graph.vertexSet().contains(mdpchild));
		DAGEdge dagedge = mdp.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdp);
		assertTrue(dagedge.getTarget() == mdpchild);
	}

	@Test
	public void testMassiveDataPipelineRightOuterJoin() throws Exception {
		StreamPipeline<String> mdpleft = new StreamPipeline<String>();
		StreamPipeline<String> mdpright = new StreamPipeline<String>();
		RightOuterJoinPredicate<String, String> roj = (datleft, datright) -> datleft.equals(datright);
		StreamPipeline<Tuple2<String, String>> mdpchild = mdpleft.rightOuterjoin(mdpright, roj);
		assertTrue(mdpchild.parents.get(0) == mdpleft);
		assertTrue(mdpleft.childs.get(0) == mdpchild);
		assertTrue(mdpchild.parents.get(1) == mdpright);
		assertTrue(mdpright.childs.get(0) == mdpchild);
		assertTrue(mdpchild.task instanceof RightOuterJoinPredicate);
		assertTrue(mdpchild.task == roj);
	}

	@Test
	public void testMassiveDataPipelineRightOuterJoinFormDAGAbsFunc() throws Exception {
		StreamPipeline<String> mdpleft = new StreamPipeline<String>();
		StreamPipeline<String> mdpright = new StreamPipeline<String>();
		RightOuterJoinPredicate<String, String> roj = (datleft, datright) -> datleft.equals(datright);
		StreamPipeline<Tuple2<String, String>> mdpchild = mdpleft.rightOuterjoin(mdpright, roj);
		mdpleft.graph.addVertex(mdpleft);
		mdpleft.formDAGAbstractFunction(mdpleft, mdpleft.childs);
		assertEquals(2, mdpleft.graph.vertexSet().size());
		assertEquals(1, mdpleft.graph.edgeSet().size());
		assertTrue(mdpleft.graph.vertexSet().contains(mdpleft));
		assertTrue(mdpleft.graph.vertexSet().contains(mdpchild));
		DAGEdge dagedge = mdpleft.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdpleft);
		assertTrue(dagedge.getTarget() == mdpchild);

		mdpright.graph.addVertex(mdpright);
		mdpright.formDAGAbstractFunction(mdpright, mdpright.childs);
		assertEquals(2, mdpright.graph.vertexSet().size());
		assertEquals(1, mdpright.graph.edgeSet().size());
		assertTrue(mdpright.graph.vertexSet().contains(mdpright));
		assertTrue(mdpright.graph.vertexSet().contains(mdpchild));
		dagedge = mdpright.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdpright);
		assertTrue(dagedge.getTarget() == mdpchild);
	}

	@Test
	public void testMassiveDataPipelineLeftOuterJoin() throws Exception {
		StreamPipeline<String> mdpleft = new StreamPipeline<String>();
		StreamPipeline<String> mdpright = new StreamPipeline<String>();
		LeftOuterJoinPredicate<String, String> loj = (datleft, datright) -> datleft.equals(datright);
		StreamPipeline<Tuple2<String, String>> mdpchild = mdpleft.leftOuterjoin(mdpright, loj);
		assertTrue(mdpchild.parents.get(0) == mdpleft);
		assertTrue(mdpleft.childs.get(0) == mdpchild);
		assertTrue(mdpchild.parents.get(1) == mdpright);
		assertTrue(mdpright.childs.get(0) == mdpchild);
		assertTrue(mdpchild.task instanceof LeftOuterJoinPredicate);
		assertTrue(mdpchild.task == loj);
	}

	@Test
	public void testMassiveDataPipelineLeftOuterJoinFormDAGAbsFunc() throws Exception {
		StreamPipeline<String> mdpleft = new StreamPipeline<String>();
		StreamPipeline<String> mdpright = new StreamPipeline<String>();
		LeftOuterJoinPredicate<String, String> loj = (datleft, datright) -> datleft.equals(datright);
		StreamPipeline<Tuple2<String, String>> mdpchild = mdpleft.leftOuterjoin(mdpright, loj);
		mdpleft.graph.addVertex(mdpleft);
		mdpleft.formDAGAbstractFunction(mdpleft, mdpleft.childs);
		assertEquals(2, mdpleft.graph.vertexSet().size());
		assertEquals(1, mdpleft.graph.edgeSet().size());
		assertTrue(mdpleft.graph.vertexSet().contains(mdpleft));
		assertTrue(mdpleft.graph.vertexSet().contains(mdpchild));
		DAGEdge dagedge = mdpleft.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdpleft);
		assertTrue(dagedge.getTarget() == mdpchild);

		mdpright.graph.addVertex(mdpright);
		mdpright.formDAGAbstractFunction(mdpright, mdpright.childs);
		assertEquals(2, mdpright.graph.vertexSet().size());
		assertEquals(1, mdpright.graph.edgeSet().size());
		assertTrue(mdpright.graph.vertexSet().contains(mdpright));
		assertTrue(mdpright.graph.vertexSet().contains(mdpchild));
		dagedge = mdpright.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdpright);
		assertTrue(dagedge.getTarget() == mdpchild);
	}

	@Test
	public void testMassiveDataPipelineSorted() throws Exception {
		StreamPipeline<String> mdp = new StreamPipeline<String>();
		SortedComparator<String> sortedcomparator = (dat1, dat2) -> dat1.compareTo(dat2);
		StreamPipeline<String> mdpchild = mdp.sorted(sortedcomparator);
		assertTrue(mdpchild.parents.get(0) == mdp);
		assertTrue(mdp.childs.get(0) == mdpchild);
		assertTrue(mdpchild.task instanceof SortedComparator);
		assertTrue(mdpchild.task == sortedcomparator);
	}

	@Test
	public void testMassiveDataPipelineSortedFormDAGAbsFunc() throws Exception {
		StreamPipeline<String> mdp = new StreamPipeline<String>();
		SortedComparator<String> sortedcomparator = (dat1, dat2) -> dat1.compareTo(dat2);
		StreamPipeline<String> mdpchild = mdp.sorted(sortedcomparator);
		mdp.graph.addVertex(mdp);
		mdp.formDAGAbstractFunction(mdp, mdp.childs);
		assertEquals(2, mdp.graph.vertexSet().size());
		assertEquals(1, mdp.graph.edgeSet().size());
		assertTrue(mdp.graph.vertexSet().contains(mdp));
		assertTrue(mdp.graph.vertexSet().contains(mdpchild));
		DAGEdge dagedge = mdp.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdp);
		assertTrue(dagedge.getTarget() == mdpchild);
	}

	@Test
	public void testMassiveDataPipelineDistinct() throws Exception {
		StreamPipeline<String> mdp = new StreamPipeline<String>();
		StreamPipeline<String> mdpchild = mdp.distinct();
		assertTrue(mdpchild.parents.get(0) == mdp);
		assertTrue(mdp.childs.get(0) == mdpchild);
		assertTrue(mdpchild.task instanceof Distinct);
	}

	@Test
	public void testMassiveDataPipelineDistinctFormDAGAbsFunc() throws Exception {
		StreamPipeline<String> mdp = new StreamPipeline<String>();
		StreamPipeline<String> mdpchild = mdp.distinct();
		mdp.graph.addVertex(mdp);
		mdp.formDAGAbstractFunction(mdp, mdp.childs);
		assertEquals(2, mdp.graph.vertexSet().size());
		assertEquals(1, mdp.graph.edgeSet().size());
		assertTrue(mdp.graph.vertexSet().contains(mdp));
		assertTrue(mdp.graph.vertexSet().contains(mdpchild));
		DAGEdge dagedge = mdp.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdp);
		assertTrue(dagedge.getTarget() == mdpchild);
	}

	@Test
	public void testMassiveDataPipelineMaptoInt() throws Exception {
		StreamPipeline<String> mdp = new StreamPipeline<String>();
		SToIntFunction<String> tointfunction = dat -> Integer.parseInt(dat);
		PipelineIntStream<String> mdpchild = mdp.mapToInt(tointfunction);
		assertTrue(mdpchild.parents.get(0) == mdp);
		assertTrue(mdp.childs.get(0) == mdpchild);
		assertTrue(mdpchild.task instanceof ToIntFunction);
		assertTrue(mdpchild.task == tointfunction);
	}

	@Test
	public void testMassiveDataPipelineMaptoIntFormDAGAbsFunc() throws Exception {
		StreamPipeline<String> mdp = new StreamPipeline<String>();
		SToIntFunction<String> tointfunction = dat -> Integer.parseInt(dat);
		PipelineIntStream<String> mdpchild = mdp.mapToInt(tointfunction);
		mdp.graph.addVertex(mdp);
		mdp.formDAGAbstractFunction(mdp, mdp.childs);
		assertEquals(2, mdp.graph.vertexSet().size());
		assertEquals(1, mdp.graph.edgeSet().size());
		assertTrue(mdp.graph.vertexSet().contains(mdp));
		assertTrue(mdp.graph.vertexSet().contains(mdpchild));
		DAGEdge dagedge = mdp.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdp);
		assertTrue(dagedge.getTarget() == mdpchild);
	}

	@Test
	public void testMassiveDataPipelineKeyBy() throws Exception {
		StreamPipeline<String> mdp = new StreamPipeline<String>();
		KeyByFunction<String, String> keybyfunction = dat -> dat;
		MapPair<String, String> mdpchild = mdp.keyBy(keybyfunction);
		assertTrue(mdpchild.parents.get(0) == mdp);
		assertTrue(mdp.childs.get(0) == mdpchild);
		assertTrue(mdpchild.task instanceof KeyByFunction);
		assertTrue(mdpchild.task == keybyfunction);
	}

	@Test
	public void testMassiveDataPipelineKeyByFormDAGAbsFunc() throws Exception {
		StreamPipeline<String> mdp = new StreamPipeline<String>();
		KeyByFunction<String, String> keybyfunction = dat -> dat;
		MapPair<String, String> mdpchild = mdp.keyBy(keybyfunction);
		mdp.graph.addVertex(mdp);
		mdp.formDAGAbstractFunction(mdp, mdp.childs);
		assertEquals(2, mdp.graph.vertexSet().size());
		assertEquals(1, mdp.graph.edgeSet().size());
		assertTrue(mdp.graph.vertexSet().contains(mdp));
		assertTrue(mdp.graph.vertexSet().contains(mdpchild));
		DAGEdge dagedge = mdp.graph.edgeSet().iterator().next();
		assertTrue(dagedge.getSource() == mdp);
		assertTrue(dagedge.getTarget() == mdpchild);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testGetDAGMap() throws Exception {
		String config = pipelineconfig.getBlocksize();
		pipelineconfig.setBlocksize("1");
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		StreamPipeline<String> filter = mdp.map(dat -> dat).filter(dat -> dat.equals(dat));
		MapPair<String, Long> mappair = filter.mapToPair(dat -> new Tuple2<String, Long>(dat, Long.parseLong(dat)));
		MapPair<String, Long> redbykeyleft = mappair.reduceByKey((dat1, dat2) -> dat1 + dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);
		MapPair<String, Long> redbykeyright = mappair.reduceByKey((dat1, dat2) -> dat1 - dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);
		StreamPipeline<String> mdproot = (StreamPipeline<String>) redbykeyleft.join(redbykeyright,
				(left, right) -> left.equals(right)).root;
		mdproot.finaltasks.add(mdproot.finaltask);
		Job job = new Job();
		job.setJm(new JobMetrics());
		mdproot.getDAG(job);
		pipelineconfig.setBlocksize(config);

		assertEquals(9, mdproot.graph.vertexSet().size());
		assertEquals(9, mdproot.graph.edgeSet().size());
	}
}
