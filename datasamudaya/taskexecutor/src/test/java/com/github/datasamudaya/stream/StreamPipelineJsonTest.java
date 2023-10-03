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
import java.util.List;
import org.apache.log4j.Logger;
import org.jooq.lambda.tuple.Tuple2;
import org.json.simple.JSONObject;
import org.junit.Test;

import com.github.datasamudaya.stream.JsonStream;
import com.github.datasamudaya.stream.StreamPipeline;

public class StreamPipelineJsonTest extends StreamPipelineBaseTestCommon {
	boolean toexecute = true;
	Logger log = Logger.getLogger(StreamPipelineDepth2Test.class);
	int sum;


	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testFilterJsonCollect() throws Throwable {
		log.info("testFilterJsonCollect Before---------------------------------------");
		JsonStream<JSONObject> datapipeline = StreamPipeline.newJsonStreamHDFS(hdfsfilepath,
				githubevents, pipelineconfig);
		List<List<JSONObject>> data = (List) datapipeline
				.filter(jsonobj -> jsonobj != null && jsonobj.get("type").equals("CreateEvent")).collect(toexecute, null);
		for (List<JSONObject> partitioneddata : data) {
			for (JSONObject obj :partitioneddata) {
				log.info(obj);
				assertEquals("CreateEvent", obj.get("type"));
			}
		}
		log.info("testFilterJsonCollect After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testFilterMapJsonCollect() throws Throwable {
		log.info("testFilterMapJsonCollect Before---------------------------------------");
		JsonStream<JSONObject> datapipeline = StreamPipeline.newJsonStreamHDFS(hdfsfilepath,
				githubevents, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.filter(jsonobj -> jsonobj != null && (jsonobj.get("type").equals("CreateEvent") || (jsonobj.get("type").equals("IssueCommentEvent") || (jsonobj.get("type").equals("WatchEvent")))))
				.mapToPair(json -> new Tuple2<>(json.get("type"), 1l)).reduceByKey((a, b) -> a + b)
				.collect(toexecute, null);
		for (List<Tuple2> partitioneddata : data) {
			for (Tuple2 obj :partitioneddata) {
				log.info(obj);
			}
		}
		log.info("testFilterMapJsonCollect After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testFilterMapJsonAllCollect() throws Throwable {
		log.info("testFilterMapJsonAllCollect Before---------------------------------------");
		JsonStream<JSONObject> datapipeline = StreamPipeline.newJsonStreamHDFS(hdfsfilepath,
				githubevents, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.filter(jsonobj -> jsonobj != null)
				.mapToPair(json -> new Tuple2<>(json.get("type"), 1l)).reduceByKey((a, b) -> a + b)
				.collect(toexecute, null);
		for (List<Tuple2> partitioneddata : data) {
			for (Tuple2 obj :partitioneddata) {
				log.info(obj);
			}
		}
		log.info("testFilterMapJsonAllCollect After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testFilterMapWithTupleObjectJsonUnionCollect() throws Throwable {
		log.info("testFilterMapJsonUnionCollect Before---------------------------------------");
		JsonStream<JSONObject> datapipeline = StreamPipeline.newJsonStreamHDFS(hdfsfilepath,
				githubevents, pipelineconfig);
		StreamPipeline<Tuple2<String, Long>> mdp1 = (StreamPipeline<Tuple2<String, Long>>) datapipeline
				.filter(jsonobj -> jsonobj != null)
				.map(json -> new Tuple2<>((String) json.get("type"), 1l));

		StreamPipeline<Tuple2<String, Long>> mdp2 = (StreamPipeline<Tuple2<String, Long>>) datapipeline
				.filter(jsonobj -> jsonobj != null)
				.map(json -> new Tuple2<>((String) json.get("type"), json.get("type").equals("CreateEvent") ? 2l : 1l));

		List<List<Tuple2>> result = mdp1.union(mdp2).collect(toexecute, null);
		for (List<Tuple2> partitioneddata : result) {
			for (Tuple2 obj :partitioneddata) {
				log.info(obj);
			}
		}
		log.info("testFilterMapJsonUnionCollect After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testFilterMapJsonIntersectionCollect() throws Throwable {
		log.info("testFilterMapJsonIntersectionCollect Before---------------------------------------");
		JsonStream<JSONObject> datapipeline = StreamPipeline.newJsonStreamHDFS(hdfsfilepath,
				githubevents, pipelineconfig);
		StreamPipeline<Tuple2<String, Long>> mdp1 = (StreamPipeline<Tuple2<String, Long>>) datapipeline
				.filter(jsonobj -> jsonobj != null)
				.map(json -> new Tuple2<>((String) json.get("type"), 1l));

		StreamPipeline<Tuple2<String, Long>> mdp2 = (StreamPipeline<Tuple2<String, Long>>) datapipeline
				.filter(jsonobj -> jsonobj != null)
				.map(json -> new Tuple2<>((String) json.get("type"), json.get("type").equals("CreateEvent") ? 2l : 1l));

		List<List<Tuple2>> result = mdp1.intersection(mdp2).collect(toexecute, null);
		for (List<Tuple2> partitioneddata : result) {
			for (Tuple2 obj :partitioneddata) {
				log.info(obj);
			}
		}
		log.info("testFilterMapJsonIntersectionCollect After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testFilterMapJsonUnionIntersectionCollect() throws Throwable {
		log.info("testFilterMapJsonUnionIntersectionCollect Before---------------------------------------");
		JsonStream<JSONObject> datapipeline = StreamPipeline.newJsonStreamHDFS(hdfsfilepath,
				githubevents, pipelineconfig);
		StreamPipeline<Tuple2<String, Long>> mdp1 = (StreamPipeline<Tuple2<String, Long>>) datapipeline
				.filter(jsonobj -> jsonobj != null)
				.map(json -> new Tuple2<>((String) json.get("type"), 1l));

		StreamPipeline<Tuple2<String, Long>> mdp2 = (StreamPipeline<Tuple2<String, Long>>) datapipeline
				.filter(jsonobj -> jsonobj != null)
				.map(json -> new Tuple2<>((String) json.get("type"), json.get("type").equals("CreateEvent") ? 2l : 1l));

		StreamPipeline<Tuple2<String, Long>> mdp3 = (StreamPipeline<Tuple2<String, Long>>) datapipeline
				.filter(jsonobj -> jsonobj != null)
				.map(json -> new Tuple2<>((String) json.get("type"), json.get("type").equals("IssueCommentEvent") ? 2l : 1l));

		List<List<Tuple2>> result = mdp1.union(mdp2).intersection(mdp3).collect(toexecute, null);
		for (List<Tuple2> partitioneddata : result) {
			for (Tuple2 obj :partitioneddata) {
				log.info(obj);
			}
		}
		log.info("testFilterMapJsonUnionIntersectionCollect After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testFilterMapJsonUnionIntersectionUnionCollect() throws Throwable {
		log.info("testFilterMapJsonUnionIntersectionUnionCollect Before---------------------------------------");
		JsonStream<JSONObject> datapipeline = StreamPipeline.newJsonStreamHDFS(hdfsfilepath,
				githubevents, pipelineconfig);
		StreamPipeline<Tuple2<String, Long>> mdp1 = (StreamPipeline<Tuple2<String, Long>>) datapipeline
				.filter(jsonobj -> jsonobj != null)
				.map(json -> new Tuple2<>((String) json.get("type"), 1l));

		StreamPipeline<Tuple2<String, Long>> mdp2 = (StreamPipeline<Tuple2<String, Long>>) datapipeline
				.filter(jsonobj -> jsonobj != null)
				.map(json -> new Tuple2<>((String) json.get("type"), json.get("type").equals("CreateEvent") ? 2l : 1l));

		StreamPipeline<Tuple2<String, Long>> mdp3 = (StreamPipeline<Tuple2<String, Long>>) datapipeline
				.filter(jsonobj -> jsonobj != null)
				.map(json -> new Tuple2<>((String) json.get("type"), json.get("type").equals("IssueCommentEvent") ? 2l : 1l));

		StreamPipeline<Tuple2<String, Long>> mdp4 = (StreamPipeline<Tuple2<String, Long>>) datapipeline
				.filter(jsonobj -> jsonobj != null)
				.map(json -> new Tuple2<>((String) json.get("type"), 1l));

		List<List<Tuple2>> result = mdp1.union(mdp2).intersection(mdp3).union(mdp4).collect(toexecute, null);
		for (List<Tuple2> partitioneddata : result) {
			for (Tuple2 obj :partitioneddata) {
				log.info(obj);
			}
		}
		log.info("testFilterMapJsonUnionIntersectionUnionCollect After---------------------------------------");
	}


	@SuppressWarnings({"unchecked"})
	@Test
	public void testFilterMapJsonUnionIntersectionUnionCount() throws Throwable {
		log.info("testFilterMapJsonUnionIntersectionUnionCount Before---------------------------------------");
		JsonStream<JSONObject> datapipeline = StreamPipeline.newJsonStreamHDFS(hdfsfilepath,
				githubevents, pipelineconfig);
		StreamPipeline<Tuple2<String, Long>> mdp1 = (StreamPipeline<Tuple2<String, Long>>) datapipeline
				.filter(jsonobj -> jsonobj != null)
				.map(json -> new Tuple2<>((String) json.get("type"), 1l));

		StreamPipeline<Tuple2<String, Long>> mdp2 = (StreamPipeline<Tuple2<String, Long>>) datapipeline
				.filter(jsonobj -> jsonobj != null)
				.map(json -> new Tuple2<>((String) json.get("type"), json.get("type").equals("CreateEvent") ? 2l : 1l));

		StreamPipeline<Tuple2<String, Long>> mdp3 = (StreamPipeline<Tuple2<String, Long>>) datapipeline
				.filter(jsonobj -> jsonobj != null)
				.map(json -> new Tuple2<>((String) json.get("type"), json.get("type").equals("IssueCommentEvent") ? 2l : 1l));

		StreamPipeline<Tuple2<String, Long>> mdp4 = (StreamPipeline<Tuple2<String, Long>>) datapipeline
				.filter(jsonobj -> jsonobj != null)
				.map(json -> new Tuple2<>((String) json.get("type"), 1l));

		List<List<Long>> result = (List<List<Long>>) mdp1.union(mdp2).intersection(mdp3).union(mdp4).count(null);
		for (List<Long> partitioneddata : result) {
			for (Long obj :partitioneddata) {
				log.info(obj);
			}
		}
		log.info("testFilterMapJsonUnionIntersectionUnionCount After---------------------------------------");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFilterJsonUnionSortedAscCollect() throws Throwable {
		log.info("testFilterJsonUnionSortedAscCollect Before---------------------------------------");
		JsonStream<JSONObject> datapipeline = StreamPipeline.newJsonStreamHDFS(hdfsfilepath, githubevents,
				pipelineconfig);
		StreamPipeline<JSONObject> mdp1 = (StreamPipeline<JSONObject>) datapipeline
				.filter(jsonobj -> jsonobj != null);

		StreamPipeline<JSONObject> mdp2 = (StreamPipeline<JSONObject>) datapipeline
				.filter(jsonobj -> jsonobj != null && jsonobj.get("type").equals("CreateEvent"));

		List<List<JSONObject>> result = (List<List<JSONObject>>) mdp1.union(mdp2)
				.sorted((JSONObject json1, JSONObject json2) -> {
					return ((String) json1.get("type")).compareTo((String) json2.get("type"));

				}).collect(toexecute, null);

		for (List<JSONObject> partitioneddata : result) {
			for (JSONObject jsonobj : partitioneddata) {
				log.info(jsonobj.get("type"));
			}
		}
		log.info("testFilterJsonUnionSortedAscCollect After---------------------------------------");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFilterJsonUnionSortedDescCollect() throws Throwable {
		log.info("testFilterJsonUnionSortedDescCollect Before---------------------------------------");
		JsonStream<JSONObject> datapipeline = StreamPipeline.newJsonStreamHDFS(hdfsfilepath, githubevents,
				pipelineconfig);
		StreamPipeline<JSONObject> mdp1 = (StreamPipeline<JSONObject>) datapipeline
				.filter(jsonobj -> jsonobj != null);

		StreamPipeline<JSONObject> mdp2 = (StreamPipeline<JSONObject>) datapipeline
				.filter(jsonobj -> jsonobj != null && jsonobj.get("type").equals("CreateEvent"));

		List<List<JSONObject>> result = (List<List<JSONObject>>) mdp1.union(mdp2)
				.sorted((JSONObject json1, JSONObject json2) -> {
					return ((String) json2.get("type")).compareTo((String) json1.get("type"));

				}).collect(toexecute, null);

		for (List<JSONObject> partitioneddata : result) {
			for (JSONObject jsonobj : partitioneddata) {
				log.info(jsonobj.get("type"));
			}
		}
		log.info("testFilterJsonUnionSortedDescCollect After---------------------------------------");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFilterJsonUnionSortedDescFilterCollect() throws Throwable {
		log.info("testFilterJsonUnionSortedDescFilterCollect Before---------------------------------------");
		JsonStream<JSONObject> datapipeline = StreamPipeline.newJsonStreamHDFS(hdfsfilepath, githubevents,
				pipelineconfig);
		StreamPipeline<JSONObject> mdp1 = (StreamPipeline<JSONObject>) datapipeline
				.filter(jsonobj -> jsonobj != null);

		StreamPipeline<JSONObject> mdp2 = (StreamPipeline<JSONObject>) datapipeline
				.filter(jsonobj -> jsonobj != null && jsonobj.get("type").equals("CreateEvent"));

		List<List<JSONObject>> result = (List<List<JSONObject>>) mdp1.union(mdp2)
				.sorted((JSONObject json1, JSONObject json2) -> {
					return ((String) json2.get("type")).compareTo((String) json1.get("type"));

				}).filter(jsonobj -> jsonobj.get("type").equals("CreateEvent")).collect(toexecute, null);

		for (List<JSONObject> partitioneddata : result) {
			for (JSONObject jsonobj : partitioneddata) {
				log.info(jsonobj.get("type"));
			}
		}
		log.info("testFilterJsonUnionSortedDescFilterCollect After---------------------------------------");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testFilterJsonIntersectionSortedDescCollect() throws Throwable {
		log.info("testFilterJsonIntersectionSortedDescCollect Before---------------------------------------");
		JsonStream<JSONObject> datapipeline = StreamPipeline.newJsonStreamHDFS(hdfsfilepath, githubevents,
				pipelineconfig);
		StreamPipeline<JSONObject> mdp1 = (StreamPipeline<JSONObject>) datapipeline
				.filter(jsonobj -> jsonobj != null);

		StreamPipeline<JSONObject> mdp2 = (StreamPipeline<JSONObject>) datapipeline
				.filter(jsonobj -> jsonobj != null && jsonobj.get("type").equals("CreateEvent"));

		List<List<JSONObject>> result = (List<List<JSONObject>>) mdp1.intersection(mdp2)
				.sorted((JSONObject json1, JSONObject json2) -> {
					return ((String) json2.get("type")).compareTo((String) json1.get("type"));
				}).collect(toexecute, null);

		for (List<JSONObject> partitioneddata : result) {
			for (JSONObject jsonobj : partitioneddata) {
				log.info(jsonobj);
			}
		}
		log.info("testFilterJsonIntersectionSortedDescCollect After---------------------------------------");
	}
}
