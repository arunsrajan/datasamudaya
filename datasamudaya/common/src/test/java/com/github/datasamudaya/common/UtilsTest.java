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
package com.github.datasamudaya.common;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.burningwave.core.assembler.StaticComponentContainer;
import org.jgroups.JChannel;
import org.jgroups.ObjectMessage;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.ClosureSerializer;

import com.github.datasamudaya.common.functions.Coalesce;
import com.github.datasamudaya.common.utils.Utils;
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;

import jdk.jshell.JShell;

import net.sf.jsqlparser.parser.SimpleNode;
import net.sf.jsqlparser.schema.Table;

public class UtilsTest {

	@Before
	public void startUp() {
		StaticComponentContainer.Modules.exportAllToAll();
	}

	@Test
	public void testaddShutDownHook() {
		Utils.addShutdownHook(() -> {
		});
	}

	@Test
	public void testloadLog4JSystemPropertiesPropertyFileNull() {
		try {
			Utils.initializeProperties("", null);
		}
		catch (Exception ex) {
			assertEquals("Property File Name cannot be null", ex.getMessage());
		}
	}

	@Test
	public void testloadLog4JSystemPropertiesFilePath() {
		try {
			Utils.initializeProperties(null, "");
		}
		catch (Exception ex) {
			assertEquals("Properties File Path cannot be null", ex.getMessage());
		}
	}

	@Test
	public void testloadLog4JSystemPropertiesProperInput() throws Exception {
		Utils.initializeProperties(DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
				+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH, DataSamudayaConstants.DATASAMUDAYA_PROPERTIES);
	}

	@Test
	public void testloadLog4JSystemPropertiesEmptyFileName() {
		try {
			Utils.initializeProperties(DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
					+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH, "");
		}
		catch (Exception ex) {
			assertEquals("Unable To Load Properties", ex.getMessage());
			assertTrue(ex.getCause().getMessage().contains("Access is denied"));
		}
	}

	@Test
	public void initializePropertiesValid() throws Exception {
		String propertiesFilePath = System.getProperty("user.dir") + "/config/";
		File file = new File(propertiesFilePath);
		if (!(file.isDirectory() && file.exists())) {
			propertiesFilePath = "../config/";
			file = new File(propertiesFilePath);
			if (!(file.isDirectory() && file.exists())) {
				throw new Exception();
			}
		}
		String propertyFile = "datasamudaya.properties";
		Utils.initializeProperties(propertiesFilePath, propertyFile);
		// Assert that properties are initialized correctly and container users share is processed as expected.
		ConcurrentMap<String, User> users = DataSamudayaUsers.get();
		assertTrue(Objects.nonNull(users));
		assertTrue(users.containsKey("arun"));
		assertEquals(Integer.valueOf(100), users.get("arun").getPercentage());
		assertTrue(Objects.nonNull(DataSamudayaProperties.get()));
		assertEquals("127.0.0.1:2181", DataSamudayaProperties.get().get("zookeeper.hostport"));

	}

	@Test
	public void initializePropertiesNullPropertyFile() throws Exception {
		try {
			String propertiesFilePath = System.getProperty("user.dir") + "/config/";
			File file = new File(propertiesFilePath);
			if (!(file.isDirectory() && file.exists())) {
				propertiesFilePath = "../config/";
				file = new File(propertiesFilePath);
				if (!(file.isDirectory() && file.exists())) {
					throw new Exception();
				}
			}
			String propertyFile = null;
			Utils.initializeProperties(propertiesFilePath, propertyFile);
		} catch (Exception ex) {
			Assert.assertEquals("Property File Name cannot be null", ex.getMessage());
		}
	}

	@Test
	public void initializePropertiesNullPropertyFilePath() throws Exception {
		try {
			String propertiesFilePath = null;
			String propertyFile = "datasamudaya.properties";
			Utils.initializeProperties(propertiesFilePath, propertyFile);
		} catch (Exception ex) {
			Assert.assertEquals("Properties File Path cannot be null", ex.getMessage());
		}
	}

	@Test
	public void initializePropertiesInvalidUsersShare() throws Exception {
		try {
			String propertiesFilePath = System.getProperty("user.dir") + "/config/";
			File file = new File(propertiesFilePath);
			if (!(file.isDirectory() && file.exists())) {
				propertiesFilePath = "../config/";
				file = new File(propertiesFilePath);
				if (!(file.isDirectory() && file.exists())) {
					throw new Exception();
				}
			}
			String propertyFile = "datasamudayainvalidusersshare.properties";
			Utils.initializeProperties(propertiesFilePath, propertyFile);
		}
		catch (Exception ex) {
			Assert.assertEquals("Users share total not tally and it should be less that or equal to 100.0", ex.getCause().getMessage());
		}

	}

	@Test
	public void initializePropertiesNonExistent() throws Exception {
		try {
			String propertiesFilePath = System.getProperty("user.dir") + "/config/";
			File file = new File(propertiesFilePath);
			if (!(file.isDirectory() && file.exists())) {
				propertiesFilePath = "../config/";
				file = new File(propertiesFilePath);
				if (!(file.isDirectory() && file.exists())) {
					throw new Exception();
				}
			}
			String propertyFile = "datasamudayanonexistent.properties";
			Utils.initializeProperties(propertiesFilePath, propertyFile);
		}
		catch (Exception ex) {
			Assert.assertEquals("..\\config\\datasamudayanonexistent.properties (The system cannot find the file specified)", ex.getCause().getMessage());
		}

	}

	@Test
	public void kryoInstanceNotNull() {
		Kryo kryo = Utils.getKryoInstance();

		// Ensure expected classes are registered
		assert kryo.getRegistration(Object.class) != null;
		assert kryo.getRegistration(Object[].class) != null;
		assert kryo.getRegistration(byte.class) != null;
		assert kryo.getRegistration(byte[].class) != null;
		assert kryo.getRegistration(String[].class) != null;
		assert kryo.getRegistration(Integer[].class) != null;
		assert kryo.getRegistration(Long[].class) != null;
		assert kryo.getRegistration(Float[].class) != null;
		assert kryo.getRegistration(Double[].class) != null;
		assert kryo.getRegistration(Vector.class) != null;
		assert kryo.getRegistration(ArrayList.class) != null;
		assert kryo.getRegistration(HashMap.class) != null;
		assert kryo.getRegistration(ConcurrentHashMap.class) != null;
		assert kryo.getRegistration(LinkedHashSet.class) != null;
		assert kryo.getRegistration(HashSet.class) != null;
		assert kryo.getRegistration(WhoIsResponse.class) != null;
		assert kryo.getRegistration(WhoIsRequest.class) != null;
		assert kryo.getRegistration(WhoAreRequest.class) != null;
		assert kryo.getRegistration(WhoAreResponse.class) != null;
		assert kryo.getRegistration(Tuple2Serializable.class) != null;
		assert kryo.getRegistration(WhoIsResponse.STATUS.class) != null;
		assert kryo.getRegistration(Coalesce.class) != null;
		assert kryo.getRegistration(JobStage.class) != null;
		assert kryo.getRegistration(Stage.class) != null;
		assert kryo.getRegistration(Table.class) != null;
		assert kryo.getRegistration(SimpleNode.class) != null;
		assert kryo.getRegistration(java.lang.invoke.SerializedLambda.class) != null;
		assert kryo.getRegistration(Tuple2.class) != null;
		assert kryo.getRegistration(ClosureSerializer.Closure.class) != null;
		assert kryo.getRegistration(JShell.class) != null;

		// Ensure custom serializers are registered
		assert kryo.getSerializer(Tuple2.class) != null;
		assert kryo.getSerializer(WhoIsResponse.STATUS.class) != null;
		assert kryo.getSerializer(Coalesce.class) != null;
		assert kryo.getSerializer(JobStage.class) != null;
		assert kryo.getSerializer(Stage.class) != null;
		assert kryo.getSerializer(Table.class) != null;
		assert kryo.getSerializer(SimpleNode.class) != null;
		assert kryo.getSerializer(ClosureSerializer.Closure.class) != null;
		assert kryo.getSerializer(JShell.class) != null;
	}

	@Test
	public void kryoInstanceTestSerializationDeserialization() {
		Kryo kryo = Utils.getKryoInstance();
		Integer originalObject = Integer.valueOf(25); // Create a test object
		byte[] serializedData;
		try (Output output = new Output(new ByteArrayOutputStream())) {
			kryo.writeObject(output, originalObject);
			serializedData = output.toBytes();
		}
		Object deserializedObject;
		try (Input input = new Input(serializedData)) {
			deserializedObject = kryo.readObject(input, Integer.class);
		}
		// Assert that deserializedObject is not null
		assertNotNull(deserializedObject);
		assertTrue(deserializedObject.equals(originalObject));
	}


	@Test
	public void kryoInstanceTestSerializationDeserializationTuple2() {
		Kryo kryo = Utils.getKryoInstance();
		Tuple2<String, Integer> originalTuple = Tuple.tuple("Hello", 42); // Create a test tuple
		byte[] serializedData;
		try (Output output = new Output(new ByteArrayOutputStream())) {
			kryo.writeObject(output, originalTuple);
			serializedData = output.toBytes();
		}
		Tuple2<?, ?> deserializedTuple;
		try (Input input = new Input(serializedData)) {
			deserializedTuple = kryo.readObject(input, Tuple2.class);
		}
		// Assert that deserializedTuple is not null
		// Assert that deserializedTuple is equal to originalTuple
		assertNotNull(deserializedTuple);
		assertTrue(deserializedTuple.equals(originalTuple));
	}

	@Test
	public void kryoInstanceTestSerializationDeserializationCustomClass() {
		Kryo kryo = Utils.getKryoInstance();
		JobStage originalCustom = new JobStage(); // Create a test custom object
		byte[] serializedData;
		try (Output output = new Output(new ByteArrayOutputStream())) {
			kryo.writeObject(output, originalCustom);
			serializedData = output.toBytes();
		}
		JobStage deserializedCustom;
		try (Input input = new Input(serializedData)) {
			deserializedCustom = kryo.readObject(input, JobStage.class);
		}
		// Assert that deserializedCustom is not null
		// Assert that deserializedCustom is equal to originalCustom
		assertNotNull(deserializedCustom);
		assertTrue(deserializedCustom.equals(originalCustom));
	}


	@Test
	public void kryoInstanceTestSerializationDeserializationEnum() {
		Kryo kryo = Utils.getKryoInstance();
		WhoIsResponse.STATUS originalStatus = WhoIsResponse.STATUS.COMPLETED; // Choose an enum value
		byte[] serializedData;
		try (Output output = new Output(new ByteArrayOutputStream())) {
			kryo.writeObject(output, originalStatus);
			serializedData = output.toBytes();
		}
		WhoIsResponse.STATUS deserializedStatus;
		try (Input input = new Input(serializedData)) {
			deserializedStatus = kryo.readObject(input, WhoIsResponse.STATUS.class);
		}
		// Assert that deserializedStatus is not null
		// Assert that deserializedStatus is equal to originalStatus
		assertNotNull(originalStatus);
		assertTrue(deserializedStatus.equals(originalStatus));
	}

	@Test
	public void testJGroupsValid() throws Exception {
		Utils.initializeProperties(DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
				+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH, DataSamudayaConstants.DATASAMUDAYA_PROPERTIES);
		Kryo kryo = Utils.getKryoInstance();
		String jobid = "job123";
		String networkaddress1 = "127.0.0.1";
		String networkaddress2 = "127.0.0.1";
		int port1 = 12345;
		int port2 = 12346;
		Map<String, WhoIsResponse.STATUS> mapreqsrc = new HashMap<>();
		Map<String, WhoIsResponse.STATUS> maprespsrc = new HashMap<>();
		Map<String, WhoIsResponse.STATUS> mapreqdest = new HashMap<>();
		Map<String, WhoIsResponse.STATUS> maprespdest = new HashMap<>();
		try {
			JChannel channelsrc = Utils.getChannelTaskExecutor(jobid, networkaddress1, port1, mapreqsrc, maprespsrc);
			JChannel channeldest = Utils.getChannelTaskExecutor(jobid, networkaddress2, port2, mapreqdest, maprespdest);
			// Send a message to the channel to trigger message reception
			mapreqdest.put("1", WhoIsResponse.STATUS.COMPLETED);
			var obj = new ObjectMessage();
			WhoIsRequest request = new WhoIsRequest();
			request.setStagepartitionid("1");
			var os = new ByteArrayOutputStream();
			var output = new Output(os);
			kryo.writeClassAndObject(output, request);
			output.flush();
			output.close();
			obj.setObject(os.toByteArray());
			channelsrc.send(obj);
			// For example, create a WhoIsRequest message and send it to the channel

			// Assert that the response map has been updated as expected
			// Assert other expected behaviors or states
			while (!maprespsrc.containsKey("1")) {
				Thread.sleep(1000);
			}
			assertNotNull(maprespsrc.get("1"));
			assertTrue(maprespsrc.get("1") == WhoIsResponse.STATUS.COMPLETED);
			channelsrc.close();
			channeldest.close();
		} catch (Exception ex) {
			// Handle any exceptions or failures
			// Fail the test if necessary
		}

	}

	@Test
	public void testJGroupsNullJobId() throws Exception {
		Utils.initializeProperties(DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
				+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH, DataSamudayaConstants.DATASAMUDAYA_PROPERTIES);
		String jobid = null;
		String networkaddress1 = "127.0.0.1";
		int port1 = 12345;
		Map<String, WhoIsResponse.STATUS> mapreqsrc = new HashMap<>();
		Map<String, WhoIsResponse.STATUS> maprespsrc = new HashMap<>();
		try {
			JChannel channelsrc = Utils.getChannelTaskExecutor(jobid, networkaddress1, port1, mapreqsrc, maprespsrc);
			channelsrc.close();
		} catch (Exception ex) {
			// Handle any exceptions or failures
			assertEquals("cluster name cannot be null", ex.getMessage());
			// Fail the test if necessary
		}

	}


	@Test
	public void testJGroupsWhois() throws Exception {
		Utils.initializeProperties(DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
				+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH, DataSamudayaConstants.DATASAMUDAYA_PROPERTIES);
		String jobid = "job123";
		String networkaddress1 = "127.0.0.1";
		String networkaddress2 = "127.0.0.1";
		int port1 = 12345;
		int port2 = 12346;
		Map<String, WhoIsResponse.STATUS> mapreqsrc = new HashMap<>();
		Map<String, WhoIsResponse.STATUS> maprespsrc = new HashMap<>();
		Map<String, WhoIsResponse.STATUS> mapreqdest = new HashMap<>();
		Map<String, WhoIsResponse.STATUS> maprespdest = new HashMap<>();
		try {
			JChannel channelsrc = Utils.getChannelTaskExecutor(jobid, networkaddress1, port1, mapreqsrc, maprespsrc);
			JChannel channeldest = Utils.getChannelTaskExecutor(jobid, networkaddress2, port2, mapreqdest, maprespdest);
			// Send a message to the channel to trigger message reception
			mapreqdest.put("1", WhoIsResponse.STATUS.COMPLETED);
			Utils.whois(channelsrc, "1");
			// For example, create a WhoIsRequest message and send it to the channel

			// Assert that the response map has been updated as expected
			// Assert other expected behaviors or states
			while (!maprespsrc.containsKey("1")) {
				Thread.sleep(1000);
			}
			assertNotNull(maprespsrc.get("1"));
			assertTrue(maprespsrc.get("1") == WhoIsResponse.STATUS.COMPLETED);
			channelsrc.close();
			channeldest.close();
		} catch (Exception ex) {
			// Handle any exceptions or failures
			// Fail the test if necessary
		}

	}

	@Test
	public void testJGroupsWhoare() throws Exception {
		Utils.initializeProperties(DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
				+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH, DataSamudayaConstants.DATASAMUDAYA_PROPERTIES);
		String jobid = "job123";
		String networkaddress1 = "127.0.0.1";
		String networkaddress2 = "127.0.0.1";
		int port1 = 12345;
		int port2 = 12346;
		Map<String, WhoIsResponse.STATUS> mapreqsrc = new HashMap<>();
		Map<String, WhoIsResponse.STATUS> maprespsrc = new HashMap<>();
		Map<String, WhoIsResponse.STATUS> mapreqdest = new HashMap<>();
		Map<String, WhoIsResponse.STATUS> maprespdest = new HashMap<>();
		try {
			JChannel channelsrc = Utils.getChannelTaskExecutor(jobid, networkaddress1, port1, mapreqsrc, maprespsrc);
			JChannel channeldest = Utils.getChannelTaskExecutor(jobid, networkaddress2, port2, mapreqdest, maprespdest);
			// Send a message to the channel to trigger message reception
			mapreqdest.put("1", WhoIsResponse.STATUS.COMPLETED);
			mapreqdest.put("2", WhoIsResponse.STATUS.RUNNING);
			mapreqdest.put("3", WhoIsResponse.STATUS.YETTOSTART);
			Utils.whoare(channelsrc);
			// For example, create a WhoIsRequest message and send it to the channel

			// Assert that the response map has been updated as expected
			// Assert other expected behaviors or states
			while (!maprespsrc.containsKey("1")) {
				Thread.sleep(1000);
			}
			assertNotNull(maprespsrc.get("1"));
			assertTrue(maprespsrc.get("1") == WhoIsResponse.STATUS.COMPLETED);
			assertNotNull(maprespsrc.get("2"));
			assertTrue(maprespsrc.get("2") == WhoIsResponse.STATUS.RUNNING);
			assertNotNull(maprespsrc.get("3"));
			assertTrue(maprespsrc.get("3") == WhoIsResponse.STATUS.YETTOSTART);
			channelsrc.close();
			channeldest.close();
		} catch (Exception ex) {
			// Handle any exceptions or failures
			// Fail the test if necessary
		}

	}

	@Test
	public void validGCStatus() {
		// Simulate a garbage collection event by manually triggering it
		System.gc();

		try {
			String gcStats = Utils.getGCStats();

			// Assert that the returned GC stats string contains valid information
			assertTrue(gcStats.contains("Garbage Collections: "));
			assertTrue(gcStats.contains("Garbage Collection Time (ms): "));
		} catch (Exception ex) {
			// Handle any exceptions or failures
			// Fail the test if necessary
		}

	}

	@Test
	public void validGCStatusWithoutGC() {
		try {
			String gcStats = Utils.getGCStats();

			// Assert that the returned GC stats string contains valid information
			assertTrue(gcStats.contains("Garbage Collections: "));
			assertTrue(gcStats.contains("Garbage Collection Time (ms): "));
		} catch (Exception ex) {
			// Handle any exceptions or failures
			// Fail the test if necessary
		}

	}

	@Test
	public void validGCStatusMultipleGCInvoke() {
		// Simulate a garbage collection event by manually triggering it
		for (int i = 0;i < 5;i++) {
			System.gc();
		}

		try {
			String gcStats = Utils.getGCStats();

			// Assert that the returned GC stats string contains valid information
			assertTrue(gcStats.contains("Garbage Collections: "));
			assertTrue(gcStats.contains("Garbage Collection Time (ms): "));
		} catch (Exception ex) {
			// Handle any exceptions or failures
			// Fail the test if necessary
		}

	}

	@Test
	public void testGetResultObjectByInput() throws Exception {
		String hostAndPort = "localhost_12345"; // Provide a valid host and port
		Object inputObject = new Coalesce<>(); // Provide a valid input object
		String jobid = "job123"; // Provide a valid job ID
		Registry server = Utils.getRPCRegistry(12345, new StreamDataCruncher() {

			@Override
			public Object postObject(Object object) throws RemoteException {
				return object;
			}
		}, jobid);
		try {
			Object resultObject = Utils.getResultObjectByInput(hostAndPort, inputObject, jobid);

			// Assert that the resultObject is not null and matches the expected result
			assertNotNull(resultObject);
			assertTrue(resultObject instanceof Coalesce);
		} catch (Exception ex) {
			// Handle any exceptions or failures
			// Fail the test if necessary
		}

	}

	@Test
	public void testInvalidRemoteServer() {
		String hostAndPort = "invalidhost_12346"; // Provide an invalid host and port
		Object inputObject = new Coalesce<>(); // Provide a valid input object
		String jobid = "job123"; // Provide a valid job ID

		try {
			Utils.getResultObjectByInput(hostAndPort, inputObject, jobid);
			// Fail the test if the code doesn't throw an exception as expected
		} catch (Exception ex) {
			// Assert that the exception message or type indicates the server is not available
			assertTrue(ex.getMessage().contains("Unknown host: invalidhost"));
		}

	}

	@Test
	public void testNullHostAndPort() {
		String hostAndPort = null; // Provide an invalid host and port
		Object inputObject = new Coalesce<>(); // Provide a valid input object
		String jobid = "job123"; // Provide a valid job ID

		try {
			Utils.getResultObjectByInput(hostAndPort, inputObject, jobid);
			// Fail the test if the code doesn't throw an exception as expected
		} catch (Exception ex) {
			// Assert that the exception message or type indicates the server is not available
			assertTrue(ex.getMessage().contains("because \"hp\" is null"));
		}

	}

	@Test
	public void testNullInputObject() throws Exception {
		String hostAndPort = "localhost_12344"; // Provide a valid host and port
		Object inputObject = null; // Provide a valid input object
		String jobid = "job123"; // Provide a valid job ID
		Registry server = Utils.getRPCRegistry(12344, new StreamDataCruncher() {

			@Override
			public Object postObject(Object object) throws RemoteException {
				return object;
			}
		}, jobid);

		try {
			assertNull(Utils.getResultObjectByInput(hostAndPort, inputObject, jobid));
			// Fail the test if the code doesn't throw an exception as expected
		} catch (Exception ex) {
			// Assert that the exception message or type indicates the server is not available
		}

	}

	@Test
	public void testRPCEmptyJobId() throws Exception {
		String hostAndPort = "localhost_12342"; // Provide a valid host and port
		Object inputObject = new Coalesce<>(); // Provide a valid input object
		String jobid = ""; // Provide a valid job ID
		Registry server = Utils.getRPCRegistry(12342, new StreamDataCruncher() {

			@Override
			public Object postObject(Object object) throws RemoteException {
				return object;
			}
		}, jobid);

		try {
			Object returnedObject = Utils.getResultObjectByInput(hostAndPort, inputObject, jobid);
			assertNotNull(returnedObject);
			assertTrue(returnedObject instanceof Coalesce);
			// Fail the test if the code doesn't throw an exception as expected
		} catch (Exception ex) {
			// Assert that the exception message or type indicates the server is not available
		}

	}

	@Test
	public void testKryoImmutableCollection() {
		var baos = new ByteArrayOutputStream();
		Output output = new Output(baos);
		Kryo kryo = Utils.getKryoInstance();
		ImmutableCollection<Integer> col = ImmutableList.copyOf(Arrays.asList(1, 2, 3, 4));
		kryo.writeClassAndObject(output, col);
		output.flush();
		output.close();

		// Example of deserialization
		var bais = new ByteArrayInputStream(baos.toByteArray());
		Input input = new Input(bais);
		Object deserializedCollection = kryo.readClassAndObject(input);
		assertNotNull(deserializedCollection);
		input.close();
	}

	@Test
	public void testPODCIDRToNodeMapping() {
		Optional<String> nodeip = Utils.getNodeIPByPodIP("10.244.0.15");
		assertNotNull(nodeip);
		assertTrue(nodeip.isPresent());
		assertNotNull(nodeip.get());
	}
	
	
}
