/*
 * Copyright 2021 the original author or authors. <p> Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at <p> https://www.apache.org/licenses/LICENSE-2.0 <p> Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */
package com.github.datasamudaya.common.utils;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.awt.AWTException;
import java.awt.Image;
import java.awt.MenuItem;
import java.awt.PopupMenu;
import java.awt.SystemTray;
import java.awt.Toolkit;
import java.awt.TrayIcon;
import java.awt.TrayIcon.MessageType;
import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.invoke.SerializedLambda;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.curator.framework.recipes.queue.SimpleDistributedQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.burningwave.core.assembler.StaticComponentContainer;
import org.ehcache.Cache;
import org.jgrapht.Graph;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.io.ComponentNameProvider;
import org.jgrapht.io.DOTExporter;
import org.jgrapht.io.ExportException;
import org.jgrapht.io.GraphExporter;
import org.jgroups.Address;
import org.jgroups.JChannel;
import org.jgroups.Message;
import org.jgroups.ObjectMessage;
import org.jgroups.Receiver;
import org.jgroups.View;
import org.jgroups.util.UUID;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
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
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.openjdk.jol.info.GraphLayout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.ollama.OllamaChatClient;
import org.springframework.ai.ollama.api.OllamaApi;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.yarn.client.CommandYarnClient;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.ClosureSerializer.Closure;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.esotericsoftware.kryo.serializers.DefaultArraySerializers.ByteArraySerializer;
import com.esotericsoftware.kryo.serializers.DefaultArraySerializers.StringArraySerializer;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.EnumSerializer;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.datasamudaya.common.AllocateContainers;
import com.github.datasamudaya.common.Block;
import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.ContainerException;
import com.github.datasamudaya.common.ContainerLaunchAttributes;
import com.github.datasamudaya.common.ContainerResources;
import com.github.datasamudaya.common.Context;
import com.github.datasamudaya.common.DAGEdge;
import com.github.datasamudaya.common.DataCruncherContext;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaNodesResources;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.DataSamudayaUsers;
import com.github.datasamudaya.common.DestroyContainer;
import com.github.datasamudaya.common.DestroyContainers;
import com.github.datasamudaya.common.Dummy;
import com.github.datasamudaya.common.EXECUTORTYPE;
import com.github.datasamudaya.common.EntityRefStop;
import com.github.datasamudaya.common.FieldCollationDirection;
import com.github.datasamudaya.common.GlobalContainerAllocDealloc;
import com.github.datasamudaya.common.GlobalContainerLaunchers;
import com.github.datasamudaya.common.GlobalJobFolderBlockLocations;
import com.github.datasamudaya.common.GlobalYARNResources;
import com.github.datasamudaya.common.Job;
import com.github.datasamudaya.common.Job.JOBTYPE;
import com.github.datasamudaya.common.JobConfiguration;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.LaunchContainers;
import com.github.datasamudaya.common.NodeIndexKey;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.PipelineConstants;
import com.github.datasamudaya.common.RemoteDataFetch;
import com.github.datasamudaya.common.RemoteDataFetcher;
import com.github.datasamudaya.common.Resources;
import com.github.datasamudaya.common.ShufflePort;
import com.github.datasamudaya.common.Stage;
import com.github.datasamudaya.common.StreamDataCruncher;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.TaskInfoYARN;
import com.github.datasamudaya.common.TaskStatus;
import com.github.datasamudaya.common.Tuple2Serializable;
import com.github.datasamudaya.common.User;
import com.github.datasamudaya.common.WhoAreRequest;
import com.github.datasamudaya.common.WhoAreResponse;
import com.github.datasamudaya.common.WhoIsRequest;
import com.github.datasamudaya.common.WhoIsResponse;
import com.github.datasamudaya.common.exceptions.JGroupsException;
import com.github.datasamudaya.common.exceptions.JobException;
import com.github.datasamudaya.common.exceptions.OutputStreamException;
import com.github.datasamudaya.common.exceptions.PropertiesException;
import com.github.datasamudaya.common.exceptions.RpcRegistryException;
import com.github.datasamudaya.common.exceptions.TaskExecutorException;
import com.github.datasamudaya.common.exceptions.YarnLaunchException;
import com.github.datasamudaya.common.exceptions.ZookeeperException;
import com.github.datasamudaya.common.functions.Coalesce;
import com.google.common.collect.ImmutableList;
import com.sun.management.OperatingSystemMXBean;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.univocity.parsers.csv.CsvWriter;

import akka.actor.typed.ActorSystem;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import de.javakaffee.kryoserializers.ArraysAsListSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptyListSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptyMapSerializer;
import de.javakaffee.kryoserializers.CollectionsEmptySetSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonListSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonMapSerializer;
import de.javakaffee.kryoserializers.CollectionsSingletonSetSerializer;
import de.javakaffee.kryoserializers.GregorianCalendarSerializer;
import de.javakaffee.kryoserializers.JdkProxySerializer;
import de.javakaffee.kryoserializers.SynchronizedCollectionsSerializer;
import de.javakaffee.kryoserializers.UnmodifiableCollectionsSerializer;
import de.javakaffee.kryoserializers.cglib.CGLibProxySerializer;
import de.javakaffee.kryoserializers.guava.ArrayListMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ArrayTableSerializer;
import de.javakaffee.kryoserializers.guava.HashBasedTableSerializer;
import de.javakaffee.kryoserializers.guava.HashMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableListSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableSetSerializer;
import de.javakaffee.kryoserializers.guava.ImmutableTableSerializer;
import de.javakaffee.kryoserializers.guava.LinkedHashMultimapSerializer;
import de.javakaffee.kryoserializers.guava.LinkedListMultimapSerializer;
import de.javakaffee.kryoserializers.guava.ReverseListSerializer;
import de.javakaffee.kryoserializers.guava.TreeBasedTableSerializer;
import de.javakaffee.kryoserializers.guava.TreeMultimapSerializer;
import de.javakaffee.kryoserializers.guava.UnmodifiableNavigableSetSerializer;
import de.javakaffee.kryoserializers.jodatime.JodaDateTimeSerializer;
import de.javakaffee.kryoserializers.jodatime.JodaLocalDateSerializer;
import de.javakaffee.kryoserializers.jodatime.JodaLocalDateTimeSerializer;
import de.javakaffee.kryoserializers.jodatime.JodaLocalTimeSerializer;
import io.altoo.akka.serialization.kryo.serializer.scala.ScalaKryo;
import io.fabric8.kubernetes.api.model.Pod;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClientException;
import net.jpountz.lz4.LZ4BlockInputStream;
import net.jpountz.lz4.LZ4BlockOutputStream;
import net.sf.jsqlparser.parser.SimpleNode;
import net.sf.jsqlparser.schema.Table;

/**
 * 
 * @author arun Utils for adding the shutdown hook and obtaining the shuffled
 *         task executors and utilities and send and receive the objects via
 *         socket.
 */
public class Utils {
	private static final Logger log = LoggerFactory.getLogger(Utils.class);

	private Utils() {
	}

	public static OllamaChatClient ollamaChatClient;

	public static MemoryPoolMXBean mpBeanLocalToJVM;

	static {
		for (MemoryPoolMXBean mpBean : ManagementFactory.getMemoryPoolMXBeans()) {
			if (mpBean.getType() == MemoryType.HEAP && mpBean.isUsageThresholdSupported()) {
				mpBeanLocalToJVM = mpBean;
				break;
			}
		}
	}
	
	/**
	 * The function returns true when memory usage is high.
	 * @return true or false
	 */
	public static boolean isMemoryUsageHigh(double spillexceedthreshold) {
        Runtime runtime = Runtime.getRuntime();
        long usedMemory = runtime.totalMemory() - runtime.freeMemory();
        long maxMemory = runtime.maxMemory();
        double usagePercentage = (double) usedMemory / maxMemory;
        return usagePercentage > spillexceedthreshold; // 80% threshold
    }
	
	/**
	 * The function true when graph layout size exceeds memory usage threshold
	 * @param objecttoinspect
	 * @param spillexceedthreshold
	 * @return true if graph layout size exceeds memory usage threshold else false
	 */
	public static boolean isMemoryUsageLimitExceedsGraphLayoutSize(Object objecttoinspect, double spillexceedthreshold) {
		Runtime runtime = Runtime.getRuntime();
        long maxMemory = runtime.maxMemory();
        long totalsizeobject = GraphLayout.parseInstance(objecttoinspect).totalSize();
        double usagePercentage = (double) totalsizeobject / maxMemory;
        return usagePercentage >= spillexceedthreshold;
    }

	/**
	 * Shutdown hook
	 * 
	 * @param runnable
	 */
	public static void addShutdownHook(Runnable runnable) {
		log.debug("Entered Utils.addShutdownHook");
		Runtime.getRuntime().addShutdownHook(new Thread(runnable));
		log.debug("Exiting Utils.addShutdownHook");
	}

	/**
	 * Thread Local kryo instance.
	 */
	static ThreadLocal<Kryo> conf = ThreadLocal.withInitial(() -> getKryoInstance());

	/**
	 * Writes the text message to outputstream.
	 * 
	 * @param os
	 * @param message
	 * @throws Exception
	 */
	public static void writeToOstream(OutputStream os, String message) throws OutputStreamException {
		try {
			if (nonNull(os)) {
				os.write(message.getBytes());
				os.write('\n');
				os.flush();
			}
		} catch (Exception ex) {
			throw new OutputStreamException("Error In Writing message to outputstream", ex);
		}
	}

	/**
	 * This method configures the log4j properties and obtains the properties
	 * from the config folder in the binary distribution.
	 * 
	 * @param propertyfile
	 * @throws Exception
	 */
	public static void initializeProperties(String propertiesfilepath, String propertyfile) throws PropertiesException {
		log.debug("Entered Utils.initializeProperties");
		if (Objects.isNull(propertyfile)) {
			throw new PropertiesException("Property File Name cannot be null");
		}
		if (Objects.isNull(propertiesfilepath)) {
			throw new PropertiesException("Properties File Path cannot be null");
		}
		try (var fis = new FileInputStream(propertiesfilepath + propertyfile);) {
			var prop = new Properties();
			prop.load(fis);
			prop.putAll(System.getProperties());
			log.debug("Properties: {}", prop.entrySet());
			DataSamudayaProperties.put(prop);
			String containerusersshare = prop.getProperty(DataSamudayaConstants.CONTAINER_ALLOC_USERS_PLUS_SHARE);
			if (nonNull(containerusersshare)) {
				String[] cus = containerusersshare.split(",");
				int userswithshare = cus.length;
				if (userswithshare > 0 && userswithshare % 2 == 0) {
					int noofusers = userswithshare / 2;
					var userssharepercentage = new ConcurrentHashMap<String, User>();
					double sharepercentagetotal = 0;
					for (int usercount = 0;usercount < noofusers;usercount++) {
						int sharepercentage = Integer.parseInt(cus[usercount * 2 + 1]);
						String username = cus[usercount * 2];
						User user = new User(username, sharepercentage, new ConcurrentHashMap<>());
						userssharepercentage.put(username, user);
						sharepercentagetotal += sharepercentage;
					}
					if (sharepercentagetotal > 100.0) {
						throw new PropertiesException(
								"Users share total not tally and it should be less that or equal to 100.0");
					}
					DataSamudayaUsers.put(userssharepercentage);
				} else {
					throw new PropertiesException(
							"Container users share property [container.alloc.users.share] not properly formatted");
				}
			}
			burningWaveInitialization();
		} catch (Exception ex) {
			throw new PropertiesException(PropertiesException.LOADING_PROPERTIES, ex);
		}
		log.debug("Exiting Utils.initializeProperties");
	}

	/**
	 * Load Properties from class path
	 * @param propertiesfilepath
	 * @param propertyfile
	 * @throws PropertiesException
	 */
	public static void initializePropertiesClasspath(String propertiesfilepath, String propertyfile) throws PropertiesException {
		log.debug("Entered Utils.initializePropertiesClasspath");
		if (Objects.isNull(propertyfile)) {
			throw new PropertiesException("Property File Name cannot be null");
		}
		if (Objects.isNull(propertiesfilepath)) {
			throw new PropertiesException("Properties File Path cannot be null");
		}
		try (var fis = Utils.class.getResourceAsStream(propertiesfilepath + propertyfile)) {
			var prop = new Properties();
			prop.load(fis);
			prop.putAll(System.getProperties());
			log.debug("Properties: {}", prop.entrySet());
			DataSamudayaProperties.put(prop);
			String containerusersshare = prop.getProperty(DataSamudayaConstants.CONTAINER_ALLOC_USERS_PLUS_SHARE);
			if (nonNull(containerusersshare)) {
				String[] cus = containerusersshare.split(",");
				int userswithshare = cus.length;
				if (userswithshare > 0 && userswithshare % 2 == 0) {
					int noofusers = userswithshare / 2;
					var userssharepercentage = new ConcurrentHashMap<String, User>();
					double sharepercentagetotal = 0;
					for (int usercount = 0;usercount < noofusers;usercount++) {
						int sharepercentage = Integer.parseInt(cus[usercount * 2 + 1]);
						String username = cus[usercount * 2];
						User user = new User(username, sharepercentage, new ConcurrentHashMap<>());
						userssharepercentage.put(username, user);
						sharepercentagetotal += sharepercentage;
					}
					if (sharepercentagetotal > 100.0) {
						throw new PropertiesException(
								"Users share total not tally and it should be less that or equal to 100.0");
					}
					DataSamudayaUsers.put(userssharepercentage);
				} else {
					throw new PropertiesException(
							"Container users share property [container.alloc.users.share] not properly formatted");
				}
			}
			burningWaveInitialization();
		} catch (Exception ex) {
			throw new PropertiesException(PropertiesException.LOADING_PROPERTIES, ex);
		}
		log.debug("Exiting Utils.initializePropertiesClasspath");
	}

	/**
	 * Burning Wave Initialized with properties file
	 */
	public static void burningWaveInitialization() {
		try {
			StaticComponentContainer.Configuration.Default
					.setFileName(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.BURNINGWAVE_PROPERTIES,
							DataSamudayaConstants.BURNINGWAVE_PROPERTIES_DEFAULT));
		} catch (UnsupportedOperationException uoe) {
		}
	}

	/**
	 * Gets the kryo object from thread local.
	 * 
	 * @return
	 */
	public static Kryo getKryo() {
		return conf.get();
	}

	/**
	 * The function configures scala kryo for akka clusters
	 * 
	 * @param kryo
	 */
	public static void configureScalaKryo(ScalaKryo kryo) {
		kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
		kryo.setReferences(true);
		kryo.setRegistrationRequired(false);
		kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
		kryo.setOptimizedGenerics(true);
		kryo.register(Arrays.asList("").getClass(), new ArraysAsListSerializer());
		kryo.register(Collections.EMPTY_LIST.getClass(), new CollectionsEmptyListSerializer());
		kryo.register(Collections.EMPTY_MAP.getClass(), new CollectionsEmptyMapSerializer());
		kryo.register(Collections.EMPTY_SET.getClass(), new CollectionsEmptySetSerializer());
		kryo.register(Collections.singletonList("").getClass(), new CollectionsSingletonListSerializer());
		kryo.register(Collections.singleton("").getClass(), new CollectionsSingletonSetSerializer());
		kryo.register(Collections.singletonMap("", "").getClass(), new CollectionsSingletonMapSerializer());
		kryo.register(GregorianCalendar.class, new GregorianCalendarSerializer());
		kryo.register(InvocationHandler.class, new JdkProxySerializer());
		UnmodifiableCollectionsSerializer.registerSerializers(kryo);
		SynchronizedCollectionsSerializer.registerSerializers(kryo);

		// custom serializers for non-jdk libs

		// register CGLibProxySerializer, works in combination with the
		// appropriate action in handleUnregisteredClass (see below)
		kryo.register(CGLibProxySerializer.CGLibProxyMarker.class, new CGLibProxySerializer());
		// joda DateTime, LocalDate, LocalDateTime and LocalTime
		kryo.register(DateTime.class, new JodaDateTimeSerializer());
		kryo.register(LocalDate.class, new JodaLocalDateSerializer());
		kryo.register(LocalDateTime.class, new JodaLocalDateTimeSerializer());
		kryo.register(LocalDateTime.class, new JodaLocalTimeSerializer());
		ImmutableListSerializer.registerSerializers(kryo);
		ImmutableSetSerializer.registerSerializers(kryo);
		ImmutableMapSerializer.registerSerializers(kryo);
		ImmutableMultimapSerializer.registerSerializers(kryo);
		ImmutableTableSerializer.registerSerializers(kryo);
		ReverseListSerializer.registerSerializers(kryo);
		UnmodifiableNavigableSetSerializer.registerSerializers(kryo);
		ArrayListMultimapSerializer.registerSerializers(kryo);
		HashMultimapSerializer.registerSerializers(kryo);
		LinkedHashMultimapSerializer.registerSerializers(kryo);
		LinkedListMultimapSerializer.registerSerializers(kryo);
		TreeMultimapSerializer.registerSerializers(kryo);
		ArrayTableSerializer.registerSerializers(kryo);
		HashBasedTableSerializer.registerSerializers(kryo);
		TreeBasedTableSerializer.registerSerializers(kryo);
		kryo.register(ImmutableList.copyOf(SqlTypeFamily.DATETIME_INTERVAL.getTypeNames()).getClass(),
				new ImmutableListSerializer());
		kryo.register(Object.class);
		kryo.register(Object[].class);
		kryo.register(Object[][].class);
		kryo.register(byte.class);
		kryo.register(byte[].class, new ByteArraySerializer());
		kryo.register(String[].class, new StringArraySerializer());
		kryo.register(Integer[].class);
		kryo.register(Long[].class);
		kryo.register(Float[].class);
		kryo.register(Double[].class);
		kryo.register(Vector.class);
		kryo.register(DiskSpillingList.class,
				new CompatibleFieldSerializer<DiskSpillingList>(kryo, DiskSpillingList.class));
		kryo.register(DiskSpillingSet.class,
				new CompatibleFieldSerializer<DiskSpillingSet>(kryo, DiskSpillingSet.class));
		kryo.register(ArrayList.class);
		kryo.register(HashMap.class);
		kryo.register(ConcurrentHashMap.class);
		kryo.register(LinkedHashSet.class);
		kryo.register(HashSet.class);
		kryo.register(WhoIsResponse.class);
		kryo.register(WhoIsRequest.class);
		kryo.register(WhoAreRequest.class);
		kryo.register(WhoAreResponse.class);
		kryo.register(Tuple2Serializable.class);
		kryo.register(WhoIsResponse.STATUS.class, new EnumSerializer(WhoIsResponse.STATUS.class));
		kryo.register(Coalesce.class, new CompatibleFieldSerializer<Coalesce>(kryo, Coalesce.class));
		kryo.register(JobStage.class, new CompatibleFieldSerializer<JobStage>(kryo, JobStage.class));
		kryo.register(Stage.class, new CompatibleFieldSerializer<Stage>(kryo, Stage.class));
		kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
		kryo.register(Table.class, new CompatibleFieldSerializer<Table>(kryo, Table.class));
		kryo.register(SimpleNode.class, new CompatibleFieldSerializer<SimpleNode>(kryo, SimpleNode.class));
		kryo.register(SerializedLambda.class);
		kryo.register(Tuple1.class, new TupleSerializer());
		kryo.register(Tuple2.class, new TupleSerializer());
		kryo.register(Tuple3.class, new TupleSerializer());
		kryo.register(Tuple4.class, new TupleSerializer());
		kryo.register(Tuple5.class, new TupleSerializer());
		kryo.register(Tuple6.class, new TupleSerializer());
		kryo.register(Tuple7.class, new TupleSerializer());
		kryo.register(Tuple8.class, new TupleSerializer());
		kryo.register(Tuple9.class, new TupleSerializer());
		kryo.register(Tuple10.class, new TupleSerializer());
		kryo.register(Tuple11.class, new TupleSerializer());
		kryo.register(Tuple12.class, new TupleSerializer());
		kryo.register(Tuple13.class, new TupleSerializer());
		kryo.register(Tuple14.class, new TupleSerializer());
		kryo.register(Tuple15.class, new TupleSerializer());
		kryo.register(Tuple16.class, new TupleSerializer());
		kryo.register(Closure.class, new ClosureSerializer());
		kryo.register(RexNode.class, new CompatibleFieldSerializer<RexNode>(kryo, RexNode.class));
		kryo.register(RemoteDataFetch.class, new CompatibleFieldSerializer<RemoteDataFetch>(kryo, RemoteDataFetch.class));
	}

	/**
	 * Gets the kryo instance by registering the required objects for
	 * serialization.
	 * 
	 * @return kryo instance
	 */
	public static Kryo getKryoInstance() {
		Kryo kryo = new Kryo();
		kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
		kryo.setReferences(true);
		kryo.setRegistrationRequired(false);
		kryo.setOptimizedGenerics(true);
		kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
		kryo.register(Arrays.asList("").getClass(), new ArraysAsListSerializer());
		kryo.register(Collections.EMPTY_LIST.getClass(), new CollectionsEmptyListSerializer());
		kryo.register(Collections.EMPTY_MAP.getClass(), new CollectionsEmptyMapSerializer());
		kryo.register(Collections.EMPTY_SET.getClass(), new CollectionsEmptySetSerializer());
		kryo.register(Collections.singletonList("").getClass(), new CollectionsSingletonListSerializer());
		kryo.register(Collections.singleton("").getClass(), new CollectionsSingletonSetSerializer());
		kryo.register(Collections.singletonMap("", "").getClass(), new CollectionsSingletonMapSerializer());
		kryo.register(GregorianCalendar.class, new GregorianCalendarSerializer());
		kryo.register(InvocationHandler.class, new JdkProxySerializer());
		UnmodifiableCollectionsSerializer.registerSerializers(kryo);
		SynchronizedCollectionsSerializer.registerSerializers(kryo);

		// custom serializers for non-jdk libs

		// register CGLibProxySerializer, works in combination with the
		// appropriate action in handleUnregisteredClass (see below)
		kryo.register(CGLibProxySerializer.CGLibProxyMarker.class, new CGLibProxySerializer());
		// joda DateTime, LocalDate, LocalDateTime and LocalTime
		kryo.register(DateTime.class, new JodaDateTimeSerializer());
		kryo.register(LocalDate.class, new JodaLocalDateSerializer());
		kryo.register(LocalDateTime.class, new JodaLocalDateTimeSerializer());
		kryo.register(LocalDateTime.class, new JodaLocalTimeSerializer());
		ImmutableListSerializer.registerSerializers(kryo);
		ImmutableSetSerializer.registerSerializers(kryo);
		ImmutableMapSerializer.registerSerializers(kryo);
		ImmutableMultimapSerializer.registerSerializers(kryo);
		ImmutableTableSerializer.registerSerializers(kryo);
		ReverseListSerializer.registerSerializers(kryo);
		UnmodifiableNavigableSetSerializer.registerSerializers(kryo);
		ArrayListMultimapSerializer.registerSerializers(kryo);
		HashMultimapSerializer.registerSerializers(kryo);
		LinkedHashMultimapSerializer.registerSerializers(kryo);
		LinkedListMultimapSerializer.registerSerializers(kryo);
		TreeMultimapSerializer.registerSerializers(kryo);
		ArrayTableSerializer.registerSerializers(kryo);
		HashBasedTableSerializer.registerSerializers(kryo);
		TreeBasedTableSerializer.registerSerializers(kryo);
		kryo.register(ImmutableList.copyOf(SqlTypeFamily.DATETIME_INTERVAL.getTypeNames()).getClass(),
				new ImmutableListSerializer());
		kryo.register(Object.class);
		kryo.register(Object[].class);
		kryo.register(Object[][].class);
		kryo.register(byte.class);
		kryo.register(byte[].class, new ByteArraySerializer());
		kryo.register(String[].class, new StringArraySerializer());
		kryo.register(Integer[].class);
		kryo.register(Long[].class);
		kryo.register(Float[].class);
		kryo.register(Double[].class);
		kryo.register(Vector.class);
		kryo.register(DiskSpillingList.class,
				new CompatibleFieldSerializer<DiskSpillingList>(kryo, DiskSpillingList.class));
		kryo.register(DiskSpillingSet.class,
				new CompatibleFieldSerializer<DiskSpillingSet>(kryo, DiskSpillingSet.class));
		kryo.register(ArrayList.class);
		kryo.register(HashMap.class);
		kryo.register(ConcurrentHashMap.class);
		kryo.register(LinkedHashSet.class);
		kryo.register(HashSet.class);
		kryo.register(WhoIsResponse.class);
		kryo.register(WhoIsRequest.class);
		kryo.register(WhoAreRequest.class);
		kryo.register(WhoAreResponse.class);
		kryo.register(Tuple2Serializable.class);
		kryo.register(WhoIsResponse.STATUS.class, new EnumSerializer(WhoIsResponse.STATUS.class),20000);
		kryo.register(Coalesce.class, new CompatibleFieldSerializer<Coalesce>(kryo, Coalesce.class),20001);
		kryo.register(JobStage.class, new CompatibleFieldSerializer<JobStage>(kryo, JobStage.class),20002);
		kryo.register(Stage.class, new CompatibleFieldSerializer<Stage>(kryo, Stage.class),20003);
		kryo.setInstantiatorStrategy(new DefaultInstantiatorStrategy(new StdInstantiatorStrategy()));
		kryo.register(Table.class, new CompatibleFieldSerializer<Table>(kryo, Table.class),20004);
		kryo.register(SimpleNode.class, new CompatibleFieldSerializer<SimpleNode>(kryo, SimpleNode.class),20005);
		kryo.register(SerializedLambda.class,20006);
		kryo.register(Tuple1.class, new TupleSerializer(),20007);
		kryo.register(Tuple2.class, new TupleSerializer(),20008);
		kryo.register(Tuple3.class, new TupleSerializer(),20009);
		kryo.register(Tuple4.class, new TupleSerializer(),20010);
		kryo.register(Tuple5.class, new TupleSerializer(),20011);
		kryo.register(Tuple6.class, new TupleSerializer(),20012);
		kryo.register(Tuple7.class, new TupleSerializer(),20013);
		kryo.register(Tuple8.class, new TupleSerializer(),20014);
		kryo.register(Tuple9.class, new TupleSerializer(),20015);
		kryo.register(Tuple10.class, new TupleSerializer(),20016);
		kryo.register(Tuple11.class, new TupleSerializer(),20017);
		kryo.register(Tuple12.class, new TupleSerializer(),20018);
		kryo.register(Tuple13.class, new TupleSerializer(),20019);
		kryo.register(Tuple14.class, new TupleSerializer(),20020);
		kryo.register(Tuple15.class, new TupleSerializer(),20021);
		kryo.register(Tuple16.class, new TupleSerializer(),20022);
		kryo.register(Closure.class, new ClosureSerializer(),20023);
		kryo.register(RexNode.class, new CompatibleFieldSerializer<RexNode>(kryo, RexNode.class),20024);
		kryo.register(RemoteDataFetch.class, new CompatibleFieldSerializer<RemoteDataFetch>(kryo, RemoteDataFetch.class),20025);
		return kryo;
	}

	/**
	 * This method configures the log4j properties and obtains the properties
	 * from the classpath in the binary distribution for mesos.
	 * 
	 * @param propertyfile
	 * @throws Exception
	 */
	public static void loadPropertiesMesos(String propertyfile) throws PropertiesException {
		log.debug("Entered Utils.loadPropertiesMesos");
		var prop = new Properties();
		try (var fis = Utils.class.getResourceAsStream(DataSamudayaConstants.FORWARD_SLASH + propertyfile);) {
			prop.load(fis);
			prop.putAll(System.getProperties());
			log.debug("Properties: {}", prop.entrySet());
			DataSamudayaProperties.put(prop);
		} catch (Exception ex) {
			throw new PropertiesException("Problem in loading properties, See the cause below", ex);
		}
		log.debug("Exiting Utils.loadPropertiesMesos");
	}

	/**
	 * This function creates and configures the jgroups channel object for the
	 * given input and returns it. This is used in jgroups mode of autonomous
	 * task execution with no scheduler behind it.
	 * 
	 * @param jobid
	 * @param networkaddress
	 * @param port
	 * @param mapreq
	 * @param mapresp
	 * @return jgroups channel object.
	 * @throws Exception
	 */
	public static JChannel getChannelTaskExecutor(String jobid, String networkaddress, int port,
			Map<String, WhoIsResponse.STATUS> mapreq, Map<String, WhoIsResponse.STATUS> mapresp) throws Exception {
		log.debug("Entered Utils.getChannelTaskExecutor");
		var channel = Utils.getChannelWithPStack(networkaddress);
		var kryo = getKryoInstance();
		var lock = new Object();
		if (!Objects.isNull(channel)) {
			channel.setName(networkaddress + "_" + port);
			channel.setReceiver(new Receiver() {
				String jobidl = jobid;
				Map<String, WhoIsResponse.STATUS> mapreql = mapreq;
				Map<String, WhoIsResponse.STATUS> maprespl = mapresp;

				@Override
				public void viewAccepted(View clusterview) {
					log.debug("View Accepted {}", clusterview);
				}

				@Override
				public void receive(Message msg) {
					synchronized (lock) {
						var rawbuffer = (byte[]) ((ObjectMessage) msg).getObject();
						try (var baos = new ByteArrayInputStream(rawbuffer); var input = new Input(baos)) {
							var object = kryo.readClassAndObject(input);
							if (object instanceof WhoIsRequest whoisrequest) {
								if (mapreql.containsKey(whoisrequest.getStagepartitionid())) {
									log.debug("Whois: {} Map Status: {} Map Response Status: {}",
											whoisrequest.getStagepartitionid(), mapreql, maprespl);
									whoisresp(msg, whoisrequest.getStagepartitionid(),
											mapreql.get(whoisrequest.getStagepartitionid()), channel);
								}
							} else if (object instanceof WhoIsResponse whoisresponse) {
								log.debug("WhoisResp: {} Status: {}", whoisresponse.getStagepartitionid(),
										whoisresponse.getStatus());
								maprespl.put(whoisresponse.getStagepartitionid(), whoisresponse.getStatus());
							} else if (object instanceof WhoAreRequest) {
								log.debug("WhoAreReq: ");
								whoareresponse(channel, msg.getSrc(), mapreql);
							} else if (object instanceof WhoAreResponse whoareresponse) {
								log.debug("WhoAreResp: ");
								maprespl.putAll(whoareresponse.getResponsemap());
							}
						} catch (Exception ex) {
							log.error("In JGroups Object deserialization error: ", ex);
						}
					}
				}
			});
			channel.setDiscardOwnMessages(true);
			channel.connect(jobid);
		}
		log.debug("Exiting Utils.getChannelTaskExecutor");
		return channel;
	}

	/**
	 * Request the status of the stage whoever is the executing the stage tasks.
	 * This method is used by the task executors.
	 * 
	 * @param channel
	 * @param stagepartitionid
	 * @throws Exception
	 */
	public static void whois(JChannel channel, String stagepartitionid) throws JGroupsException {
		log.debug("Entered Utils.whois");
		var whoisrequest = new WhoIsRequest();
		whoisrequest.setStagepartitionid(stagepartitionid);
		try (var baos = new ByteArrayOutputStream(); var output = new Output(baos);) {
			getKryo().writeClassAndObject(output, whoisrequest);
			output.flush();
			channel.send(new ObjectMessage(null, baos.toByteArray()));
		} catch (Exception ex) {
			throw new JGroupsException(JGroupsException.JGROUPS_PROCESSING_EXCETPION, ex);
		}
		log.debug("Exiting Utils.whois");
	}

	/**
	 * Request the status of the all the stages whoever are the executing the
	 * stage tasks. This method is used by the job scheduler in jgroups mode of
	 * stage task executions.
	 * 
	 * @param channel
	 * @throws JGroupsException
	 */
	public static void whoare(JChannel channel) throws JGroupsException {
		log.debug("Entered Utils.whoare");
		var whoarerequest = new WhoAreRequest();
		try (var baos = new ByteArrayOutputStream(); var output = new Output(baos);) {
			getKryo().writeClassAndObject(output, whoarerequest);
			output.flush();
			channel.send(new ObjectMessage(null, baos.toByteArray()));
		} catch (Exception ex) {
			throw new JGroupsException(JGroupsException.JGROUPS_PROCESSING_EXCETPION, ex);
		}
		log.debug("Exiting Utils.whoare");
	}

	/**
	 * Response of the whoare request used by the schedulers.
	 * 
	 * @param channel
	 * @param address
	 * @param maptosend
	 * @throws Exception
	 */
	public static void whoareresponse(JChannel channel, Address address, Map<String, WhoIsResponse.STATUS> maptosend)
			throws JGroupsException {
		log.debug("Entered Utils.whoareresponse");
		var whoareresp = new WhoAreResponse();
		whoareresp.setResponsemap(maptosend);
		try (var baos = new ByteArrayOutputStream(); var output = new Output(baos);) {
			getKryo().writeClassAndObject(output, whoareresp);
			output.flush();
			channel.send(new ObjectMessage(address, baos.toByteArray()));
		} catch (Exception ex) {
			throw new JGroupsException(JGroupsException.JGROUPS_PROCESSING_EXCETPION, ex);
		}
		log.debug("Exiting Utils.whoareresponse");
	}

	/**
	 * Response of the whois request used by the task executors to execute the
	 * next stage tasks.
	 * 
	 * @param msg
	 * @param stagepartitionid
	 * @param jobid
	 * @param status
	 * @param jchannel
	 * @param networkaddress
	 * @throws Exception
	 */
	public static void whoisresp(Message msg, String stagepartitionid, WhoIsResponse.STATUS status, JChannel jchannel)
			throws JGroupsException {
		log.debug("Entered Utils.whoisresp");
		var whoisresponse = new WhoIsResponse();
		whoisresponse.setStagepartitionid(stagepartitionid);
		whoisresponse.setStatus(status);
		try (var baos = new ByteArrayOutputStream(); var output = new Output(baos);) {
			getKryo().writeClassAndObject(output, whoisresponse);
			output.flush();
			jchannel.send(new ObjectMessage(msg.getSrc(), baos.toByteArray()));
		} catch (Exception ex) {
			throw new JGroupsException(JGroupsException.JGROUPS_PROCESSING_EXCETPION, ex);
		}
		log.debug("Exiting Utils.whoisresp");
	}

	/**
	 * This method stores graph information of stages in file.
	 * 
	 * @param graph
	 * @param writer
	 * @throws ExportException
	 */
	public static void renderGraphStage(Graph<Stage, DAGEdge> graph, Writer writer) throws ExportException {
		log.debug("Entered Utils.renderGraphStage");
		ComponentNameProvider<Stage> vertexIdProvider = stage -> {

			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				log.warn(DataSamudayaConstants.INTERRUPTED, e);
				// Restore interrupted state...
				Thread.currentThread().interrupt();
			} catch (Exception ex) {
				log.error("Delay Error, see cause below \n", ex);
			}
			return "" + System.currentTimeMillis();

		};
		ComponentNameProvider<Stage> vertexLabelProvider = Stage::toString;
		GraphExporter<Stage, DAGEdge> exporter = new DOTExporter<>(vertexIdProvider, vertexLabelProvider, null);
		exporter.exportGraph(graph, writer);
		var path = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.GRAPDIRPATH);
		new File(path).mkdirs();
		try (var stagegraphfile = new FileWriter(
				path + DataSamudayaProperties.get().getProperty(DataSamudayaConstants.GRAPHFILESTAGESPLANNAME)
						+ System.currentTimeMillis());) {
			stagegraphfile.write(writer.toString());
		} catch (Exception e) {
			log.error("File Write Error, see cause below \n", e);
		}
		log.debug("Exiting Utils.renderGraphStage");
	}

	/**
	 * This method stores graph information of physical execution plan in file.
	 * 
	 * @param graph
	 * @param writer
	 * @throws ExportException
	 */
	public static void renderGraphPhysicalExecPlan(Graph<Task, DAGEdge> graph, Writer writer) {
		log.debug("Entered Utils.renderGraphPhysicalExecPlan");
		ComponentNameProvider<Task> vertexIdProvider = jobstage -> {

			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				log.warn(DataSamudayaConstants.INTERRUPTED, e);
				// Restore interrupted state...
				Thread.currentThread().interrupt();
			} catch (Exception ex) {
				log.error("Delay Error, see cause below \n", ex);
			}
			return "" + System.currentTimeMillis();

		};
		ComponentNameProvider<Task> vertexLabelProvider = Task::toString;
		var exporter = new DOTExporter<Task, DAGEdge>(vertexIdProvider, vertexLabelProvider, null);
		exporter.exportGraph(graph, writer);
		var path = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.GRAPDIRPATH);
		new File(path).mkdirs();
		try (var stagegraphfile = new FileWriter(
				path + DataSamudayaProperties.get().getProperty(DataSamudayaConstants.GRAPHFILEPEPLANNAME)
						+ System.currentTimeMillis());) {
			stagegraphfile.write(writer.toString());
		} catch (Exception e) {
			log.error("File Write Error, see cause below \n", e);
		}
		log.debug("Exiting Utils.renderGraphPhysicalExecPlan");
	}

	/**
	 * This function returns the GC status.
	 * 
	 * @return garbage collectors status.
	 */
	public static String getGCStats() {
		log.debug("Entered Utils.getGCStats");
		var totalGarbageCollections = 0;
		var garbageCollectionTime = 0;
		for (var gc : ManagementFactory.getGarbageCollectorMXBeans()) {
			var count = gc.getCollectionCount();

			if (count >= 0) {
				totalGarbageCollections += count;
			}

			var time = gc.getCollectionTime();

			if (time >= 0) {
				garbageCollectionTime += time;
			}
		}
		log.debug("Exiting Utils.getGCStats");
		return "Garbage Collections: " + totalGarbageCollections + " n " + "Garbage Collection Time (ms): "
				+ garbageCollectionTime;
	}

	/**
	 * This function returns the object by socket using the host port of the
	 * server and the input object to the server.
	 * 
	 * @param hp
	 * @param inputobj
	 * @return object
	 * @throws Exception
	 */
	public static Object getResultObjectByInput(String hp, Object inputobj, String jobid) throws RpcRegistryException {
		try {
			var hostport = hp.split(DataSamudayaConstants.UNDERSCORE);
			final Registry registry = LocateRegistry.getRegistry(hostport[0], Integer.parseInt(hostport[1]));
			StreamDataCruncher cruncher = (StreamDataCruncher) registry
					.lookup(DataSamudayaConstants.BINDTESTUB + DataSamudayaConstants.HYPHEN + jobid);
			return cruncher.postObject(Utils.convertObjectToBytesCompressed(inputobj, null));
		} catch (Exception ex) {
			throw new RpcRegistryException(String.format(
					"Unable to read result Object for the input object %s from host port %s for jobid %s", inputobj, hp, jobid), ex);
		}
	}

	/**
	 * The function sends the object to remote rpc server and returns the result
	 * @param hp
	 * @param inputobj
	 * @param jobid
	 * @param cl
	 * @return The result of the Object sent to rpc server
	 * @throws RpcRegistryException
	 */
	public static Object getResultObjectByInput(String hp, Object inputobj, String jobid, ClassLoader cl) throws RpcRegistryException {
		try {
			var hostport = hp.split(DataSamudayaConstants.UNDERSCORE);
			final Registry registry = LocateRegistry.getRegistry(hostport[0], Integer.parseInt(hostport[1]));
			StreamDataCruncher cruncher = (StreamDataCruncher) registry
					.lookup(DataSamudayaConstants.BINDTESTUB + DataSamudayaConstants.HYPHEN + jobid);
			return cruncher.postObject(Utils.convertObjectToBytesCompressed(inputobj, cl));
		} catch (Exception ex) {
			throw new RpcRegistryException(String.format(
					"Unable to read result Object for the input object %s from host port %s", inputobj, hp), ex);
		}
	}

	/**
	 * This function returns the jgroups channel object.
	 * 
	 * @param bindaddr
	 * @return jgroups channel object
	 */
	public static synchronized JChannel getChannelWithPStack(String bindaddr) {
		try {
			System.setProperty(DataSamudayaConstants.BINDADDRESS, bindaddr);
			String configfilepath = System.getProperty(DataSamudayaConstants.USERDIR)
					+ DataSamudayaConstants.FORWARD_SLASH
					+ DataSamudayaProperties.get().getProperty(DataSamudayaConstants.JGROUPSCONF);
			log.debug("Composing Jgroups for address latch {} with trail {}", bindaddr, configfilepath);
			return new JChannel(configfilepath);
		} catch (Exception ex) {
			log.error("Unable to add Protocol Stack: ", ex);
		}
		return null;
	}

	/**
	 * This function returns the list of uri in string format for the list of
	 * Path objects.
	 * 
	 * @param paths
	 * @return list of uri in string format.
	 */
	public static List<String> getAllFilePaths(List<Path> paths) {
		return paths.stream().map(path -> path.toUri().toString()).toList();
	}

	/**
	 * This function returns the total length of files in long for the given
	 * file paths in hdfs.
	 * 
	 * @param hdfs
	 * @param paths
	 * @return total lengths of all files
	 * @throws IOException
	 */
	public static long getTotalLengthByFiles(FileSystem hdfs, List<Path> paths) throws IOException {
		long totallength = 0;
		for (var filepath : paths) {
			var fs = (DistributedFileSystem) hdfs;
			var dis = fs.getClient().open(filepath.toUri().getPath());
			totallength += dis.getFileLength();
			dis.close();
		}
		return totallength;
	}

	/**
	 * This function creates the jar file.
	 * 
	 * @param folder
	 * @param outputfolder
	 * @param outjarfilename
	 */
	public static void createJar(File folder, String outputfolder, String outjarfilename) {
		var manifest = new Manifest();
		manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
		try (var target = new JarOutputStream(
				new FileOutputStream(outputfolder + DataSamudayaConstants.FORWARD_SLASH + outjarfilename), manifest);) {
			add(folder, target);
			add(new File(DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
					+ DataSamudayaConstants.DIST_CONFIG_FOLDER
					+ DataSamudayaConstants.FORWARD_SLASH
					+ DataSamudayaConstants.DATASAMUDAYA_PROPERTIES)
			, target);
			add(new File(DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
					+ DataSamudayaConstants.DIST_CONFIG_FOLDER
					+ DataSamudayaConstants.FORWARD_SLASH
					+ DataSamudayaConstants.AKKACONF)
			, target);
		} catch (IOException ioe) {
			log.error("Unable to create Jar", ioe);
		}
	}

	/**
	 * Adds files to jar for the input parameters file and jar stream.
	 * 
	 * @param source
	 * @param target
	 * @throws IOException
	 */
	private static void add(File source, JarOutputStream target) throws IOException {
		BufferedInputStream in = null;
		try {
			if (source.isDirectory()) {
				for (var nestedFile : source.listFiles())
					add(nestedFile, target);
				return;
			}

			var entry = new JarEntry(source.getName());
			entry.setTime(source.lastModified());
			target.putNextEntry(entry);
			in = new BufferedInputStream(new FileInputStream(source));

			var buffer = new byte[1024];
			while (true) {
				var count = in.read(buffer);
				if (count == -1) {
					break;
				}
				target.write(buffer, 0, count);
			}
			target.closeEntry();
		} finally {
			if (in != null) {
				in.close();
			}
		}
	}

	/**
	 * Returns unique cache id.
	 * 
	 * @return
	 */
	public static String getCacheID() {
		return getUUID();
	}

	/**
	 * Get UUid
	 * 
	 * @return uuid
	 */
	public static String getUUID() {
		return UUID.randomUUID().toString();
	}

	private static final AtomicInteger uniquejobidincrement = new AtomicInteger(1);

	/**
	 * This function returns unique jobid globally for all the streaming jobs.
	 * 
	 * @return unique job id
	 */
	public static int getUniqueJobID() {
		return uniquejobidincrement.getAndIncrement();
	}

	private static final AtomicInteger uniqueappidincrement = new AtomicInteger(1);

	/**
	 * This function returns unique appid globally for all the map reduce jobs.
	 * 
	 * @return
	 */
	public static int getUniqueAppID() {
		return uniqueappidincrement.getAndIncrement();
	}

	/**
	 * Stars embedded zookeeper server for given port, number of maximum
	 * connections and tick time.
	 * 
	 * @param clientport
	 * @param numconnections
	 * @param ticktime
	 * @return Zookeeper Server factory class.
	 * @throws Exception
	 */
	public static ServerCnxnFactory startZookeeperServer(int clientport, int numconnections, int ticktime)
			throws ZookeeperException {
		try {
			var dataDirectory = System.getProperty("java.io.tmpdir");
			var dir = new File(dataDirectory, "zookeeper").getAbsoluteFile();
			var server = new ZooKeeperServer(dir, dir, ticktime);
			ServerCnxnFactory scf = ServerCnxnFactory.createFactory(new InetSocketAddress(clientport), numconnections);
			scf.startup(server);
			return scf;
		} catch (InterruptedException e) {
			log.warn(DataSamudayaConstants.INTERRUPTED, e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
		return null;
	}

	/**
	 * This function returns the current usable diskspace.
	 * 
	 * @return disk space
	 */
	@SuppressWarnings("static-access")
	public static Double usablediskspace() {
		log.debug("Entered HeartBeatServer.usablediskspace");
		var file = new File(DataSamudayaConstants.SLASH);
		var values = new ArrayList<Double>();
		var list = file.listRoots();
		for (var driver : list) {
			var driveGB = driver.getUsableSpace() / (double) DataSamudayaConstants.GB;
			values.add(driveGB);
		}
		var totalHDSize = 0d;
		for (var i = 0;i < values.size();i++) {
			totalHDSize += values.get(i);
		}
		log.debug("Exiting HeartBeatServer.usablediskspace");
		return totalHDSize;
	}

	/**
	 * This function returns the current available physical memory.
	 * 
	 * @return physical memory
	 */
	public static Long getTotalAvailablePhysicalMemory() {
		log.debug("Entered HeartBeatServer.getTotalAvailablePhysicalMemory");
		var os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
		var availablePhysicalMemorySize = os.getFreePhysicalMemorySize();
		if (nonNull(DataSamudayaProperties.get().get(DataSamudayaConstants.MEMORY_FROM_NODE))) {
			Long memoryavailableinmb = Long
					.valueOf((String) DataSamudayaProperties.get().getProperty(DataSamudayaConstants.MEMORY_FROM_NODE,
							DataSamudayaConstants.MEMORY_FROM_NODE_DEFAULT))
					* DataSamudayaConstants.MB;
			if (availablePhysicalMemorySize > memoryavailableinmb) {
				availablePhysicalMemorySize = memoryavailableinmb;
			}
		}
		log.debug("Exiting HeartBeatServer.getTotalAvailablePhysicalMemory");
		return availablePhysicalMemorySize;
	}

	/**
	 * This function returns the current available processors.
	 * 
	 * @return physical memory
	 */
	public static int getAvailableProcessors() {
		log.debug("Entered HeartBeatServer.getAvailableProcessors");
		int processors = Runtime.getRuntime().availableProcessors();
		if (nonNull(DataSamudayaProperties.get().get(DataSamudayaConstants.CPU_FROM_NODE))) {
			int availableprocessors = Integer.parseInt(DataSamudayaProperties.get()
					.getProperty(DataSamudayaConstants.CPU_FROM_NODE, DataSamudayaConstants.CPU_FROM_NODE_DEFAULT));
			if (availableprocessors < processors) {
				processors = availableprocessors;
			}
		}
		log.debug("Exiting HeartBeatServer.getAvailableProcessors");
		return processors;
	}

	/**
	 * This function returns the total physical memory.
	 * 
	 * @return physical memory
	 */
	public static Long getPhysicalMemory() {
		log.debug("Entered HeartBeatServer.getPhysicalMemory");
		var os = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
		var physicalMemorySize = os.getTotalPhysicalMemorySize();
		log.debug("Exiting HeartBeatServer.getPhysicalMemory");
		return physicalMemorySize;
	}

	/**
	 * This function returns the current total diskspace.
	 * 
	 * @return disk space
	 */
	public static Double totaldiskspace() {
		log.debug("Entered HeartBeatServer.totaldiskspace");
		var file = new File(DataSamudayaConstants.SLASH);
		var values = new ArrayList<Double>();
		var list = file.listRoots();
		for (var driver : list) {
			var driveGB = driver.getTotalSpace() / (double) DataSamudayaConstants.GB;
			values.add(driveGB);
		}
		var totalHDSize = 0d;
		for (var i = 0;i < values.size();i++) {
			totalHDSize += values.get(i);
		}
		log.debug("Exiting HeartBeatServer.totaldiskspace");
		return totalHDSize;
	}

	/**
	 * This method writes the input stream to hdfs for the given file path.
	 * 
	 * @param hdfsurl
	 * @param filepath
	 * @param is
	 * @throws Exception
	 */
	public static void writeResultToHDFS(String hdfsurl, String filepath, InputStream is) throws Exception {
		try (var hdfs = FileSystem.get(new URI(hdfsurl), new Configuration());
				BufferedWriter bw = new BufferedWriter(
						new OutputStreamWriter(hdfs.create(new Path(hdfsurl + filepath),
								Short.parseShort(DataSamudayaProperties.get().getProperty(
										DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
										DataSamudayaConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)))));
				var in = new Input(is);) {
			while (in.available() > 0) {
				Object result = getKryo().readClassAndObject(in);
				if (result instanceof List res) {
					for (var value : res) {
						bw.write(value.toString());
						bw.write(DataSamudayaConstants.NEWLINE);
					}
				} else {
					bw.write(result.toString());
				}
			}
			bw.flush();
		} catch (IOException ioe) {
		} catch (Exception e) {
			log.error(PipelineConstants.FILEIOERROR, e);
			throw new Exception(PipelineConstants.FILEIOERROR, e);
		}

	}

	/**
	 * This function returns the jobid-stageid-taskid for the rdf object as
	 * input.
	 * 
	 * @param rdf
	 * @return jobid-stageid-taskid
	 * @throws Exception
	 */
	public static String getIntermediateInputStreamRDF(RemoteDataFetch rdf) throws Exception {
		log.debug("Entered Utils.getIntermediateInputStreamRDF");
		var path = rdf.getJobid() + DataSamudayaConstants.HYPHEN + rdf.getStageid() + DataSamudayaConstants.HYPHEN
				+ rdf.getTaskid();
		log.debug("Returned Utils.getIntermediateInputStreamRDF");
		return path;
	}

	/**
	 * This function returns the jobid-stageid-taskid for the task object as
	 * input.
	 * 
	 * @param task
	 * @return jobid-stageid-taskid
	 * @throws Exception
	 */
	public static String getIntermediateInputStreamTask(Task task) {
		log.debug("Entered Utils.getIntermediateInputStreamTask");
		var path = task.jobid + DataSamudayaConstants.HYPHEN + task.stageid + DataSamudayaConstants.HYPHEN
				+ task.taskid;
		log.debug("Returned Utils.getIntermediateInputStreamTask");
		return path;
	}

	/**
	 * This function launches containers for mapreduce sql console.
	 * 
	 * @param user
	 * @param jobid
	 * @return list of launchcontainers object.
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public static synchronized List<LaunchContainers> launchContainers(String user, String jobid) throws Exception {
		var nrs = DataSamudayaNodesResources.getAllocatedResources();
		var resources = nrs.values();
		int numavailable = resources.size();
		Iterator<ConcurrentMap<String, Resources>> res = resources.iterator();
		var globallaunchcontainers = new ArrayList<LaunchContainers>();
		var usersshare = DataSamudayaUsers.get();
		if (isNull(usersshare)) {
			throw new ContainerException(PipelineConstants.USERNOTCONFIGURED.formatted(user));
		}
		PipelineConfig pc = new PipelineConfig();
		int numberofcontainerpernode = Integer.parseInt(pc.getNumberofcontainers());
		for (int container = 0;container < numavailable;container++) {
			ConcurrentMap<String, Resources> userrestolaunch = res.next();
			Resources restolaunch = userrestolaunch.get(user);
			var cpu = restolaunch.getNumberofprocessors();
			var usershare = usersshare.get(user);
			if (isNull(usershare)) {
				throw new ContainerException(PipelineConstants.USERNOTCONFIGURED.formatted(user));
			}
			cpu = cpu / numberofcontainerpernode;
			if (cpu <= 0) {
				throw new ContainerException(PipelineConstants.INSUFFCPUALLOCATIONERROR);
			}
			var actualmemory = restolaunch.getFreememory();
			if (actualmemory < (128 * DataSamudayaConstants.MB)) {
				throw new ContainerException(PipelineConstants.MEMORYALLOCATIONERROR);
			}
			var memoryrequire = actualmemory;
			var heapmem = memoryrequire * Integer.parseInt(DataSamudayaProperties.get()
					.getProperty(DataSamudayaConstants.HEAP_PERCENTAGE, DataSamudayaConstants.HEAP_PERCENTAGE_DEFAULT))
					/ 100;

			var directmem = (memoryrequire - heapmem) / numberofcontainerpernode;
			heapmem = heapmem / numberofcontainerpernode;
			var ac = new AllocateContainers();
			ac.setJobid(jobid);
			ac.setNumberofcontainers(numberofcontainerpernode);
			List<Integer> ports = (List<Integer>) Utils.getResultObjectByInput(restolaunch.getNodeport(), ac,
					DataSamudayaConstants.EMPTY);
			if (Objects.isNull(ports)) {
				throw new ContainerException("Port Allocation Error From Container");
			}
			log.debug("Chamber alloted with node: {} amidst ports: {}", restolaunch.getNodeport(), ports);

			var cla = new ContainerLaunchAttributes();
			var crl = new ArrayList<ContainerResources>();
			for (Integer port : ports) {
				var crs = new ContainerResources();
				crs.setPort(port);
				crs.setCpu(cpu);
				crs.setMinmemory(heapmem);
				crs.setMaxmemory(heapmem);
				crs.setDirectheap(directmem);
				crs.setGctype(pc.getGctype());
				crs.setExecutortype(EXECUTORTYPE.EXECUTOR);
				crl.add(crs);
				String conthp = restolaunch.getNodeport().split(DataSamudayaConstants.UNDERSCORE)[0]
						+ DataSamudayaConstants.UNDERSCORE + port;
				GlobalContainerAllocDealloc.getHportcrs().put(conthp, crs);
				GlobalContainerAllocDealloc.getContainernode().put(conthp, restolaunch.getNodeport());
				restolaunch.setFreememory(restolaunch.getFreememory() - heapmem - directmem);
				restolaunch.setNumberofprocessors(restolaunch.getNumberofprocessors() - cpu);
			}
			DataSamudayaUsers.get().get(user).getNodecontainersmap().put(restolaunch.getNodeport(), crl);
			cla.setCr(crl);
			cla.setNumberofcontainers(numberofcontainerpernode);
			LaunchContainers lc = new LaunchContainers();
			lc.setCla(cla);
			lc.setNodehostport(restolaunch.getNodeport());
			lc.setJobid(jobid);
			List<Integer> launchedcontainerports = (List<Integer>) Utils.getResultObjectByInput(lc.getNodehostport(),
					lc, DataSamudayaConstants.EMPTY);
			globallaunchcontainers.add(lc);
			if (Objects.isNull(launchedcontainerports)) {
				throw new ContainerException("Task Executor Launch Error From Container");
			}
			int index = 0;
			while (index < launchedcontainerports.size()) {
				while (true) {
					String tehost = lc.getNodehostport().split("_")[0];
					try (var sock = new Socket(tehost, launchedcontainerports.get(index));) {
						break;
					} catch (Exception ex) {
						try {
							log.debug("Waiting for chamber {} to replete dispatch....",
									tehost + DataSamudayaConstants.UNDERSCORE + launchedcontainerports.get(index));
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							log.warn(DataSamudayaConstants.INTERRUPTED, e);
							// Restore interrupted state...
							Thread.currentThread().interrupt();
						} catch (Exception e) {
							log.error(DataSamudayaConstants.EMPTY, e);
						}
					}
				}
				index++;
			}
			log.debug("Chamber dispatched node: {} with ports: {}", restolaunch.getNodeport(), launchedcontainerports);
		}
		GlobalContainerLaunchers.put(user, jobid, globallaunchcontainers);
		return globallaunchcontainers;
	}

	/**
	 * This method launches and returns containers as per user specs
	 * 
	 * @param user
	 * @param jobid
	 * @param cpuuser
	 * @param memoryuser
	 * @param numberofcontainers
	 * @return containers
	 * @throws InterruptedException
	 * @throws ContainerException
	 * @throws RpcRegistryException
	 */
	public static List<LaunchContainers> launchContainersUserSpec(String user, String jobid, int cpuuser,
			int memoryuser, int numberofcontainers)
			throws ContainerException, InterruptedException, RpcRegistryException {
		GlobalContainerAllocDealloc.getGlobalcontainerallocdeallocsem().acquire();
		long memoryuserbytes = Long.valueOf(memoryuser) * DataSamudayaConstants.MB;
		var nrs = DataSamudayaNodesResources.getAllocatedResources();
		var resources = nrs.values();
		int numavailable = resources.size();
		Iterator<ConcurrentMap<String, Resources>> res = resources.iterator();
		var globallaunchcontainers = new ArrayList<LaunchContainers>();
		var usersshare = DataSamudayaUsers.get();
		if (isNull(usersshare)) {
			throw new ContainerException(PipelineConstants.USERNOTCONFIGURED.formatted(user));
		}
		PipelineConfig pc = new PipelineConfig();
		List<String> launchedcontainerhostports = new ArrayList<>();
		for (int container = 0;container < numavailable;container++) {
			ConcurrentMap<String, Resources> noderesmap = res.next();
			Resources restolaunch = noderesmap.get(user);
			var usershare = usersshare.get(user);
			if (isNull(usershare)) {
				throw new ContainerException(PipelineConstants.USERNOTCONFIGURED.formatted(user));
			}
			int cpu = restolaunch.getNumberofprocessors();
			cpu = cpu / numberofcontainers;
			if (cpu <= 0) {
				throw new ContainerException(PipelineConstants.INSUFFCPUALLOCATIONERROR);
			}
			cpu = cpu < cpuuser ? cpu : cpuuser;
			var actualmemory = restolaunch.getFreememory();
			if (actualmemory < (128 * DataSamudayaConstants.MB)) {
				throw new ContainerException(PipelineConstants.MEMORYALLOCATIONERROR);
			}
			var memoryrequire = actualmemory;
			memoryrequire = memoryrequire / numberofcontainers;
			memoryrequire = memoryrequire < memoryuserbytes ? memoryrequire : memoryuserbytes;
			var heapmem = memoryrequire * Integer.parseInt(DataSamudayaProperties.get()
					.getProperty(DataSamudayaConstants.HEAP_PERCENTAGE, DataSamudayaConstants.HEAP_PERCENTAGE_DEFAULT))
					/ 100;

			var directmem = memoryrequire - heapmem;
			var ac = new AllocateContainers();
			ac.setJobid(jobid);
			ac.setNumberofcontainers(numberofcontainers);
			List<Integer> ports = (List<Integer>) Utils.getResultObjectByInput(restolaunch.getNodeport(), ac,
					DataSamudayaConstants.EMPTY);
			if (Objects.isNull(ports)) {
				throw new ContainerException("Port Allocation Error From Container");
			}
			log.debug("Chamber alloted with node: {} amidst ports: {}", restolaunch.getNodeport(), ports);

			var cla = new ContainerLaunchAttributes();
			var crl = new ArrayList<ContainerResources>();
			for (Integer port : ports) {
				var crs = new ContainerResources();
				crs.setPort(port);
				crs.setCpu(cpu);
				crs.setMinmemory(heapmem);
				crs.setMaxmemory(heapmem);
				crs.setDirectheap(directmem);
				crs.setGctype(pc.getGctype());
				crs.setExecutortype(EXECUTORTYPE.EXECUTOR);
				crl.add(crs);
			}
			cla.setCr(crl);
			cla.setNumberofcontainers(numberofcontainers);
			LaunchContainers lc = new LaunchContainers();
			lc.setCla(cla);
			lc.setNodehostport(restolaunch.getNodeport());
			lc.setJobid(jobid);
			lc.setTotalnumberofcontainerperuser(numberofcontainers);
			globallaunchcontainers.add(lc);
			List<Integer> launchedcontainerports = (List<Integer>) Utils.getResultObjectByInput(lc.getNodehostport(),
					lc, DataSamudayaConstants.EMPTY);
			String containerhost = lc.getNodehostport().split(DataSamudayaConstants.UNDERSCORE)[0];
			launchedcontainerports.stream().map(port -> containerhost + DataSamudayaConstants.UNDERSCORE + port)
					.forEach(launchedcontainerhostports::add);
			if (Objects.isNull(launchedcontainerports)) {
				throw new ContainerException("Task Executor Launch Error From Container");
			}
			for (ContainerResources crs : crl) {
				String conthp = containerhost + DataSamudayaConstants.UNDERSCORE + crs.getPort();
				GlobalContainerAllocDealloc.getHportcrs().put(conthp, crs);
				GlobalContainerAllocDealloc.getContainernode().put(conthp, restolaunch.getNodeport());
				restolaunch.setFreememory(restolaunch.getFreememory() - heapmem - directmem);
				restolaunch.setNumberofprocessors(restolaunch.getNumberofprocessors() - cpu);
			}
			DataSamudayaUsers.get().get(user).getNodecontainersmap().put(restolaunch.getNodeport(), crl);
			log.debug("Chamber dispatched node: {} with ports: {}", restolaunch.getNodeport(), launchedcontainerports);
		}
		GlobalContainerAllocDealloc.getGlobalcontainerallocdeallocsem().release();
		int index = 0;
		while (index < launchedcontainerhostports.size()) {
			while (true) {
				String tehostport = launchedcontainerhostports.get(index);
				String[] tehp = tehostport.split(DataSamudayaConstants.UNDERSCORE);
				try (var sock = new Socket(tehp[0], Integer.parseInt(tehp[1]));) {
					break;
				} catch (Exception ex) {
					try {
						log.debug("Waiting for chamber {} to replete dispatch....",
								tehp[0] + DataSamudayaConstants.UNDERSCORE + tehp[1]);
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						log.warn(DataSamudayaConstants.INTERRUPTED, e);
						// Restore interrupted state...
						Thread.currentThread().interrupt();
					} catch (Exception e) {
						log.error(DataSamudayaConstants.EMPTY, e);
					}
				}
			}
			index++;
		}

		DataSamudayaMetricsExporter.getNumberOfTaskExecutorsAllocatedCounter().inc(globallaunchcontainers.size());
		GlobalContainerLaunchers.put(user, jobid, globallaunchcontainers);
		return globallaunchcontainers;
	}

	/**
	 * The function allocates driver and executor with given specs 
	 * @param user
	 * @param jobid
	 * @param cpuuser
	 * @param memoryuser
	 * @param numberofcontainers
	 * @param cpudriver
	 * @param memorydriver
	 * @return list of launched containers
	 * @throws ContainerException
	 * @throws InterruptedException
	 * @throws RpcRegistryException
	 */
	public static List<LaunchContainers> launchContainersExecutorSpecWithDriverSpec(String user, String jobid, int cpuuser,
			int memoryexecutor, int numberofcontainers, int cpudriver, int memorydriver, boolean tolaunchcontainer)
			throws ContainerException, InterruptedException, RpcRegistryException {
		try {
			GlobalContainerAllocDealloc.getGlobalcontainerallocdeallocsem().acquire();
			long memoryuserbytes = Long.valueOf(memoryexecutor) * DataSamudayaConstants.MB;
			long memorydriverbytes = Long.valueOf(memorydriver) * DataSamudayaConstants.MB;
			var nrs = DataSamudayaNodesResources.getAllocatedResources();
			var resources = nrs.values();
			var globallaunchcontainers = new ArrayList<LaunchContainers>();
			var usersshare = DataSamudayaUsers.get();
			if (isNull(usersshare)) {
				throw new ContainerException(PipelineConstants.USERNOTCONFIGURED.formatted(user));
			}
			PipelineConfig pc = new PipelineConfig();
			List<String> launchedcontainerhostports = new ArrayList<>();
			if (cpudriver > 0 && memorydriver > 0) {
				allocateDriverOrExecutor(user,
						resources, usersshare,
						1, cpudriver, memorydriverbytes, jobid,
						pc, globallaunchcontainers, launchedcontainerhostports,
						EXECUTORTYPE.DRIVER);
			}
			allocateDriverOrExecutor(user,
					resources, usersshare,
					numberofcontainers, cpuuser, memoryuserbytes, jobid,
					pc, globallaunchcontainers, launchedcontainerhostports,
					EXECUTORTYPE.EXECUTOR);
			if (tolaunchcontainer) {
				for (LaunchContainers lc : globallaunchcontainers) {
					lc.setTotalnumberofcontainerperuser(numberofcontainers);
					List<Integer> launchedcontainerports = (List<Integer>) Utils.getResultObjectByInput(lc.getNodehostport(),
							lc, DataSamudayaConstants.EMPTY);
					String containerhost = lc.getNodehostport().split(DataSamudayaConstants.UNDERSCORE)[0];
					launchedcontainerports.stream().map(port -> containerhost + DataSamudayaConstants.UNDERSCORE + port)
							.forEach(launchedcontainerhostports::add);
					if (Objects.isNull(launchedcontainerports)) {
						throw new ContainerException("Task Executor Launch Error From Container");
					}
				}
				int index = 0;
				while (index < launchedcontainerhostports.size()) {
					while (true) {
						String tehostport = launchedcontainerhostports.get(index);
						String[] tehp = tehostport.split(DataSamudayaConstants.UNDERSCORE);
						try (var sock = new Socket(tehp[0], Integer.parseInt(tehp[1]));) {
							break;
						} catch (Exception ex) {
							try {
								log.debug("Waiting for chamber {} to replete dispatch....",
										tehp[0] + DataSamudayaConstants.UNDERSCORE + tehp[1]);
								Thread.sleep(1000);
							} catch (InterruptedException e) {
								log.warn(DataSamudayaConstants.INTERRUPTED, e);
								// Restore interrupted state...
								Thread.currentThread().interrupt();
							} catch (Exception e) {
								log.error(DataSamudayaConstants.EMPTY, e);
							}
						}
					}
					index++;
				}
			}
			DataSamudayaMetricsExporter.getNumberOfTaskExecutorsAllocatedCounter().inc(globallaunchcontainers.size());
			GlobalContainerLaunchers.put(user, jobid, globallaunchcontainers);
			return globallaunchcontainers;
		} catch (InterruptedException ex) {
			log.warn(DataSamudayaConstants.INTERRUPTED, ex);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		} catch (ContainerException ex) {
			throw ex;
		} catch (Exception ex) {
			throw new ContainerException("Unable To Allocate And LaunchContainer", ex);
		} finally {
			GlobalContainerAllocDealloc.getGlobalcontainerallocdeallocsem().release();
		}
		return new ArrayList<>();		
	}

	/**
	 * allocates diver or executors with the given spec
	 * @param user
	 * @param resources
	 * @param usersshare
	 * @param numberofcontainers
	 * @param cpuuser
	 * @param memoryuserbytes
	 * @param jobid
	 * @param pc
	 * @param globallaunchcontainers
	 * @param launchedcontainerhostports
	 * @param executortype
	 * @throws ContainerException
	 * @throws RpcRegistryException
	 */
	private static void allocateDriverOrExecutor(String user,
			Collection<ConcurrentMap<String, Resources>> resources, ConcurrentMap<String, User> usersshare,
			int numberofcontainers, int cpuuser, long memoryuserbytes, String jobid,
			PipelineConfig pc, List<LaunchContainers> globallaunchcontainers,
			List<String> launchedcontainerhostports,
			EXECUTORTYPE executortype) throws ContainerException, RpcRegistryException {
		Map<String, Boolean> allresourcesallocated = new HashMap<>();
		int numofcontcount = 0;
		outer:
		while (numofcontcount < numberofcontainers) {
			Iterator<ConcurrentMap<String, Resources>> res = resources.iterator();
			while (res.hasNext()) {
				ConcurrentMap<String, Resources> noderesmap = res.next();
				Resources restolaunch = noderesmap.get(user);
				var usershare = usersshare.get(user);
				if (isNull(usershare)) {
					throw new ContainerException(PipelineConstants.USERNOTCONFIGURED.formatted(user));
				}
				int cpu = restolaunch.getNumberofprocessors();
				if (resources.size() == allresourcesallocated.size()) {
					deallocateResources(globallaunchcontainers, resources, user);
					throw new ContainerException(PipelineConstants.INSUFFICIENTRESOURCES.formatted(user, cpuuser, memoryuserbytes, executortype));
				}
				if (cpu <= 0 || cpu < cpuuser) {
					allresourcesallocated.put(restolaunch.getNodeport(), true);
					continue;
				}
				var actualmemory = restolaunch.getFreememory();
				var memoryrequire = actualmemory;
				if (actualmemory < (128 * DataSamudayaConstants.MB) || memoryrequire < memoryuserbytes) {
					allresourcesallocated.put(restolaunch.getNodeport(), true);
					continue;
				}
				var heapmem = memoryuserbytes * Integer.parseInt(DataSamudayaProperties.get()
						.getProperty(DataSamudayaConstants.HEAP_PERCENTAGE, DataSamudayaConstants.HEAP_PERCENTAGE_DEFAULT))
						/ 100;

				var directmem = memoryuserbytes - heapmem;
				var ac = new AllocateContainers();
				ac.setJobid(jobid);
				ac.setNumberofcontainers(1);
				List<Integer> ports = (List<Integer>) Utils.getResultObjectByInput(restolaunch.getNodeport(), ac,
						DataSamudayaConstants.EMPTY);
				if (Objects.isNull(ports)) {
					throw new ContainerException("Port Allocation Error From Node");
				}
				log.debug("Chamber alloted with node: {} amidst ports: {}", restolaunch.getNodeport(), ports);

				var cla = new ContainerLaunchAttributes();
				var crl = new ArrayList<ContainerResources>();
				for (Integer port : ports) {
					var crs = new ContainerResources();
					crs.setPort(port);
					crs.setCpu(cpuuser);
					crs.setMinmemory(heapmem);
					crs.setMaxmemory(heapmem);
					crs.setDirectheap(directmem);
					crs.setGctype(pc.getGctype());
					crs.setExecutortype(executortype);
					crl.add(crs);
				}
				cla.setCr(crl);
				cla.setNumberofcontainers(1);
				LaunchContainers lc = new LaunchContainers();
				lc.setCla(cla);
				lc.setNodehostport(restolaunch.getNodeport());
				lc.setJobid(jobid);
				globallaunchcontainers.add(lc);
				String containerhost = lc.getNodehostport().split(DataSamudayaConstants.UNDERSCORE)[0];
				for (ContainerResources crs : crl) {
					String conthp = containerhost + DataSamudayaConstants.UNDERSCORE + crs.getPort();
					GlobalContainerAllocDealloc.getHportcrs().put(conthp, crs);
					GlobalContainerAllocDealloc.getContainernode().put(conthp, restolaunch.getNodeport());
					restolaunch.setFreememory(restolaunch.getFreememory() - heapmem - directmem);
					restolaunch.setNumberofprocessors(restolaunch.getNumberofprocessors() - cpuuser);
				}
				if (nonNull(DataSamudayaUsers.get().get(user).getNodecontainersmap().get(restolaunch.getNodeport()))) {
					DataSamudayaUsers.get().get(user).getNodecontainersmap().get(restolaunch.getNodeport()).addAll(crl);
				} else {
					DataSamudayaUsers.get().get(user).getNodecontainersmap().put(restolaunch.getNodeport(), new ArrayList<>(crl));
				}
				numofcontcount++;
				if (executortype == EXECUTORTYPE.DRIVER || numofcontcount == numberofcontainers || resources.size() == allresourcesallocated.size()) {
					if(numofcontcount<numberofcontainers && resources.size() == allresourcesallocated.size()) {
						deallocateResources(globallaunchcontainers, resources, user);
						throw new ContainerException(PipelineConstants.INSUFFICIENTRESOURCES.formatted(user, cpuuser, memoryuserbytes, executortype));
					}
					allresourcesallocated.clear();
					break outer;
				}
			}
		}
	}
	
	/**
	 * Deallocate resources back to user resources map
	 * @param launchcontainers
	 * @param resources
	 * @param user
	 */
	private static void deallocateResources(List <LaunchContainers> launchcontainers, Collection<ConcurrentMap<String, Resources>> resources, String user) {
		for (LaunchContainers lcs : launchcontainers) {
			String[] nodehostport = lcs.getNodehostport().split(DataSamudayaConstants.UNDERSCORE);			
			Iterator<ConcurrentMap<String, Resources>> res = resources.iterator();
			for(ContainerResources cr:lcs.getCla().getCr()) {
				String conthp = nodehostport[0] + DataSamudayaConstants.UNDERSCORE + cr.getPort();
				GlobalContainerAllocDealloc.getHportcrs().remove(conthp);
				GlobalContainerAllocDealloc.getContainernode().remove(conthp);
				while (res.hasNext()) {
					ConcurrentMap<String, Resources> noderesmap = res.next();
					Resources restolaunch = noderesmap.get(user);
					if(restolaunch.getNodeport().equals(lcs.getNodehostport())); {
						restolaunch.setFreememory(restolaunch.getFreememory() + cr.getMaxmemory() + cr.getDirectheap());
						restolaunch.setNumberofprocessors(restolaunch.getNumberofprocessors() + cr.getCpu());
						break;
					}
				}
			}
		}
	}

	/**
	 * Allocate resources based on user allocation percentage for the nodes
	 * 
	 * @param resources
	 * @param userresourcesmap
	 */
	public static void allocateResourcesByUser(Resources resources, Map<String, Resources> userresourcesmap) {
		var usersshare = DataSamudayaUsers.get();
		usersshare.entrySet().stream().forEach(es -> {
			Resources resperuser = new Resources(resources.getNodeport(), resources.getTotalmemory(),
					resources.getFreememory() * es.getValue().getPercentage() / 100,
					resources.getNumberofprocessors() * es.getValue().getPercentage() / 100,
					resources.getTotaldisksize(), resources.getUsabledisksize(), resources.getPhysicalmemorysize());
			userresourcesmap.put(es.getKey(), resperuser);
		});
	}

	private static final Semaphore yarnmutex = new Semaphore(1);

	/**
	 * launches the YARN Executors creating containers
	 * 
	 * @param pipelineconfig
	 * @param cpuuser
	 * @param memoryuser
	 * @param numberofcontainers
	 * @throws InterruptedException
	 * @throws Exception
	 */
	public static void launchYARNExecutors(String teid, int cpuuser, int memoryuser, int numberofcontainers,
			String yarnappcontextfile, boolean isyarndriverrequired) throws YarnLaunchException {
		try {
			yarnmutex.acquire();
			new File(DataSamudayaConstants.LOCAL_FS_APPJRPATH).mkdirs();
			Utils.createJar(new File(DataSamudayaConstants.YARNFOLDER), DataSamudayaConstants.LOCAL_FS_APPJRPATH,
					DataSamudayaConstants.YARNOUTJAR);
			if (isyarndriverrequired) {
				numberofcontainers++;
			}
			System.setProperty("jobcount", "1");
			System.setProperty("containercount", "" + numberofcontainers);
			System.setProperty("containermemory", "" + memoryuser);
			System.setProperty("containercpu", "" + cpuuser);
			ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(
					DataSamudayaConstants.FORWARD_SLASH + yarnappcontextfile, Utils.class);
			var client = (CommandYarnClient) context.getBean(DataSamudayaConstants.YARN_CLIENT);
			client.setAppName(DataSamudayaConstants.DATASAMUDAYA);
			client.getEnvironment().put(DataSamudayaConstants.YARNDATASAMUDAYAJOBID, teid);
			client.getEnvironment().put(DataSamudayaConstants.ISDRIVERREQUIREDYARN, isyarndriverrequired + DataSamudayaConstants.EMPTY);
			ApplicationId appid = client.submitApplication(true);
			var zo = new ZookeeperOperations();
			zo.connect();
			SimpleDistributedQueue inputqueue = zo.createDistributedQueue(DataSamudayaConstants.ROOTZNODEZK
					+ DataSamudayaConstants.YARN_INPUT_QUEUE + DataSamudayaConstants.FORWARD_SLASH + teid);
			SimpleDistributedQueue outputqueue = zo.createDistributedQueue(DataSamudayaConstants.ROOTZNODEZK
					+ DataSamudayaConstants.YARN_OUTPUT_QUEUE + DataSamudayaConstants.FORWARD_SLASH + teid);
			Map<String, Object> yarnresourcesmap = new ConcurrentHashMap<>();
			yarnresourcesmap.put("appid", appid);
			yarnresourcesmap.put("client", client);
			yarnresourcesmap.put("inputqueue", inputqueue);
			yarnresourcesmap.put("outputqueue", outputqueue);
			yarnresourcesmap.put("zk", zo);
			GlobalYARNResources.setYarnResourcesByTeId(teid, yarnresourcesmap);
			var appreport = client.getApplicationReport(appid);
			while (appreport.getYarnApplicationState() != YarnApplicationState.RUNNING) {
				if (appreport.getYarnApplicationState() == YarnApplicationState.FAILED || appreport.getYarnApplicationState() == YarnApplicationState.KILLED) {
					break;
				}
				appreport = client.getApplicationReport(appid);
				Thread.sleep(1000);
			}
		} catch (InterruptedException ex) {
			log.warn(DataSamudayaConstants.INTERRUPTED, ex);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		} catch (Exception ex) {
			throw new YarnLaunchException(YarnLaunchException.YARNLAUNCHEXCEPTION, ex);
		} finally {
			yarnmutex.release();
		}
	}

	/**
	 * creates Job in HDFS to execute in YARN
	 * 
	 * @param pipelineconfig
	 * @param sptsl
	 * @param graph
	 * @param tasksptsthread
	 * @param jsidjsmap
	 * @throws Exception
	 */
	public static void createJobInHDFS(PipelineConfig pipelineconfig, List<?> sptsl,
			SimpleDirectedGraph<?, DAGEdge> graph, Map<String, ?> tasksptsthread, Map<String, JobStage> jsidjsmap,
			Job job)
			throws JobException {
		try {
			OutputStream os = pipelineconfig.getOutput();
			pipelineconfig.setOutput(null);
			new File(DataSamudayaConstants.LOCAL_FS_APPJRPATH).mkdirs();
			Utils.createJar(new File(DataSamudayaConstants.YARNFOLDER), DataSamudayaConstants.LOCAL_FS_APPJRPATH,
					DataSamudayaConstants.YARNOUTJAR);
			var yarninputfolder = DataSamudayaConstants.YARNINPUTFOLDER + DataSamudayaConstants.FORWARD_SLASH
					+ pipelineconfig.getJobid();
			RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(pipelineconfig, yarninputfolder,
					DataSamudayaConstants.MASSIVEDATA_YARNINPUT_CONFIGFILE, pipelineconfig);
			RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(sptsl, yarninputfolder,
					DataSamudayaConstants.MASSIVEDATA_YARNINPUT_DATAFILE, pipelineconfig);
			RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(graph, yarninputfolder,
					DataSamudayaConstants.MASSIVEDATA_YARNINPUT_GRAPH_FILE, pipelineconfig);
			RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(tasksptsthread, yarninputfolder,
					DataSamudayaConstants.MASSIVEDATA_YARNINPUT_TASK_FILE, pipelineconfig);
			RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(jsidjsmap, yarninputfolder,
					DataSamudayaConstants.MASSIVEDATA_YARNINPUT_JOBSTAGE_FILE, pipelineconfig);
			RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(job, yarninputfolder,
					DataSamudayaConstants.MASSIVEDATA_YARNINPUT_JOB_FILE, pipelineconfig);
			pipelineconfig.setOutput(os);
		} catch (Exception ex) {
			throw new JobException(JobException.JOBCREATIONEXCEPTION_MESSAGE, ex);
		}
	}

	/**
	 * This function creates the MR Job in HDFS
	 * 
	 * @param jobconf
	 * @param yarninputfolder
	 * @param mapclzchunkfile
	 * @param combiner
	 * @param reducer
	 * @param folderfileblocksmap
	 * @throws JobException
	 */
	public static void createJobInHDFSMR(JobConfiguration jobconf, String yarninputfolder,
			Map<String, Set<Object>> mapclzchunkfile, Set<Object> combiner, Set<?> reducer,
			Map<?, ?> folderfileblocksmap

	) throws JobException {
		try {
			OutputStream os = jobconf.getOutput();
			jobconf.setOutput(null);
			RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(mapclzchunkfile, yarninputfolder,
					DataSamudayaConstants.MASSIVEDATA_YARNINPUT_MAPPER, jobconf);
			RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(combiner, yarninputfolder,
					DataSamudayaConstants.MASSIVEDATA_YARNINPUT_COMBINER, jobconf);
			RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(reducer, yarninputfolder,
					DataSamudayaConstants.MASSIVEDATA_YARNINPUT_REDUCER, jobconf);
			RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(folderfileblocksmap, yarninputfolder,
					DataSamudayaConstants.MASSIVEDATA_YARNINPUT_FILEBLOCKS, jobconf);
			RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(jobconf, yarninputfolder,
					DataSamudayaConstants.MASSIVEDATA_YARNINPUT_CONFIGURATION, jobconf);
			jobconf.setOutput(os);
		} catch (Exception ex) {
			throw new JobException(JobException.JOBCREATIONEXCEPTION_MESSAGE, ex);
		}
	}

	/**
	 * Sends Job Information to DistributedQueue
	 * 
	 * @param teid
	 * @param jobid
	 * @throws Exception
	 */
	public static void sendJobToYARNDistributedQueue(String teid, String jobid) throws Exception {
		var objectMapper = new ObjectMapper();
		var taskInfo = new TaskInfoYARN(jobid, false, false);
		((SimpleDistributedQueue) GlobalYARNResources.getYarnResourcesByTeId(teid).get("inputqueue"))
				.offer(objectMapper.writeValueAsBytes(taskInfo));
	}

	/**
	 * Send Job Information to Distributed queue
	 * 
	 * @param zo
	 * @param jobid
	 * @throws Exception
	 */
	public static void sendJobToYARNDistributedQueue(ZookeeperOperations zo, String jobid) throws Exception {
		var objectMapper = new ObjectMapper();
		var taskInfo = new TaskInfoYARN(jobid, false, false);
		SimpleDistributedQueue inputqueue = zo.createDistributedQueue(DataSamudayaConstants.ROOTZNODEZK
				+ DataSamudayaConstants.YARN_INPUT_QUEUE + DataSamudayaConstants.FORWARD_SLASH + jobid);
		inputqueue.offer(objectMapper.writeValueAsBytes(taskInfo));
	}

	/**
	 * Sends Job Information to DistributedQueue
	 * 
	 * @param teid
	 * @param jobid
	 * @throws Exception
	 */
	public static void shutDownYARNContainer(String teid) throws Exception {
		var objectMapper = new ObjectMapper();
		var taskInfo = new TaskInfoYARN(null, true, false);
		((SimpleDistributedQueue) GlobalYARNResources.getYarnResourcesByTeId(teid).get("inputqueue"))
				.offer(objectMapper.writeValueAsBytes(taskInfo));
	}

	/**
	 * Shutdown YARN resources like container
	 * 
	 * @param zo
	 * @param jobid
	 * @throws Exception
	 */
	public static void shutDownYARNContainer(ZookeeperOperations zo, String jobid) throws Exception {
		var objectMapper = new ObjectMapper();
		var taskInfo = new TaskInfoYARN(null, true, false);
		SimpleDistributedQueue inputqueue = zo.createDistributedQueue(DataSamudayaConstants.ROOTZNODEZK
				+ DataSamudayaConstants.YARN_INPUT_QUEUE + DataSamudayaConstants.FORWARD_SLASH + jobid);
		inputqueue.offer(objectMapper.writeValueAsBytes(taskInfo));
	}

	/**
	 * get Job Status
	 * 
	 * @param teid
	 * @param jobid
	 * @return task info yarn
	 * @throws Exception
	 */
	public static TaskInfoYARN getJobOutputStatusYARNDistributedQueueBlocking(String teid) throws Exception {
		SimpleDistributedQueue outputqueue = (SimpleDistributedQueue) GlobalYARNResources.getYarnResourcesByTeId(teid)
				.get("outputqueue");
		while (outputqueue.peek() == null) {
			Thread.sleep(1000);
		}
		var objectMapper = new ObjectMapper();
		return objectMapper.readValue(outputqueue.poll(), TaskInfoYARN.class);
	}

	/**
	 * Get Submitted YARN Job Status
	 * 
	 * @param zo
	 * @param jobid
	 * @return taskinfoyarn
	 * @throws Exception
	 */
	public static TaskInfoYARN getJobOutputStatusYARNDistributedQueueBlocking(ZookeeperOperations zo, String jobid)
			throws Exception {
		var objectMapper = new ObjectMapper();
		SimpleDistributedQueue outputqueue = zo.createDistributedQueue(DataSamudayaConstants.ROOTZNODEZK
				+ DataSamudayaConstants.YARN_OUTPUT_QUEUE + DataSamudayaConstants.FORWARD_SLASH + jobid);
		while (outputqueue.peek() == null) {
			Thread.sleep(1000);
		}
		return objectMapper.readValue(outputqueue.poll(), TaskInfoYARN.class);
	}

	/**
	 * This method configures pipeline as per schedulers;
	 * 
	 * @param scheduler
	 * @param pipelineconfig
	 */
	public static void setConfigForScheduler(String scheduler, PipelineConfig pipelineconfig) {
		if (scheduler.equalsIgnoreCase(DataSamudayaConstants.JGROUPS)) {
			pipelineconfig.setLocal(DataSamudayaConstants.FALSE);
			pipelineconfig.setYarn(DataSamudayaConstants.FALSE);
			pipelineconfig.setMesos(DataSamudayaConstants.FALSE);
			pipelineconfig.setJgroups(DataSamudayaConstants.TRUE);
			pipelineconfig.setMode(DataSamudayaConstants.MODE_NORMAL);
		} else if (scheduler.equalsIgnoreCase(DataSamudayaConstants.YARN)) {
			pipelineconfig.setLocal(DataSamudayaConstants.FALSE);
			pipelineconfig.setYarn(DataSamudayaConstants.TRUE);
			pipelineconfig.setMesos(DataSamudayaConstants.FALSE);
			pipelineconfig.setJgroups(DataSamudayaConstants.FALSE);
			pipelineconfig.setMode(DataSamudayaConstants.MODE_NORMAL);
		} else if (scheduler.equalsIgnoreCase(DataSamudayaConstants.STANDALONE)) {
			pipelineconfig.setLocal(DataSamudayaConstants.FALSE);
			pipelineconfig.setYarn(DataSamudayaConstants.FALSE);
			pipelineconfig.setMesos(DataSamudayaConstants.FALSE);
			pipelineconfig.setJgroups(DataSamudayaConstants.FALSE);
			pipelineconfig.setMode(DataSamudayaConstants.MODE_NORMAL);
		} else if (scheduler.equalsIgnoreCase(DataSamudayaConstants.EXECMODE_IGNITE)) {
			pipelineconfig.setLocal(DataSamudayaConstants.FALSE);
			pipelineconfig.setYarn(DataSamudayaConstants.FALSE);
			pipelineconfig.setMesos(DataSamudayaConstants.FALSE);
			pipelineconfig.setJgroups(DataSamudayaConstants.FALSE);
			pipelineconfig.setMode(DataSamudayaConstants.MODE_DEFAULT);
		}
	}

	/**
	 * This function returns true if user exists with the allocation share
	 * percentage. returns false if no configuration for the given user exists.
	 * 
	 * @param user
	 * @return true or false
	 */
	public static Boolean isUserExists(String user) {
		var usersshare = DataSamudayaUsers.get();
		var usershare = usersshare.get(user);
		return !isNull(usershare);
	}

	/**
	 * This function returns the cpus and memory allocated for the given list of
	 * LaunchContainers.
	 * 
	 * @param containers
	 * @return resources map of cpu and memory.
	 */
	public static Map<String, Object> getAllocatedContainersResources(List<LaunchContainers> containers) {
		Long cpu = 0l;
		Long memoryinmb = 0l;
		for (LaunchContainers lc : containers) {
			for (ContainerResources cr : lc.getCla().getCr()) {
				cpu += cr.getCpu();
				memoryinmb += cr.getMaxmemory() + cr.getDirectheap();
			}
		}
		var resources = new HashMap<String, Object>();
		resources.put(DataSamudayaConstants.CPUS, cpu);
		resources.put(DataSamudayaConstants.MEM, memoryinmb / DataSamudayaConstants.MB);
		return resources;
	}

	/**
	 * Gets the rpc registry for the given port, class which implements
	 * StreamDataCruncher and id.
	 * 
	 * @param port
	 * @param streamdatacruncher
	 * @param containerid
	 * @return get rpc registry.
	 * @throws Exception
	 */
	public static Registry getRPCRegistry(int port, final StreamDataCruncher streamdatacruncher, String id)
			throws RpcRegistryException {
		try {
			StreamDataCruncher stub = (StreamDataCruncher) UnicastRemoteObject.exportObject(streamdatacruncher, port);
			Registry registry = LocateRegistry.createRegistry(port);
			registry.rebind(DataSamudayaConstants.BINDTESTUB + DataSamudayaConstants.HYPHEN + id, stub);
			return registry;
		} catch (Exception ex) {
			throw new RpcRegistryException(RpcRegistryException.REGISTRYCREATE_MESSAGE, ex);
		}
	}

	/**
	 * This method gathers the exception stacktrace in task object.
	 * 
	 * @param ex
	 * @param task
	 */
	public static void getStackTrace(Exception ex, Task task) {
		var message = new StringWriter();
		PrintWriter writer = new PrintWriter(message);
		ex.printStackTrace(writer);
		task.taskstatus = TaskStatus.FAILED;
		task.stagefailuremessage = message.toString();
	}

	/**
	 * Write integer value to scheduler
	 * 
	 * @param os
	 * @param value
	 * @throws Exception
	 */
	public static void writeInt(OutputStream os, Integer value) throws OutputStreamException {
		try (var baos = new ByteArrayOutputStream(); var output = new Output(baos);) {
			Utils.getKryo().writeClassAndObject(output, value);
			output.flush();
			byte[] values = baos.toByteArray();
			DataOutputStream dos = new DataOutputStream(os);
			dos.writeInt(values.length);
			dos.write(values);
			dos.flush();
		} catch (Exception ex) {
			throw new OutputStreamException(OutputStreamException.ERRORCAUGHT_MESSAGE, ex);
		}
	}

	/**
	 * Prints Information about Nodes and Containers
	 * 
	 * @param lcs
	 * @param out
	 */
	public static void printNodesAndContainers(List<LaunchContainers> lcs, PrintWriter out) {
		for (LaunchContainers lc : lcs) {
			out.println();
			out.println(lc.getNodehostport() + ":");
			for (ContainerResources crs : lc.getCla().getCr()) {
				out.print(DataSamudayaConstants.TAB);
				out.println(
						"cpu: " + crs.getCpu() + " memory: "
								+ (crs.getMaxmemory() / DataSamudayaConstants.MB
								+ crs.getDirectheap() / DataSamudayaConstants.MB)
								+ " mb port:" + crs.getPort());
			}
		}
	}

	/**
	 * Write bytes information to schedulers outputstream via kryo serializer.
	 * 
	 * @param os
	 * @param outbyt
	 * @throws Exception
	 */
	public static void writeDataStream(OutputStream os, byte[] outbyt) throws OutputStreamException {
		try (var baos = new ByteArrayOutputStream(); var output = new Output(baos);) {
			Utils.getKryo().writeClassAndObject(output, outbyt);
			output.flush();
			byte[] values = baos.toByteArray();
			DataOutputStream dos = new DataOutputStream(os);
			dos.writeInt(values.length);
			dos.write(values);
			dos.flush();
		} catch (Exception ex) {
			throw new OutputStreamException(OutputStreamException.ERRORCAUGHT_MESSAGE, ex);
		}
	}

	/**
	 * This method sends the sql data to the ouput stream.
	 * 
	 * @param data
	 * @param out
	 */
	public static long printTableOrError(List data, PrintWriter out, JOBTYPE jobtype) {
		if (isNull(data) || data.isEmpty()) {
			out.println("No data available to display.");
			return 0;
		}

		if (data.get(0) instanceof String errors) {
			for (Object error : data) {
				out.println(error);
			}
			return 0;
		} else if (data.get(0) instanceof DataCruncherContext dcc) {
			var contexts = (List<DataCruncherContext>) data;
			contexts.stream().forEach(context -> {
				context.keys().stream().forEach(key -> {
					List<Object[]> rec = (List<Object[]>) context.get(key);
					printTable(rec, out);
				});
			});
			return 1;
		} else if (data.get(0) instanceof Double value) {
			out.println(value);
			return 1;
		} else if (data.get(0) instanceof Integer intval) {
			out.println(intval);
			return 1;
		} else if (data.get(0) instanceof Float floatval) {
			out.println(floatval);
			return 1;
		} else if (data.get(0) instanceof Long lvalue) {
			out.println(lvalue);
			return 1;
		}

		if (jobtype == JOBTYPE.NORMAL || jobtype == JOBTYPE.PIG) {
			return printTable(data, out);
		}
		return 0;
	}

	/**
	 * Print in pig format for the pig output
	 * 
	 * @param tableData
	 * @param out
	 */
	private static void printTablePig(List<?> tableData, PrintWriter out) {
		if (tableData.isEmpty()) {
			out.println("Table is empty.");
			out.flush();
			return;
		}

		Object valuemap = tableData.get(0);
		if (valuemap instanceof Map) {
			Map<String, Object> mapforkeys = (Map<String, Object>) valuemap;
			Set<String> keys = mapforkeys.keySet();
			out.print("(");
			out.flush();
			for (String key : keys) {
				out.print(key);
				out.print(",");
				out.flush();
			}
			out.print(")");
			out.println();
			out.flush();
			List<Map<String, Object>> tableDataToPrint = (List<Map<String, Object>>) tableData;
			for (Map<String, Object> row : tableDataToPrint) {
				out.print("(");
				out.flush();
				for (String key : keys) {
					out.print(row.get(key));
					out.print(",");
					out.flush();
				}
				out.print(")");
				out.flush();
				out.println();
				out.flush();
			}
		} else if (valuemap instanceof Tuple2 tuple2) {
			Tuple2<Map<String, Object>, List<Map<String, Object>>> tup2 = tuple2;
			Set<String> keys = tup2.v1.keySet();
			out.print("(");
			out.flush();
			for (String key : keys) {
				out.print(key);
				out.print(",");
				out.flush();
			}
			out.print(")");
			out.println();
			out.flush();
			List<Tuple2<Map<String, Object>, List<Map<String, Object>>>> tableDataToPrint = (List<Tuple2<Map<String, Object>, List<Map<String, Object>>>>) tableData;
			for (Tuple2<Map<String, Object>, List<Map<String, Object>>> row : tableDataToPrint) {
				out.print("(");
				out.flush();
				for (String key : keys) {
					out.print(row.v1.get(key));
					out.print(",");
					out.flush();
				}
				out.print(")");
				out.print("(");
				out.flush();
				out.print(row.v2);
				out.flush();
				out.print(")");
				out.flush();
				out.println();
				out.flush();
			}
		}
	}

	/**
	 * Prints the result in map to output
	 * 
	 * @param tableData
	 * @param out
	 */
	public static long printTable(List<Object[]> tableData, PrintWriter out) {
		if (tableData.isEmpty()) {
			out.println("Table is empty.");
			out.flush();
			return 0;
		}

		// Print table rows
		for (Object[] rows : tableData) {
			for (Object row : rows) {
				out.printf(String.format("%%-%ds | ", 15), row);
				out.flush();
			}
			out.println();
			out.flush();
		}
		return tableData.size();
	}

	/**
	 * Obtain random port
	 * 
	 * @return randomport
	 */
	public static int getRandomPort() {
		while (true) {
			try (ServerSocket s = new ServerSocket(0);) {
				int port = s.getLocalPort();
				if (port + DataSamudayaConstants.PORT_OFFSET > 65535) {
					log.debug("port {} in use, so getting port again...", port + DataSamudayaConstants.PORT_OFFSET);
					continue;
				}
				return port;
			} catch (Exception e) {
				log.error("port in use or exceeded its limit, so getting port again...", e);
			}
		}
	}

	/**
	 * Destroys all the allocated container allocated to current job.
	 * 
	 * @throws Exception
	 */
	public static void destroyTaskExecutors(Job job) throws Exception {
		try {
			// Global semaphore to allocated and deallocate containers.
			GlobalContainerAllocDealloc.getGlobalcontainerallocdeallocsem().acquire();
			if (!Objects.isNull(job.getNodes())) {
				var nodes = job.getNodes();
				var jobcontainerids = GlobalContainerAllocDealloc.getJobcontainerids();
				var chpcres = GlobalContainerAllocDealloc.getHportcrs();
				var deallocateall = true;
				if (!Objects.isNull(job.getTaskexecutors())) {
					// Obtain containers from job
					var cids = jobcontainerids.get(job.getId());
					for (String te : job.getTaskexecutors()) {
						if (!cids.isEmpty()) {
							cids.remove(te);
							jobcontainerids.remove(te);
							var dc = new DestroyContainer();
							dc.setJobid(job.getId());
							dc.setContainerhp(te);
							// Remove the container from global container node
							// map
							String node = GlobalContainerAllocDealloc.getContainernode().remove(te);
							Set<String> containers = GlobalContainerAllocDealloc.getNodecontainers().get(node);
							containers.remove(te);
							// Remove the container from the node and destroy
							// it.
							Utils.getResultObjectByInput(node, dc, DataSamudayaConstants.EMPTY);
							ContainerResources cr = chpcres.remove(te);
							Resources allocresources = DataSamudayaNodesResources.get().get(node);
							if (!job.getPipelineconfig().getContaineralloc()
									.equals(DataSamudayaConstants.CONTAINER_ALLOC_USERSHARE)) {
								long maxmemory = cr.getMaxmemory();
								long directheap = cr.getDirectheap();
								allocresources.setFreememory(allocresources.getFreememory() + maxmemory + directheap);
								allocresources
										.setNumberofprocessors(allocresources.getNumberofprocessors() + cr.getCpu());
							}
						} else {
							deallocateall = false;
						}
					}
				}
				if (deallocateall) {
					var dc = new DestroyContainers();
					dc.setJobid(job.getId());
					log.debug("Destroying Containers with id: {} for the hosts: {}", job.getId(), nodes);
					// Destroy all the containers from all the nodes
					for (var node : nodes) {
						Utils.getResultObjectByInput(node, dc, DataSamudayaConstants.EMPTY);
					}
				}
			}
		} catch (InterruptedException ex) {
			log.warn(DataSamudayaConstants.INTERRUPTED, ex);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		} catch (Exception ex) {
			throw new TaskExecutorException(TaskExecutorException.TASKEXECUTORDESTROYEXCEPTION_MESSAGE, ex);
		} finally {
			GlobalContainerAllocDealloc.getGlobalcontainerallocdeallocsem().release();
		}
	}

	/**
	 * This method destroys containers for the given user and jobid.
	 * 
	 * @param user
	 * @param jobid
	 * @throws Exception
	 */
	public static synchronized void destroyContainers(String user, String jobid) throws ContainerException {
		var dcs = new DestroyContainers();
		dcs.setJobid(jobid);
		var usersshare = DataSamudayaUsers.get();
		if (isNull(usersshare)) {
			throw new ContainerException(PipelineConstants.USERNOTCONFIGURED.formatted(user));
		}
		var lcs = GlobalContainerLaunchers.get(user, jobid);
		lcs.stream().forEach(lc -> {
			try {
				ConcurrentMap<String, Resources> nodesresallocated = DataSamudayaNodesResources.getAllocatedResources()
						.get(lc.getNodehostport());
				Resources resallocated = nodesresallocated.get(user);
				Utils.getResultObjectByInput(lc.getNodehostport(), dcs, DataSamudayaConstants.EMPTY);
				lc.getCla().getCr().stream().forEach(cr -> {
					try {
						resallocated.setFreememory(resallocated.getFreememory() + cr.getMaxmemory() + cr.getDirectheap());
						resallocated.setNumberofprocessors(resallocated.getNumberofprocessors() + cr.getCpu());
					} catch (Exception e) {
						log.error(DataSamudayaConstants.EMPTY, e);
					}
				});
			} catch (Exception e) {
				log.error(DataSamudayaConstants.EMPTY, e);
			}
		});
		DataSamudayaMetricsExporter.getNumberOfTaskExecutorsDeAllocatedCounter().inc(lcs.size());
		GlobalContainerLaunchers.remove(user, jobid);
		GlobalJobFolderBlockLocations.remove(jobid);
		Map<String, List<LaunchContainers>> jobcontainermap = GlobalContainerLaunchers.get(user);
		if (MapUtils.isEmpty(jobcontainermap)) {
			GlobalContainerLaunchers.remove(user);
		}
	}

	/**
	 * The function shuts containers for a given user, container id, node and executor
	 * @param user
	 * @param jobid
	 * @param nodehp
	 * @param executorhp
	 * @throws ContainerException
	 */
	public static synchronized void destroyContainer(String user, String jobid, String nodehp, String executorhp) throws ContainerException {
		var dc = new DestroyContainer();
		dc.setJobid(jobid);
		var usersshare = DataSamudayaUsers.get();
		if (isNull(usersshare)) {
			throw new ContainerException(PipelineConstants.USERNOTCONFIGURED.formatted(user));
		}
		String[] hostport = executorhp.split(DataSamudayaConstants.UNDERSCORE);
		int executorport = Integer.parseInt(hostport[1]) ;
		var lcs = GlobalContainerLaunchers.get(user, jobid);
		var itemtoremove = new ArrayList<Integer>();
		for(int lcindex=0;lcindex<lcs.size();lcindex++) {
			LaunchContainers lc = lcs.get(lcindex);
			if (lc.getNodehostport().equals(nodehp)) {
				try {
					ConcurrentMap<String, Resources> nodesresallocated = DataSamudayaNodesResources
							.getAllocatedResources().get(lc.getNodehostport());

					Resources resallocated = nodesresallocated.get(user);					
					ContainerResources crtodestroy = null;
					boolean iscontaineravailabletodestroy = false;
					for (ContainerResources cr : lc.getCla().getCr()) {
						if (cr.getPort() == executorport) {
							iscontaineravailabletodestroy = true;
							crtodestroy = cr;
							dc.setContainerhp(executorhp);
							Utils.getResultObjectByInput(lc.getNodehostport(), dc, DataSamudayaConstants.EMPTY);
							try {
								resallocated.setFreememory(
										resallocated.getFreememory() + cr.getMaxmemory() + cr.getDirectheap());
								resallocated.setNumberofprocessors(resallocated.getNumberofprocessors() + cr.getCpu());
							} catch (Exception e) {
								log.error(DataSamudayaConstants.EMPTY, e);
							}
							break;
						}
					}
					if(iscontaineravailabletodestroy) {
						lc.getCla().getCr().remove(crtodestroy);
						if (CollectionUtils.isEmpty(lc.getCla().getCr())) {
							itemtoremove.add(lcindex);
						}
						GlobalJobFolderBlockLocations.remove(jobid);
						break;
					}					
				} catch (Exception e) {
					log.error(DataSamudayaConstants.EMPTY, e);
				}
			}
		}
		if(!CollectionUtils.isEmpty(itemtoremove)) {
			for(Integer item:itemtoremove) {
				lcs.remove(item);
			}
		}
		DataSamudayaMetricsExporter.getNumberOfTaskExecutorsDeAllocatedCounter().inc(1);
		if(CollectionUtils.isEmpty(lcs)) {
			GlobalContainerLaunchers.remove(user, jobid);
		}		
		Map<String, List<LaunchContainers>> jobcontainermap = GlobalContainerLaunchers.get(user);
		if (MapUtils.isEmpty(jobcontainermap)) {
			GlobalContainerLaunchers.remove(user);
		}
	}
	
	/**
	 * Formats date from util date object
	 * 
	 * @param date
	 * @return formatted date
	 */
	public static String formatDate(Date date) {
		return new SimpleDateFormat("dd-MM-yyyy HH:mm:ss").format(date);
	}

	/**
	 * Generates path of intermediate result file system for given task
	 * 
	 * @param task
	 * @return path of intermediate result file system
	 * @throws Exception
	 */
	public static String getIntermediateResultFS(Task task) {
		return task.jobid + DataSamudayaConstants.HYPHEN + task.stageid + DataSamudayaConstants.HYPHEN + task.taskid;
	}

	/**
	 * Writes the map object to csv with given output stream
	 * 
	 * @param data
	 * @param ostream
	 * @throws IOException
	 * @throws IllegalAccessException
	 * @throws IllegalArgumentException
	 */
	public static void convertToCsv(List<?> data, OutputStream ostream)
			throws IOException, IllegalArgumentException, IllegalAccessException {
		log.debug("Writing data to csv Start");
		if (data.isEmpty()) {
			return;
		}
		Object firstdataobject = data.get(0);

		// Prepare CSV data
		String[] header = null;
		Field[] fields = firstdataobject.getClass().getDeclaredFields();
		boolean ismap = false;
		if (firstdataobject instanceof Map map) {
			ismap = true;
			List<String> headers = map.keySet().stream().filter(key -> !((String) key).endsWith("-count")).toList();
			header = headers.toArray(new String[0]);
		} else {
			// Get the fields of the object using reflection
			header = new String[fields.length];
			for (int i = 0;i < fields.length;i++) {
				header[i] = fields[i].getName();
			}
		}
		try (CSVPrinter printer = new CSVPrinter(new OutputStreamWriter(ostream),
				CSVFormat.DEFAULT.withHeader(header))) {
			if (ismap) {
				List<Object> rowData = new ArrayList<>();
				for (Map<String, Object> map : (List<Map>) data) {
					for (String headr : header) {
						rowData.add(map.get(headr));
					}
					printer.printRecord(rowData);
					rowData.clear();
				}
			} else {
				List<Object> rowData = new ArrayList<>();
				for (Object obj : data) {

					for (int i = 0;i < fields.length;i++) {
						fields[i].setAccessible(true);
						Object value = fields[i].get(obj);
						rowData.add(value);
					}

					printer.printRecord(rowData);
					rowData.clear();
				}
			}
		}
		log.debug("Writing data to csv End");
	}

	/**
	 * The function converts object to csv
	 * 
	 * @param obj
	 * @param printer
	 * @param colsinorder
	 * @throws Exception
	 */
	public static void convertMapToCsv(Object obj, CsvWriter writer) throws Exception {
		if (obj instanceof Map map) {
			map.keySet().parallelStream().filter(key -> !((String) key).endsWith("-count"))
					.map(header -> map.get(header)).forEachOrdered(value -> {
				writer.addValue(value);
			});
			writer.writeValuesToRow();
		} else {
			Field[] fields = obj.getClass().getDeclaredFields();
			for (int i = 0;i < fields.length;i++) {
				fields[i].setAccessible(true);
				Object value = fields[i].get(obj);
				writer.addValue(value);
			}
			writer.writeValuesToRow();
		}
	}

	/**
	 * This function returns number of bytes to process for the given blocks
	 * 
	 * @param block
	 * @return numberofbytesprocessed
	 */
	public static long numBytesBlocks(Block[] block) {
		long totalbytes = nonNull(block[0]) ? block[0].getBlockend() - block[0].getBlockstart() : 0l;
		totalbytes += nonNull(block[1]) ? block[1].getBlockend() - block[1].getBlockstart() : 0l;
		return totalbytes;
	}

	/**
	 * Color for primary and alternate
	 * 
	 * @param i
	 * @return colorvalue
	 */
	public static String getColor(int i) {
		if (i % 2 == 0) {
			return DataSamudayaProperties.get().getProperty(DataSamudayaConstants.COLOR_PICKER_PRIMARY,
					DataSamudayaConstants.COLOR_PICKER_PRIMARY_DEFAULT);
		} else {
			return DataSamudayaProperties.get().getProperty(DataSamudayaConstants.COLOR_PICKER_ALTERNATE,
					DataSamudayaConstants.COLOR_PICKER_ALTERNATE_DEFAULT);
		}
	}

	/**
	 * Get Checksum of hdfs file
	 * 
	 * @param paths
	 * @param hdfs
	 * @return checksum
	 */
	public static Map<Path, String> getCheckSum(List<Path> paths, FileSystem hdfs) {
		Map<Path, String> checksums = new ConcurrentHashMap<>();
		paths.parallelStream().forEach(path -> {
			try {
				checksums.put(path, new String(hdfs.getFileChecksum(path).toString().split(":")[1]));
			} catch (Exception e) {
				log.error(DataSamudayaConstants.EMPTY, e);
			}
		});
		return checksums;
	}

	/**
	 * Obtains index of required columns from original columns
	 * 
	 * @param requiredcols
	 * @param allcolumns
	 * @return index of required columns
	 */
	public static Integer[] indexOfRequiredColumns(List<String> requiredcols, List<String> allcolumns) {
		List<Integer> indexcolumns = new ArrayList<>();
		for (String col : requiredcols) {
			indexcolumns.add(allcolumns.indexOf(col));
		}
		return indexcolumns.toArray(new Integer[0]);
	}

	/**
	 * Estimate the initial capacity of HashMap for given number of keys;
	 * 
	 * @param numberOfKeys
	 * @return initial capacity of hash map.
	 */
	public static int calculateInitialCapacity(int numberOfKeys) {
		// Calculate the initial capacity based on the estimated number of keys
		// You can use your own logic here; a common approach is to use a power
		// of two
		int capacity = 1;
		while (capacity < numberOfKeys) {
			capacity <<= 1; // Left shift to double the value
		}
		return capacity;
	}

	/**
	 * This method forms the Local File File Path for Spilling data to disk
	 * 
	 * @param task
	 * @return local file path to temporary dir
	 */
	public static String getLocalFilePathForTask(Task task, String appendwithpath, boolean appendintermediate,
			boolean left, boolean right) {
		new File(System.getProperty(DataSamudayaConstants.TMPDIR) + DataSamudayaConstants.FORWARD_SLASH
				+ task.getJobid()).mkdirs();
		return System.getProperty(DataSamudayaConstants.TMPDIR) + DataSamudayaConstants.FORWARD_SLASH + task.getJobid()
				+ DataSamudayaConstants.FORWARD_SLASH + task.getJobid() + DataSamudayaConstants.HYPHEN
				+ task.getStageid() + DataSamudayaConstants.HYPHEN + task.getTaskid()
				+ (StringUtils.isNotEmpty(appendwithpath) ? DataSamudayaConstants.HYPHEN + appendwithpath
				: DataSamudayaConstants.EMPTY)
				+ (appendintermediate ? DataSamudayaConstants.HYPHEN + DataSamudayaConstants.INTERMEDIATE
				: DataSamudayaConstants.EMPTY)
				+ (left || right
				? left ? DataSamudayaConstants.HYPHEN + DataSamudayaConstants.INTERMEDIATEJOINLEFT
				: DataSamudayaConstants.HYPHEN + DataSamudayaConstants.INTERMEDIATEJOINRIGHT
				: DataSamudayaConstants.EMPTY)
				+ DataSamudayaConstants.DOT + DataSamudayaConstants.DATA;
	}

	/**
	 * The function returns the file path to store intermediate data generated by Map Reduce Tasks to disk
	 * @param task
	 * @return Path To Intermediate Data File generated by MR 
	 */
	public static String getLocalFilePathForMRTask(Task task, String appendwithpath) {
		new File(System.getProperty(DataSamudayaConstants.TMPDIR) + DataSamudayaConstants.FORWARD_SLASH
				+ task.getJobid()).mkdirs();
		return System.getProperty(DataSamudayaConstants.TMPDIR) + DataSamudayaConstants.FORWARD_SLASH + task.getJobid()
				+ DataSamudayaConstants.FORWARD_SLASH + task.getJobid() + DataSamudayaConstants.HYPHEN
				+ task.getStageid() + DataSamudayaConstants.HYPHEN + task.getTaskid()
				+ (StringUtils.isNotEmpty(appendwithpath) ? DataSamudayaConstants.HYPHEN + appendwithpath
				: DataSamudayaConstants.EMPTY)
				+ DataSamudayaConstants.DOT + DataSamudayaConstants.DATA;
	}

	/**
	 * The function returns folder path for given jobid
	 * 
	 * @param jobid
	 * @return folder path for given job
	 */
	public static String getFolderPathForJob(String jobid) {
		return System.getProperty(DataSamudayaConstants.TMPDIR) + DataSamudayaConstants.FORWARD_SLASH + jobid;
	}

	/**
	 * The Copy of the spilled data from one file to another file
	 * 
	 * @param dslinput
	 * @param kryo
	 * @param dslout
	 */
	public static void copySpilledDataSourceToDestination(DiskSpillingList dslinput, DiskSpillingList dslout) {
		if (isNull(dslinput.getTask().getHostport()) || dslinput.getTask().getHostport().split(DataSamudayaConstants.UNDERSCORE)[0]
				.equals(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST))) {
			Kryo kryo = Utils.getKryoInstance();
			try (FileInputStream istream = new FileInputStream(
					Utils.getLocalFilePathForTask(dslinput.getTask(), dslinput.getAppendwithpath(),
							dslinput.getAppendintermediate(), dslinput.getLeft(), dslinput.getRight()));
					var sis = new SnappyInputStream(istream);
					Input input = new Input(sis);) {
				while (input.available() > 0) {
					List records = (List) kryo.readClassAndObject(input);
					if (CollectionUtils.isNotEmpty(records)) {
						dslout.addAll(records);
					}
				}
			} catch (Exception ex) {
				log.error(DataSamudayaConstants.EMPTY, ex);
			}
		} else {
			try (RemoteIteratorClient client = new RemoteIteratorClient(dslinput.getTask()
			, dslinput.getAppendwithpath()
			, dslinput.getAppendintermediate()
			, dslinput.getLeft()
			, dslinput.getRight()
			, dslinput.getTask().getFcsc(),
					RequestType.LIST, IteratorType.DISKSPILLITERATOR, false, null)) {
				while (client.hasNext()) {

					List records = (List) Utils
							.convertBytesToObjectCompressed((byte[]) client.next(), null);

					if (CollectionUtils.isNotEmpty(records)) {
						dslout.addAll(records);
					}

				}
			} catch (Exception e) {
				log.error(DataSamudayaConstants.EMPTY, e);
			}
		}
	}

	/**
	 * Copy Disk Spilling List To Disk For Sorting
	 * @param dslinput
	 */
	public static void copyDiskSpillingListToDisk(DiskSpillingList dslinput) {
		Kryo kryo = Utils.getKryoInstance();
		try (FileOutputStream ostream = new FileOutputStream(
				Utils.getLocalFilePathForTask(dslinput.getTask(), dslinput.getAppendwithpath(),
						dslinput.getAppendintermediate(), dslinput.getLeft(), dslinput.getRight()));
				var sos = new SnappyOutputStream(ostream);
				Output output = new Output(sos);) {
			kryo.writeClassAndObject(output, dslinput.getData());
			output.flush();
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
	}

	/**
	 * If source is spilled and written in file copy to destination file
	 * 
	 * @param dslinput
	 * @param fos
	 * @throws Exception
	 */
	public static void copySpilledDataSourceToFileShuffle(DiskSpillingList dslinput, Output output) throws Exception {
		Kryo kryo = Utils.getKryoInstance();
		InputStream istream = null;
		if (nonNull(dslinput)) {
			if (isNull(dslinput.getTask().getHostport()) || dslinput.getTask().getHostport().split(DataSamudayaConstants.UNDERSCORE)[0]
					.equals(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST))) {
				log.debug("Copying from File from Local Executors {} Spilled File", dslinput.getTask().getHostport());
				istream = new FileInputStream(dslinput.getDiskfilepath());
				try (InputStream inputstream = istream;
						var sis = new SnappyInputStream(istream);
						Input input = new Input(sis);) {
					while (input.available() > 0) {
						List records = (List) kryo.readClassAndObject(input);
						kryo.writeClassAndObject(output, records);
						output.flush();
					}
				} catch (Exception ex) {
					log.error(DataSamudayaConstants.EMPTY, ex);
				}
			} else {
				log.debug("Copying from File from Remote Executors {} Spilled File", dslinput.getTask().getHostport());
				String[] hostport = dslinput.getTask().getHostport().split(DataSamudayaConstants.UNDERSCORE);
				RemoteDataFetch rdf = new RemoteDataFetch();
				rdf.setHp(dslinput.getTask().getHostport());
				rdf.setShufflefilepath(
						Utils.getFilePathRemoteDataFetch(dslinput.getTask(), dslinput.getAppendwithpath(),
								dslinput.getAppendintermediate(), dslinput.getLeft(), dslinput.getRight()));
				log.debug("Path Of Remote Data File {}", rdf.getShufflefilepath());
				rdf.setTejobid(dslinput.getTask().getTeid());
				int port = Utils.getRemoteShufflePort(dslinput.getTask().getHostport(), dslinput.getTask().getTeid());
				log.debug("Obtaining ShufflePort {} of Task Executor {}", port, dslinput.getTask().getHostport());
				Socket sock = new Socket(hostport[0], port);
				try (OutputStream outputstream = sock.getOutputStream();
						Output outputrdf = new Output(outputstream);) {
					kryo.writeClassAndObject(outputrdf, rdf);
					outputrdf.flush();
					log.debug("Written Remote Data Fetch to host {} and port {}", hostport[0], port);
					try (Socket socket = sock;
						InputStream inputstream = sock.getInputStream();
							LZ4BlockInputStream sis = new LZ4BlockInputStream(inputstream);
						Input input = new Input(sis);) {
						log.debug("Reading Data From Remote Data Fetch from host {} and port {}", hostport[0], port);
						while (true) {
							Object records = kryo.readClassAndObject(input);
							if (isNull(records) || records instanceof Dummy) {
								break;
							}
							kryo.writeClassAndObject(output, records);
							output.flush();
						}
					}
				} catch (Exception ex) {
					log.error(DataSamudayaConstants.EMPTY, ex);
				}
			}
		}
	}

	/**
	 * The method returns shuffle port of task executor
	 * 
	 * @param hostport
	 * @param teid
	 * @return shuffle port from task executors
	 */
	public static int getRemoteShufflePort(String hostport, String teid) {
		try {
			return (int) Utils.getResultObjectByInput(hostport, new ShufflePort(), teid);
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
		return 0;
	}

	/**
	 * The method gets stream of data from file
	 * 
	 * @param is
	 * @return stream of objects
	 */
	public static Stream<?> getStreamData(InputStream is) {
		try {
			return StreamSupport.stream(new Spliterators.AbstractSpliterator<Object>(Long.MAX_VALUE,
					Spliterator.IMMUTABLE | Spliterator.ORDERED | Spliterator.NONNULL) {
				InputStream istream = is;
				SnappyInputStream sisinternal = new SnappyInputStream(istream);
				Input inputinternal = new Input(sisinternal);
				Kryo kryo = Utils.getKryoInstance();
				Collection<?> intermdata = null;
				Iterator<?> iterator = null;

				public boolean tryAdvance(Consumer<? super Object> action) {
					try {
						if (isNull(intermdata) && inputinternal.available() > 0 || inputinternal.available() > 0 && !iterator.hasNext()) {
							intermdata = (Collection<?>) kryo.readClassAndObject(inputinternal);
							iterator = intermdata.iterator();
						}
						if (nonNull(iterator) && iterator.hasNext()) {
							action.accept(iterator.next());
						}
						if (inputinternal.available() > 0 || iterator.hasNext()) {
							return true;
						}
						inputinternal.close();
						sisinternal.close();
						istream.close();
						return false;
					} catch (Exception ex) {
						log.error(DataSamudayaConstants.EMPTY, ex);
					}
					return false;
				}
			}, false);
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
		return null;
	}


	/**
	 * The function returns stream of data of diskspilled whether it is local or remote server.
	 * @param dslinput
	 * @return get stream of diskspill for list
	 */
	public static Stream<?> getStreamData(DiskSpillingList dslinput) {
		try {
			return StreamSupport.stream(new Spliterators.AbstractSpliterator<Object>(Long.MAX_VALUE,
					Spliterator.IMMUTABLE | Spliterator.ORDERED | Spliterator.NONNULL) {
				InputStream istream;
				SnappyInputStream sisinternal;
				Input inputinternal;
				Collection<?> intermdata = null;
				Iterator<?> iterator = null;
				RemoteIteratorClient client = null;

				public boolean tryAdvance(Consumer<? super Object> action) {
					try {
						if (dslinput.isSpilled()) {
							if (isNull(dslinput.getTask().getHostport())
									|| dslinput.getTask().getHostport().split(DataSamudayaConstants.UNDERSCORE)[0]
									.equals(DataSamudayaProperties.get()
											.getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST))) {
								if (isNull(istream)) {
									istream = new FileInputStream(Utils.getLocalFilePathForTask(dslinput.getTask(),
											dslinput.getAppendwithpath(), dslinput.getAppendintermediate(),
											dslinput.getLeft(), dslinput.getRight()));
									sisinternal = new SnappyInputStream(istream);
									inputinternal = new Input(sisinternal);
								}
								if (isNull(intermdata) && inputinternal.available() > 0
										|| inputinternal.available() > 0 && !iterator.hasNext()) {
									intermdata = (Collection<?>) Utils.getKryoInstance().readClassAndObject(inputinternal);
									iterator = intermdata.iterator();
								}

							} else {
								if (isNull(client)) {
									client = new RemoteIteratorClient(dslinput.getTask()
									, dslinput.getAppendwithpath()
									, dslinput.getAppendintermediate()
									, dslinput.getLeft()
									, dslinput.getRight()
									, dslinput.getTask().getFcsc(),
											RequestType.LIST, IteratorType.DISKSPILLITERATOR, false, null);
								}
								if (isNull(iterator) && client.hasNext() || client.hasNext() && nonNull(iterator)
										&& !iterator.hasNext()) {
									List records = (List) Utils.convertBytesToObjectCompressed((byte[]) client.next(),
											null);
									iterator = records.iterator();
								}
							}
						} else if (isNull(iterator)) {
							if (nonNull(dslinput.getData())) {
								iterator = dslinput.getData().iterator();
							} else {
								iterator = new Vector<>().iterator();
							}
						}
						if (nonNull(iterator) && iterator.hasNext()) {
							action.accept(iterator.next());
						}
						if (nonNull(inputinternal) && inputinternal.available() > 0 || nonNull(iterator) && iterator.hasNext()) {
							return true;
						}
						log.debug("Stream Close {} {} {} {}", inputinternal, sisinternal, istream, client);
						if (nonNull(inputinternal)) {
							try {
								inputinternal.close();
							} catch (Exception e) {
								log.error(DataSamudayaConstants.EMPTY, e);
							}
						}
						if (nonNull(sisinternal)) {
							try {
								sisinternal.close();
							} catch (Exception e) {
								log.error(DataSamudayaConstants.EMPTY, e);
							}
						}
						if (nonNull(istream)) {
							try {
								istream.close();
							} catch (Exception e) {
								log.error(DataSamudayaConstants.EMPTY, e);
							}
						}
						if (nonNull(client)) {
							try {
								client.close();
							} catch (Exception e) {
								log.error(DataSamudayaConstants.EMPTY, e);
							}
						}
						log.debug("Stream Close {}", Utils.getLocalFilePathForTask(dslinput.getTask(),
								dslinput.getAppendwithpath(), dslinput.getAppendintermediate(),
								dslinput.getLeft(), dslinput.getRight()));
						return false;
					} catch (Throwable ex) {
						log.error(DataSamudayaConstants.EMPTY, ex);
						if (nonNull(inputinternal)) {
							try {
								inputinternal.close();
							} catch (Exception e) {
								log.error(DataSamudayaConstants.EMPTY, e);
							}
						}
						if (nonNull(sisinternal)) {
							try {
								sisinternal.close();
							} catch (Exception e) {
								log.error(DataSamudayaConstants.EMPTY, e);
							}
						}
						if (nonNull(istream)) {
							try {
								istream.close();
							} catch (Exception e) {
								log.error(DataSamudayaConstants.EMPTY, e);
							}
						}
						if (nonNull(client)) {
							try {
								client.close();
							} catch (Exception e) {
								log.error(DataSamudayaConstants.EMPTY, e);
							}
						}
					}
					return false;
				}
			}, false);
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
		return null;
	}

	/**
	 * The function returns stream of data of diskspilled whether it is local or remote server.
	 * @param dslinput
	 * @return get stream of diskspill for set
	 */
	public static Stream<?> getStreamData(DiskSpillingSet dslinput) {
		try {
			return StreamSupport.stream(new Spliterators.AbstractSpliterator<Object>(Long.MAX_VALUE,
					Spliterator.IMMUTABLE | Spliterator.ORDERED | Spliterator.NONNULL) {
				InputStream istream;
				SnappyInputStream sisinternal;
				Input inputinternal;
				Collection<?> intermdata = null;
				Iterator<?> iterator = null;
				RemoteIteratorClient client = null;

				public boolean tryAdvance(Consumer<? super Object> action) {
					try {
						if (dslinput.isSpilled()) {
							if (isNull(dslinput.getTask().getHostport())
									|| dslinput.getTask().getHostport().split(DataSamudayaConstants.UNDERSCORE)[0]
									.equals(DataSamudayaProperties.get()
											.getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST))) {
								if (isNull(istream)) {
									istream = new FileInputStream(Utils.getLocalFilePathForTask(dslinput.getTask(),
											dslinput.getAppendwithpath(), dslinput.getAppendintermediate(),
											dslinput.getLeft(), dslinput.getRight()));
									sisinternal = new SnappyInputStream(istream);
									inputinternal = new Input(sisinternal);
								}
								if (isNull(intermdata) && inputinternal.available() > 0
										|| inputinternal.available() > 0 && !iterator.hasNext()) {
									intermdata = (Collection<?>) Utils.getKryoInstance().readClassAndObject(inputinternal);
									iterator = intermdata.iterator();
								}

							} else {
								if (isNull(client)) {
									client = new RemoteIteratorClient(dslinput.getTask(), dslinput.getAppendwithpath(),
											dslinput.getAppendintermediate(),
											dslinput.getLeft(),
											dslinput.getRight(), dslinput.getTask().getFcsc(),
											RequestType.LIST, IteratorType.DISKSPILLITERATOR, false, null);
								}
								if (isNull(iterator) && client.hasNext() || client.hasNext() && nonNull(iterator)
										&& !iterator.hasNext()) {
									List records = (List) Utils.convertBytesToObjectCompressed((byte[]) client.next(),
											null);
									iterator = records.iterator();
								}
							}
						} else if (isNull(iterator)) {
							if (nonNull(dslinput.getData())) {
								iterator = dslinput.getData().iterator();
							} else {
								iterator = new LinkedHashSet<>().iterator();
							}
						}
						if (nonNull(iterator) && iterator.hasNext()) {
							action.accept(iterator.next());
						}
						if (nonNull(inputinternal) && inputinternal.available() > 0 || nonNull(iterator) && iterator.hasNext()) {
							return true;
						}
						log.debug("Stream Close {} {} {} {}", inputinternal, sisinternal, istream, client);
						if (nonNull(inputinternal)) {
							try {
								inputinternal.close();
							} catch (Exception e) {
								log.error(DataSamudayaConstants.EMPTY, e);
							}
						}
						if (nonNull(sisinternal)) {
							try {
								sisinternal.close();
							} catch (Exception e) {
								log.error(DataSamudayaConstants.EMPTY, e);
							}
						}
						if (nonNull(istream)) {
							try {
								istream.close();
							} catch (Exception e) {
								log.error(DataSamudayaConstants.EMPTY, e);
							}
						}
						if (nonNull(client)) {
							try {
								client.close();
							} catch (Exception e) {
								log.error(DataSamudayaConstants.EMPTY, e);
							}
						}
						log.debug("Stream Close {}", Utils.getLocalFilePathForTask(dslinput.getTask(),
								dslinput.getAppendwithpath(), dslinput.getAppendintermediate(),
								dslinput.getLeft(), dslinput.getRight()));
						return false;
					} catch (Throwable ex) {
						log.error(DataSamudayaConstants.EMPTY, ex);
						if (nonNull(inputinternal)) {
							try {
								inputinternal.close();
							} catch (Exception e) {
								log.error(DataSamudayaConstants.EMPTY, e);
							}
						}
						if (nonNull(sisinternal)) {
							try {
								sisinternal.close();
							} catch (Exception e) {
								log.error(DataSamudayaConstants.EMPTY, e);
							}
						}
						if (nonNull(istream)) {
							try {
								istream.close();
							} catch (Exception e) {
								log.error(DataSamudayaConstants.EMPTY, e);
							}
						}
						if (nonNull(client)) {
							try {
								client.close();
							} catch (Exception e) {
								log.error(DataSamudayaConstants.EMPTY, e);
							}
						}
					}
					return false;
				}
			}, false);
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
		return null;
	}

	/**
	 * The function converts ordered tree to lists inorder traversal
	 * @param nik
	 * @param orderedlist
	 */
	public static void traverseBinaryTree(NodeIndexKey nik, List<NodeIndexKey> orderedlist) {
		if (isNull(nik)) {
			return;
		}
		traverseBinaryTree(nik.getLeft(), orderedlist);
		orderedlist.add(nik);
		traverseBinaryTree(nik.getRight(), orderedlist);
	}

	/**
	 * The function converts the list of objects to stream fetching the data from iterator
	 * @param niks
	 * @param cache
	 * @return list of object to stream
	 */
	public static Stream<?> getStreamData(List<NodeIndexKey> niks, Cache<String, byte[]> cache) {
		boolean iscachenonempty = nonNull(cache);
		try {
			return StreamSupport.stream(new Spliterators.AbstractSpliterator<Object>(Long.MAX_VALUE,
					Spliterator.IMMUTABLE | Spliterator.ORDERED | Spliterator.NONNULL) {
				Kryo kryo = Utils.getKryoInstance();

				public boolean tryAdvance(Consumer<? super Object> action) {
					List<NodeIndexKey> orderednodesinternal = niks;
					Map<String, Input> keyinputmap = new ConcurrentHashMap<>();
					Map<String, SnappyInputStream> keysismap = new ConcurrentHashMap<>();
					Map<String, List> cachekeylistmap = new ConcurrentHashMap<>();
					Map<String, Integer> cachekeyindexmap = new ConcurrentHashMap<>();
					Map<Task, RemoteIteratorClient<NodeIndexKey>> taskrlistiterclientmap = new ConcurrentHashMap<>();
					try {
						for (var node :orderednodesinternal) {
							if (iscachenonempty) {
								if (isNull(keyinputmap.get(node.getCachekey()))) {
									ByteArrayInputStream bais = new ByteArrayInputStream(cache.get(node.getCachekey()));
									keysismap.put(node.getCachekey(), new SnappyInputStream(bais));
									keyinputmap.put(node.getCachekey(), new Input(keysismap.get(node.getCachekey())));
									cachekeylistmap.put(node.getCachekey(), (List) kryo.readClassAndObject(keyinputmap.get(node.getCachekey())));
									cachekeyindexmap.put(node.getCachekey(), 0);
								}
								action.accept(cachekeylistmap.get(node.getCachekey()).get(cachekeyindexmap.get(node.getCachekey())));
								cachekeyindexmap.put(node.getCachekey(), cachekeyindexmap.get(node.getCachekey()) + 1);
							} else {
								if (isNull(taskrlistiterclientmap.get(node.getTask()))) {
									taskrlistiterclientmap.put(node.getTask(), new RemoteIteratorClient<>(node.getTask(), null, false, false, false, null, RequestType.ELEMENT, IteratorType.SORTORUNIONORINTERSECTION, false, null));
								}
								action.accept(taskrlistiterclientmap.get(node.getTask()).next().getValue());
							}
						}
						keyinputmap.entrySet().stream().forEach(entry -> entry.getValue().close());
						keysismap.entrySet().stream().forEach(entry -> {
							try {
								entry.getValue().close();
							} catch (IOException e) {
								log.error(DataSamudayaConstants.EMPTY, e);
							}
						});
						taskrlistiterclientmap.entrySet().stream().forEach(entry -> {
							try {
								entry.getValue().close();
							} catch (IOException e) {
								log.error(DataSamudayaConstants.EMPTY, e);
							}
						});
						return false;
					} catch (Exception ex) {
						log.error(DataSamudayaConstants.EMPTY, ex);
					}
					return false;
				}
			}, false);
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
		return null;
	}

	/**
	 * The function returns config of akka system
	 * 
	 * @param hostname
	 * @param akkaport
	 * @return configuration of akka system
	 * @throws Exception
	 * @throws IOException
	 */
	public static Config getAkkaSystemConfig(String hostname, int akkaport, String configtype)
			throws IOException, Exception {
		String akkaconf;
		if (configtype.equals(DataSamudayaConstants.TEPROPLOADCLASSPATHCONFIG)) {
			akkaconf = IOUtils.toString(Utils.class.getResourceAsStream(DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.AKKACONF), Charset.defaultCharset());
		} else {
			akkaconf = Files.readString(java.nio.file.Path.of(DataSamudayaConstants.PREV_FOLDER
					+ DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.DIST_CONFIG_FOLDER
					+ DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.AKKACONF), Charset.defaultCharset());
		}
		return ConfigFactory.parseString(String.format(akkaconf, hostname, akkaport, hostname, akkaport));
	}

	/**
	 * The function converts the java object to bytes using kryo serializer
	 * 
	 * @param obj
	 * @return
	 */
	public static byte[] convertObjectToBytes(Object obj) {
		try (var ostream = new ByteArrayOutputStream(); var op = new Output(ostream);) {
			Kryo kryo = Utils.getKryoInstance();
			kryo.writeClassAndObject(op, obj);
			op.flush();
			return ostream.toByteArray();
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
		return null;
	}

	/**
	 * The function converts compressed bytes to Object using kryo
	 * 
	 * @param obj
	 * @return bytes to object
	 */
	public static Object convertBytesToObjectCompressed(byte[] obj, ClassLoader cl) {
		try (var istream = new ByteArrayInputStream(obj);
				var sis = new SnappyInputStream(istream);
				var ip = new Input(sis);) {
			Kryo kryo = Utils.getKryoInstance();
			if (nonNull(cl)) {
				kryo.setClassLoader(cl);
			}
			return kryo.readClassAndObject(ip);
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
		return null;
	}

	/**
	 * The function converts the java object to bytes using kryo serializer
	 * 
	 * @param obj
	 * @return
	 */
	public static byte[] convertObjectToBytesCompressed(Object obj, ClassLoader cl) {
		try (var ostream = new ByteArrayOutputStream();
				var sos = new SnappyOutputStream(ostream);
				var op = new Output(sos);) {
			Kryo kryo = Utils.getKryoInstance();
			if (nonNull(cl)) {
				kryo.setClassLoader(cl);
			}
			kryo.writeClassAndObject(op, obj);
			op.flush();
			return ostream.toByteArray();
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
		return null;
	}

	/**
	 * The function converts bytes to Object using kryo
	 * 
	 * @param obj
	 * @return bytes to object
	 */
	public static Object convertBytesToObject(byte[] obj) {
		try (var istream = new ByteArrayInputStream(obj); var ip = new Input(istream);) {
			Kryo kryo = Utils.getKryoInstance();
			return kryo.readClassAndObject(ip);
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
		return null;
	}

	/**
	 * Start Shuffle Records Server Iterator
	 * 
	 * @param port
	 * @throws Exception
	 */
	public static Tuple2<ServerSocket, ExecutorService> startShuffleRecordsServer() throws Exception {
		final ExecutorService executors;
		final ServerSocket serverSocket;
		try {
			executors = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("ShuffleRecordsServer-", 0).factory());
			serverSocket = new ServerSocket(0);
			log.debug("Shuffle Server started at port. {}", serverSocket.getLocalPort());
			executors.execute(() -> {
				while (true) {
					if (nonNull(serverSocket) && serverSocket.isClosed()) {
						break;
					}
					final Socket sock;
					try {
						sock = serverSocket.accept();
						log.error("Accepting Connections from client {}", sock.getPort());
					} catch (Exception ex) {
						log.error(DataSamudayaConstants.EMPTY, ex);
						continue;
					}
					executors.execute(() -> {
						try (Socket socket = sock;) {
							Kryo readkryo = Utils.getKryoInstance();
							Kryo writekryo = Utils.getKryoInstance();
							try (InputStream istream = socket.getInputStream();
									Input input = new Input(istream);
									OutputStream soutput = socket.getOutputStream();
									LZ4BlockOutputStream socketsos = new LZ4BlockOutputStream(soutput);
									Output output = new Output(socketsos);) {
								log.debug("File Started To be Processed for remote shuffle from path {}", System.getProperty(DataSamudayaConstants.TMPDIR));
								RemoteDataFetch rdf = (RemoteDataFetch) readkryo.readClassAndObject(input);
								log.debug("File To be Processed for remote shuffle with path {} and subpath rdf {}",
										System.getProperty(DataSamudayaConstants.TMPDIR), rdf);
								try (FileInputStream fstream = new FileInputStream(
										System.getProperty(DataSamudayaConstants.TMPDIR) + rdf.getShufflefilepath());
										InputStream sis = new SnappyInputStream(fstream);
										Input fsinput = new Input(sis)) {
									// Provide iterator functionality
									while (fsinput.available() > 0) {
										writekryo.writeClassAndObject(output, readkryo.readClassAndObject(fsinput));
										output.flush();
									}
									writekryo.writeClassAndObject(output, new Dummy());
									output.flush();
								}
							} catch (Exception ex) {
								log.error(DataSamudayaConstants.EMPTY, ex);
							}
						} catch (Exception ex) {
							log.error(DataSamudayaConstants.EMPTY, ex);
						}
					});
				}
			});
			return new Tuple2<>(serverSocket, executors);
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
		return null;
	}

	/**
	 * The method get formats the File path for the Shuffled result blocks
	 * 
	 * @param task
	 * @param appendwithpath
	 * @param appendintermediate
	 * @param left
	 * @param right
	 * @return File path for the Shuffled result blocks
	 */
	public static String getFilePathRemoteDataFetch(Task task, String appendwithpath, boolean appendintermediate,
			boolean left, boolean right) {
		return DataSamudayaConstants.FORWARD_SLASH + task.getJobid() + DataSamudayaConstants.FORWARD_SLASH
				+ task.getJobid() + DataSamudayaConstants.HYPHEN + task.getStageid() + DataSamudayaConstants.HYPHEN
				+ task.getTaskid()
				+ (StringUtils.isNotEmpty(appendwithpath) ? DataSamudayaConstants.HYPHEN + appendwithpath
				: DataSamudayaConstants.EMPTY)
				+ (appendintermediate ? DataSamudayaConstants.HYPHEN + DataSamudayaConstants.INTERMEDIATE
				: DataSamudayaConstants.EMPTY)
				+ (left || right
				? left ? DataSamudayaConstants.HYPHEN + DataSamudayaConstants.INTERMEDIATEJOINLEFT
				: DataSamudayaConstants.HYPHEN + DataSamudayaConstants.INTERMEDIATEJOINRIGHT
				: DataSamudayaConstants.EMPTY)
				+ DataSamudayaConstants.DOT + DataSamudayaConstants.DATA;
	}

	static final Random rand = new Random(System.currentTimeMillis());

	/**
	 * The function forms binary tree for sorting
	 * @param root
	 * @param child
	 * @param rfcs
	 * @return rootnode
	 */
	public static void formSortedBinaryTree(NodeIndexKey root, NodeIndexKey child, List<FieldCollationDirection> rfcs) {
		try {
			if (root == null || child == null) {
				return;
			} else {
				if (compare(root.getKey(), child.getKey(), rfcs) > 0) {
					if (isNull(root.getLeft())) {
						root.setLeft(child);
					} else {
						formSortedBinaryTree(root.getLeft(), child, rfcs);
					}
				}
				if (compare(root.getKey(), child.getKey(), rfcs) < 0) {
					if (isNull(root.getRight())) {
						root.setRight(child);
					} else {
						formSortedBinaryTree(root.getRight(), child, rfcs);
					}
				}
				if (compare(root.getKey(), child.getKey(), rfcs) == 0) {
					if (rand.nextBoolean()) {
						if (isNull(root.getLeft())) {
							root.setLeft(child);
						} else {
							formSortedBinaryTree(root.getLeft(), child, rfcs);
						}
					} else {
						if (isNull(root.getRight())) {
							root.setRight(child);
						} else {
							formSortedBinaryTree(root.getRight(), child, rfcs);
						}
					}
				}
			}
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
	}

	/**
	 * Compare two object array
	 * @param obj1
	 * @param obj2
	 * @param rfcs
	 * @return 0 if equals , 1 if object1 is greater than object2 or -1 if object1 is less than object2
	 */
	public static int compare(Object[] obj1, Object[] obj2, List<FieldCollationDirection> rfcs) {
		int numofelem = rfcs.size();
		for (int index = 0;index < numofelem;index++) {
			FieldCollationDirection fc = rfcs.get(index);
			String sortOrder = fc.getDirection().name();
			Object value1 = obj1[0].getClass() == Object[].class ? ((Object[]) obj1[0])[index]
					: obj1[index];
			Object value2 = obj2[0].getClass() == Object[].class ? ((Object[]) obj2[0])[index]
					: obj2[index];
			int result = compareTo(value1, value2);
			if ("DESCENDING".equals(sortOrder)) {
				result = -result;
			}
			if (result != 0) {
				return result;
			}
		}
		return 0;
	}

	/**
	 * Compare two objects and returns comparison value
	 * @param obj1
	 * @param obj2
	 * @return comparison value of Wrapper class
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
	 * The function returns key from object for forming binary tree sort
	 * @param rfcs
	 * @param obj
	 * @return Key object from obj
	 */
	public static Object[] getKeyFromNodeIndexKey(List<FieldCollationDirection> rfcs, Object[] obj) {
		List<Object> keys = new ArrayList<>();
		for (int i = 0;i < rfcs.size();i++) {
			FieldCollationDirection fc = rfcs.get(i);
			Object value = obj[0].getClass() == Object[].class ? ((Object[]) obj[0])[fc.getFieldindex()]
					: obj[fc.getFieldindex()];
			keys.add(value);
		}
		return keys.toArray();
	}

	/**
	 * Cleanup function to deregister actors from actors system
	 * @param actorsystem
	 * @param actors
	 * @param jobid
	 * @return successful cleanup of tmp folder for job
	 * @throws Exception 
	 */
	public static boolean cleanupTaskActorFromSystem(ActorSystem actorsystem, Map<String, EntityTypeKey> actors, String jobid) throws Exception {
		for (Entry<String, EntityTypeKey> entity :actors.entrySet()) {
			String[] hpshardentitykey = extractAkkaActorHostPortShardIdEntityTypeKey(entity.getKey());
			final EntityRef entityRef = ClusterSharding.get(actorsystem)
					.entityRefFor(entity.getValue(), hpshardentitykey[2]);
			entityRef.tell(EntityRefStop.STOP);
		}
		deleteJobDir(jobid);
		actors.clear();
		return true;
	}
	
	/**
	 * The function returns host port of actor system shardid and entityid
	 * @param entityurl
	 * @return entity url splitted in array
	 */
	public static String[] extractAkkaActorHostPortShardIdEntityTypeKey(String entityurl) {
		return entityurl.split(DataSamudayaConstants.FORWARD_SLASH);
	}

	/**
	 * The method deletes temporary job folder
	 * @param jobid
	 * @throws Exception
	 */
	public static void deleteJobDir(String jobid) throws Exception {
		File deletefolder = new File(System.getProperty(DataSamudayaConstants.TMPDIR)
				+ File.separatorChar + jobid);
		if (deletefolder.exists()) {
			FileUtils.deleteDirectory(deletefolder);
		}
	}

	/**
	 * The function nullify the left and right nodes to serialize object without stackoverflow error.
	 * @param niks
	 */
	public static void NullifyLeftAndRightNikTree(DiskSpillingList<NodeIndexKey> niks) {
		niks.getData().stream().forEach(nik -> {
			nik.setLeft(null);
			nik.setRight(null);
		});
	}

	/**
	 * The Copy of the spilled data from one file to another file
	 * 
	 * @param dslinput
	 * @param kryo
	 * @param dslout
	 */
	public static void copySpilledDataSourceToDestination(DiskSpillingSet dslinput, DiskSpillingList dslout) {
		if (isNull(dslinput.getTask().getHostport()) || dslinput.getTask().getHostport().split(DataSamudayaConstants.UNDERSCORE)[0]
				.equals(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST))) {
			Kryo kryo = Utils.getKryoInstance();
			try (FileInputStream istream = new FileInputStream(
					Utils.getLocalFilePathForTask(dslinput.getTask(), dslinput.getAppendwithpath(),
							dslinput.getAppendintermediate(), dslinput.getLeft(), dslinput.getRight()));
					var sis = new SnappyInputStream(istream);
					Input input = new Input(sis);) {
				while (input.available() > 0) {
					Set records = (Set) kryo.readClassAndObject(input);
					if (CollectionUtils.isNotEmpty(records)) {
						dslout.addAll(records);
					}
				}
			} catch (Exception ex) {
				log.error(DataSamudayaConstants.EMPTY, ex);
			}
		} else {
			try (RemoteIteratorClient client = new RemoteIteratorClient(dslinput.getTask()
			, dslinput.getAppendwithpath()
			, dslinput.getAppendintermediate()
			, dslinput.getLeft()
			, dslinput.getRight()
			, dslinput.getTask().getFcsc(),
					RequestType.LIST, IteratorType.DISKSPILLITERATOR, false, null)) {
				while (client.hasNext()) {

					List records = (List) Utils
							.convertBytesToObjectCompressed((byte[]) client.next(), null);

					if (CollectionUtils.isNotEmpty(records)) {
						dslout.addAll(records);
					}

				}
			} catch (Exception e) {
				log.error(DataSamudayaConstants.EMPTY, e);
			}
		}
	}

	/**
	 * The Copy of the spilled data from list to set
	 * @param dslinput
	 * @param dslout
	 */
	public static void copySpilledDataSourceToDestination(DiskSpillingList dslinput, DiskSpillingSet dslout) {
		if (isNull(dslinput.getTask().getHostport()) || dslinput.getTask().getHostport().split(DataSamudayaConstants.UNDERSCORE)[0]
				.equals(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST))) {
			Kryo kryo = Utils.getKryoInstance();
			try (FileInputStream istream = new FileInputStream(
					Utils.getLocalFilePathForTask(dslinput.getTask(), dslinput.getAppendwithpath(),
							dslinput.getAppendintermediate(), dslinput.getLeft(), dslinput.getRight()));
					var sis = new SnappyInputStream(istream);
					Input input = new Input(sis);) {
				while (input.available() > 0) {
					List records = (List) kryo.readClassAndObject(input);
					if (CollectionUtils.isNotEmpty(records)) {
						dslout.addAll(records);
					}
				}
			} catch (Exception ex) {
				log.error(DataSamudayaConstants.EMPTY, ex);
			}
		} else {
			try (RemoteIteratorClient client = new RemoteIteratorClient(dslinput.getTask()
			, dslinput.getAppendwithpath()
			, dslinput.getAppendintermediate()
			, dslinput.getLeft()
			, dslinput.getRight()
			, dslinput.getTask().getFcsc(),
					RequestType.LIST, IteratorType.DISKSPILLITERATOR, false, null)) {
				while (client.hasNext()) {

					List records = (List) Utils
							.convertBytesToObjectCompressed((byte[]) client.next(), null);

					if (CollectionUtils.isNotEmpty(records)) {
						dslout.addAll(records);
					}

				}
			} catch (Exception e) {
				log.error(DataSamudayaConstants.EMPTY, e);
			}
		}
	}

	/**
	 * The Copy of the spilled data from set to set
	 * @param dslinput
	 * @param dslout
	 */
	public static void copySpilledDataSourceToDestination(DiskSpillingSet dslinput, DiskSpillingSet dslout) {
		if (isNull(dslinput.getTask().getHostport()) || dslinput.getTask().getHostport().split(DataSamudayaConstants.UNDERSCORE)[0]
				.equals(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST))) {
			Kryo kryo = Utils.getKryoInstance();
			try (FileInputStream istream = new FileInputStream(
					Utils.getLocalFilePathForTask(dslinput.getTask(), dslinput.getAppendwithpath(),
							dslinput.getAppendintermediate(), dslinput.getLeft(), dslinput.getRight()));
					var sis = new SnappyInputStream(istream);
					Input input = new Input(sis);) {
				while (input.available() > 0) {
					Set records = (Set) kryo.readClassAndObject(input);
					if (CollectionUtils.isNotEmpty(records)) {
						dslout.addAll(records);
					}
				}
			} catch (Exception ex) {
				log.error(DataSamudayaConstants.EMPTY, ex);
			}
		} else {
			try (RemoteIteratorClient client = new RemoteIteratorClient(dslinput.getTask()
			, dslinput.getAppendwithpath()
			, dslinput.getAppendintermediate()
			, dslinput.getLeft()
			, dslinput.getRight()
			, dslinput.getTask().getFcsc(),
					RequestType.LIST, IteratorType.DISKSPILLITERATOR, false, null)) {
				while (client.hasNext()) {

					List records = (List) Utils
							.convertBytesToObjectCompressed((byte[]) client.next(), null);

					if (CollectionUtils.isNotEmpty(records)) {
						dslout.addAll(records);
					}

				}
			} catch (Exception e) {
				log.error(DataSamudayaConstants.EMPTY, e);
			}
		}
	}

	/**
	 * Copy Spilled Context from source to destination.
	 * @param dslinput
	 * @param dslout
	 */
	public static void copySpilledContextToDestination(DiskSpillingContext dscinput, List<Context> dscouts, Object key, Task remotetask, boolean isremote) {
		if (!isremote && ((nonNull(dscinput)
				&& dscinput.getTask().getHostport().split(DataSamudayaConstants.UNDERSCORE)[0]
				.equals(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST)))
				|| remotetask.getHostport().split(DataSamudayaConstants.UNDERSCORE)[0]
				.equals(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST)))) {
			Kryo kryo = Utils.getKryoInstance();
			String filepath = nonNull(dscinput) ? Utils.getLocalFilePathForMRTask(dscinput.getTask(), dscinput.getAppendwithpath())
					: Utils.getLocalFilePathForMRTask(remotetask, null);
			if (new File(filepath).exists()) {
				log.debug("In Fetching Local DiskSpilled with file path {}", filepath);
				try (FileInputStream istream = new FileInputStream(filepath);
						var sis = new SnappyInputStream(istream);
						Input input = new Input(sis);) {
					while (input.available() > 0) {
						DataCruncherContext ctx = (DataCruncherContext) kryo.readClassAndObject(input);
						if (nonNull(ctx)) {
							for (Context dscout :dscouts) {
								dscout.addAll(key, ctx.get(key));
							}
						}
					}
				} catch (Exception ex) {
					log.error(DataSamudayaConstants.EMPTY, ex);
				}
				return;
			}
		}
		log.debug("In Fetching Remote DiskSpilled");
		try (RemoteIteratorClient client = new RemoteIteratorClient<byte[]>(remotetask, null, false, false, false, null,
				RequestType.CONTEXT, IteratorType.DISKSPILLITERATOR, true, key)) {
			while (client.hasNext()) {

				Collection values = (Collection) Utils.convertBytesToObjectCompressed((byte[]) client.next(), null);

				for (Context dscout : dscouts) {
					dscout.addAll(key, values);
				}
			}
		} catch (Exception e) {
			log.error(DataSamudayaConstants.EMPTY, e);
		}

	}

	/**
	 * Copy Disk Spilling Set To Disk For Union
	 * @param dslinput
	 */
	public static void copyDiskSpillingSetToDisk(DiskSpillingSet dslinput) {
		Kryo kryo = Utils.getKryoInstance();
		try (FileOutputStream ostream = new FileOutputStream(
				Utils.getLocalFilePathForTask(dslinput.getTask(), dslinput.getAppendwithpath(),
						dslinput.getAppendintermediate(), dslinput.getLeft(), dslinput.getRight()));
				var sos = new SnappyOutputStream(ostream);
				Output output = new Output(sos);) {
			kryo.writeClassAndObject(output, dslinput.getData());
			output.flush();
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
	}

	/**
	 * The function sends push notification to the system tray
	 * @param title
	 * @param message
	 * @param messageType
	 * @throws AWTException
	 */
	public static void showNotification(String title, String message, MessageType messageType) throws AWTException {
		// Check if SystemTray is supported
		if (!SystemTray.isSupported()) {
			log.error("System tray is not supported!");
			return;
		}

		// Get the SystemTray instance
		SystemTray tray = SystemTray.getSystemTray();

		// Load an image
		Image image = Toolkit.getDefaultToolkit().createImage("Notification Image");

		// Create a popup menu (optional)
		PopupMenu popup = new PopupMenu();
		MenuItem defaultItem = new MenuItem("Open");
		defaultItem.addActionListener(e -> log.debug("Opening..."));
		popup.add(defaultItem);

		// Create a TrayIcon
		TrayIcon trayIcon = new TrayIcon(image, "Push Notification", popup);
		// Auto-size
		trayIcon.setImageAutoSize(true);

		// Add the TrayIcon to the SystemTray
		tray.add(trayIcon);

		// Display a notification
		trayIcon.displayMessage(title, message, messageType);
	}

	/**
	 * The function forms details on job for push notification
	 * @param job
	 * @return job information
	 */
	public static String getJobInfo(Job job) {
		StringBuilder builder = new StringBuilder();
		builder.append("Job Metrics");
		builder.append(DataSamudayaConstants.NEWLINE);
		builder.append("JobId: ");
		builder.append(job.getId());
		builder.append(DataSamudayaConstants.NEWLINE);
		builder.append("Job Mode: ");
		builder.append(job.getPipelineconfig().getMode());
		builder.append(DataSamudayaConstants.NEWLINE);
		builder.append("Job Type: ");
		builder.append(job.getJobtype());
		builder.append(DataSamudayaConstants.NEWLINE);
		if (nonNull(job.getJm())) {
			builder.append("Total Time Taken: ");
			builder.append(job.getJm().getTotaltimetaken());
			builder.append(" Seconds");
			builder.append(DataSamudayaConstants.NEWLINE);
		}
		return builder.toString();
	}

	/**
	 * The function returns node ip for a given pod ip
	 * @param podIP
	 * @return NodeIp
	 */
	public static Optional<String> getNodeIPByPodIP(String podIP) {
		try {
			String nodeip = null;
			KubernetesClient kubeclient = new DefaultKubernetesClient();
			List<Pod> podsWithIPAddress = kubeclient.pods().inAnyNamespace().withField(DataSamudayaConstants.PODIP_STATUS, podIP)
					.list().getItems();
			if (podsWithIPAddress.size() > 0) {
				String nodename = podsWithIPAddress.get(0).getSpec().getNodeName();
				log.debug("Using Pod Ip {} To Node Name: {}", podIP, nodename);
				nodeip = kubeclient.nodes().withName(nodename).get().getStatus().getAddresses().get(0).getAddress().split(":")[0];
				log.debug("Using Pod Ip {} To Node Address: {}", podIP, nodeip);
			}

			return Optional.ofNullable(nodeip);
		} catch (KubernetesClientException e) {
			log.warn("Couldn't resolve Node by Pod IP " + podIP, e);
			return Optional.empty();
		}
	}

	/**
	 * The function returns blocks location string to store as key in ignite
	 * @param bl blocks location
	 * @return blocks location as key
	 */
	public static String getBlocksLocation(BlocksLocation bl) {
		Block[] blocks = bl.getBlock();
		return blocks[0].getFilename()
				+ DataSamudayaConstants.HYPHEN
				+ blocks[0].getBlockOffset()
				+ DataSamudayaConstants.ROUNDED_BRACKET_OPEN
				+ blocks[0].getBlockstart() + DataSamudayaConstants.HYPHEN + blocks[0].getBlockend() + DataSamudayaConstants.ROUNDED_BRACKET_CLOSE;
	}

	/**
	 * The function returns list of launched containers for given jobid and executor host port map
	 * @param exechostportmap
	 * @param jobid
	 * @return list of launched containers
	 */
	public static List<LaunchContainers> getLcs(Map<String, Set<String>> exechostportmap, String jobid, int cpupercontainer) {
		var lcs = new ArrayList<LaunchContainers>();
		for (Entry<String, Set<String>> hosthps : exechostportmap.entrySet()) {
			var crl = new ArrayList<ContainerResources>();
			for (String te :hosthps.getValue()) {
				var crs = new ContainerResources();
				crs.setCpu(cpupercontainer);
				crs.setPort(Integer.parseInt(te.split(DataSamudayaConstants.UNDERSCORE)[1]));
				crs.setExecutortype(EXECUTORTYPE.EXECUTOR);
				crl.add(crs);
			}
			var cla = new ContainerLaunchAttributes();
			cla.setCr(crl);
			cla.setNumberofcontainers(hosthps.getValue().size());
			LaunchContainers lc = new LaunchContainers();
			lc.setCla(cla);
			lc.setNodehostport(hosthps.getKey() + DataSamudayaConstants.UNDERSCORE + 65534);
			lc.setJobid(jobid);
			lcs.add(lc);
		}
		return lcs;
	}

	/**
	 * The function returns ollama chat client
	 * @return chat client
	 */
	public static OllamaChatClient getOllamaChatClient() {
		var chatclient = new OllamaChatClient(new OllamaApi(DataSamudayaProperties.get()
				.getProperty(DataSamudayaConstants.OLLAMA_BASE_URL,
						DataSamudayaConstants.OLLAMA_BASE_URL_DEFAULT)));
		return chatclient;
	}

	/**
	 * Initializes Ollama Chat Client and returns it
	 * @return
	 */
	public static void initializeOllamaChatClient() {
		ollamaChatClient = new OllamaChatClient(new OllamaApi(DataSamudayaProperties.get()
				.getProperty(DataSamudayaConstants.OLLAMA_BASE_URL,
						DataSamudayaConstants.OLLAMA_BASE_URL_DEFAULT)));
	}

	/**
	 * This method returns line reader object
	 * @return Line Reader object
	 * @throws Exception
	 */
	public static LineReader getLineReaderTerminal(String path) throws Exception {
		Terminal terminal = TerminalBuilder.builder()
				.nativeSignals(true)
				.signalHandler(Terminal.SignalHandler.SIG_IGN)
				.build();
		return LineReaderBuilder.builder().variable(
				LineReader.HISTORY_FILE,
				path
		).terminal(terminal).variable(
				LineReader.HISTORY_FILE_SIZE,
				1_000 // history entries
		).build();
	}

	/**
	 * The function configures the Metastore for hive to store table information
	 * @return HiveConf 
	 * @throws Exception
	 */
	public static HiveConf getHiveConf(String user) throws Exception {
		File scratchDirFile = new File(DataSamudayaProperties.get()
				.getProperty(DataSamudayaConstants.HIVE_SCRATCH_DIR,
						DataSamudayaConstants.HIVE_SCRATCH_DIR_DEFAULT));
		String scratchDir = scratchDirFile.getAbsolutePath();
		Configuration cfg = new Configuration();
		HiveConf conf = new HiveConf(cfg, HiveConf.class);
		conf.setBoolean("datanucleus.schema.autoCreateAll", true);
		conf.addToRestrictList("columns.comments");
		conf.set("hive.scratch.dir.permission", "777");
		conf.setVar(ConfVars.SCRATCH_DIR_PERMISSION, "777");
		scratchDirFile.mkdirs();
		// also set the permissions manually since Hive doesn't do it...
		scratchDirFile.setWritable(true, false);
		String storeid = "1234";
		conf.set("hive.metastore.warehouse.dir", scratchDir + File.separatorChar + user
				+ DataSamudayaProperties.get()
				.getProperty(DataSamudayaConstants.WAREHOUSE_DIR_PATH,
						DataSamudayaConstants.WAREHOUSE_DIR_PATH_DEFAULT) + storeid);
		conf.set("hive.metastore.metadb.dir",
				scratchDir + File.separatorChar + user
						+ DataSamudayaProperties.get()
						.getProperty(DataSamudayaConstants.METASTORE_DIR_PATH,
								DataSamudayaConstants.METASTORE_DIR_PATH_DEFAULT) + storeid);
		conf.set("hive.exec.scratchdir", scratchDir + File.separatorChar + user);
		conf.set("fs.permissions.umask-mode", "022");
		conf.set("hive.metastore.dbtype", "derby");
		conf.set("hive.metastore.local", "true");
		conf.set("hive.metastore.schema.verification", "false");
		conf.set("hive.metastore.try.direct.sql", "false");
		conf.set("hive.metastore.fastpath", "true");
		conf.set("javax.jdo.option.ConnectionURL",
				"jdbc:derby:;databaseName=" + scratchDir
						+ File.separatorChar + user
						+ DataSamudayaProperties.get()
						.getProperty(DataSamudayaConstants.METASTORE_DIR_PATH,
								DataSamudayaConstants.METASTORE_DIR_PATH_DEFAULT) + storeid + ";create=true");
		conf.set("hive.metastore.local", "true");
		conf.set("hive.aux.jars.path", "");
		conf.set("hive.added.jars.path", "");
		conf.set("hive.added.files.path", "");
		conf.set("hive.added.archives.path", "");
		conf.set("fs.default.name", "file:///");
		conf.set("mapred.job.tracker", "local");
		conf.set("hive.server2.thrift.bind.host", "localhost");
		conf.set("hive.server2.thrift.port", DataSamudayaProperties.get()
				.getProperty(DataSamudayaConstants.THRIFT_PORT,
						DataSamudayaConstants.THRIFT_PORT_DEFAULT));
		conf.set("hive.server2.transport.mode", "binary");
		conf.set("hive.server2.thrift.client.user", "admin");
		conf.set("hive.server2.thrift.client.password", "admin");
		conf.set("hive.server2.authentication", "LDAP");
		conf.setBoolean("hive.server2.use.SSL", false);
		// clear mapred.job.tracker - Hadoop defaults to 'local' if not defined. Hive
		// however expects this to be set to 'local' - if it's not, it does a remote
		// execution (i.e. no child JVM)
		Field field = Configuration.class.getDeclaredField("properties");
		field.setAccessible(true);
		Properties props = (Properties) field.get(conf);
		props.remove("mapreduce.framework.name");
		props.setProperty("fs.default.name", "file:///");
		// intercept SessionState to clean the threadlocal
		Field tss = SessionState.class.getDeclaredField("tss");
		tss.setAccessible(true);
		return new HiveConf(conf);
	}

	/**
	 * The function starts hive session and returns session state
	 * @return session start
	 * @throws Exception
	 */
	public static SessionState startHiveSession(String user) throws Exception {
		SessionState sessionState = new SessionState(getHiveConf(user));
		return SessionState.start(sessionState);
	}

	/**
	 * The current session ends
	 * @param sessionState
	 * @throws Exception
	 */
	public static void endStartHiveSession(SessionState sessionState) throws Exception {
		SessionState.endStart(sessionState);
	}
	
	/**
	 * The function writes the classpath to a file
	 */
	public static void gatherClasspathDependenciesToFile(String folderPath, String fileName) {
		  // Get the classpath from the system properties
        String classpath = System.getProperty("java.class.path");
        
        try (BufferedWriter writer = Files.newBufferedWriter(Paths.get(folderPath, fileName))) {
           writer.append(classpath);
           log.info("Classpath written to {}", fileName);
        } catch (IOException e) {
        	log.error("Error writing to classpath.txt: ", e);
        }
	}
	/**
	 * Uploads a file to HDFS
	 * @param hdfs
	 * @param dir
	 * @param filename
	 * @throws Throwable
	 */
	public static void uploadfile(FileSystem hdfs, String dir, String filename) throws Throwable {
		InputStream is = Utils.class.getResourceAsStream(filename);
		String jobpath = dir;
		String filepath = jobpath + filename;
		Path jobpathurl = new Path(jobpath);
		if (!hdfs.exists(jobpathurl)) {
			hdfs.mkdirs(jobpathurl);
		}
		Path filepathurl = new Path(filepath);
		FSDataOutputStream fsdos = hdfs.create(filepathurl);
		IOUtils.copy(is, fsdos);
		fsdos.hflush();
		is.close();
		fsdos.close();
	}
	
	/**
	 * The function updates zookeeper tasks from akka actors
	 * @param task
	 * @param isresultavailable
	 */
	public static void updateZookeeperTasksData(Task task, boolean isresultavailable) {
		try (ZookeeperOperations zo = new ZookeeperOperations()){
			zo.connect();
			zo.updateTasksForJobNode(task, isresultavailable);
		} catch (ZookeeperException e) {
			log.error(DataSamudayaConstants.EMPTY, e);
		} catch (Exception e) {
			log.error(DataSamudayaConstants.EMPTY, e);
		}
	}
	
}
