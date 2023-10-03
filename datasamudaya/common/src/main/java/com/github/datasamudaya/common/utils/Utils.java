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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.security.KeyStore;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLServerSocket;
import javax.net.ssl.SSLServerSocketFactory;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.apache.commons.lang3.SerializationUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ZooKeeperServer;
import org.burningwave.core.assembler.StaticComponentContainer;
import org.jgrapht.Graph;
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
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.objenesis.strategy.StdInstantiatorStrategy;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.ClosureSerializer.Closure;
import com.esotericsoftware.kryo.serializers.DefaultArraySerializers.StringArraySerializer;
import com.esotericsoftware.kryo.serializers.CompatibleFieldSerializer;
import com.esotericsoftware.kryo.serializers.DefaultSerializers.EnumSerializer;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import com.github.datasamudaya.common.AllocateContainers;
import com.github.datasamudaya.common.ContainerException;
import com.github.datasamudaya.common.ContainerLaunchAttributes;
import com.github.datasamudaya.common.ContainerResources;
import com.github.datasamudaya.common.DAGEdge;
import com.github.datasamudaya.common.DataCruncherContext;
import com.github.datasamudaya.common.DestroyContainer;
import com.github.datasamudaya.common.DestroyContainers;
import com.github.datasamudaya.common.GlobalContainerAllocDealloc;
import com.github.datasamudaya.common.GlobalContainerLaunchers;
import com.github.datasamudaya.common.GlobalJobFolderBlockLocations;
import com.github.datasamudaya.common.Job;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.LaunchContainers;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaNodesResources;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.DataSamudayaUsers;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.PipelineConstants;
import com.github.datasamudaya.common.RemoteDataFetch;
import com.github.datasamudaya.common.Resources;
import com.github.datasamudaya.common.Stage;
import com.github.datasamudaya.common.StreamDataCruncher;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.TaskStatus;
import com.github.datasamudaya.common.Tuple2Serializable;
import com.github.datasamudaya.common.User;
import com.github.datasamudaya.common.WhoAreRequest;
import com.github.datasamudaya.common.WhoAreResponse;
import com.github.datasamudaya.common.WhoIsRequest;
import com.github.datasamudaya.common.WhoIsResponse;
import com.github.datasamudaya.common.Job.JOBTYPE;
import com.github.datasamudaya.common.functions.Coalesce;

import jdk.jshell.JShell;
import net.sf.jsqlparser.parser.SimpleNode;
import net.sf.jsqlparser.schema.Table;

/**
 * 
 * @author arun Utils for adding the shutdown hook and obtaining the shuffled
 *         task executors and utilities and send and receive the objects via
 *         socket.
 */
public class Utils {
	private static org.slf4j.Logger log = LoggerFactory.getLogger(Utils.class);

	private Utils() {
	}

	static MemoryPoolMXBean mpBeanLocalToJVM;

	static {
		for (MemoryPoolMXBean mpBean : ManagementFactory.getMemoryPoolMXBeans()) {
			if (mpBean.getType() == MemoryType.HEAP) {
				mpBeanLocalToJVM = mpBean;
				break;
			}
		}
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
	static ThreadLocal<Kryo> conf = new ThreadLocal<>() {
		public Kryo initialValue() {
			return getKryoInstance();
		}
	};

	/**
	 * Writes the text message to outputstream.
	 * 
	 * @param os
	 * @param message
	 * @throws Exception
	 */
	public static void writeToOstream(OutputStream os, String message) throws Exception {
		if (nonNull(os)) {
			os.write(message.getBytes());
			os.write('\n');
			os.flush();
		}
	}

	/**
	 * This method configures the log4j properties and obtains the properties from
	 * the config folder in the binary distribution.
	 * 
	 * @param propertyfile
	 * @throws Exception
	 */
	public static void initializeProperties(String propertiesfilepath, String propertyfile) throws Exception {
		log.debug("Entered Utils.initializeProperties");
		if (Objects.isNull(propertyfile)) {
			throw new Exception("Property File Name cannot be null");
		}
		if (Objects.isNull(propertiesfilepath)) {
			throw new Exception("Properties File Path cannot be null");
		}
		try (var fis = new FileInputStream(propertiesfilepath + propertyfile);) {
			var prop = new Properties();
			prop.load(fis);
			prop.putAll(System.getProperties());
			log.debug("Properties: " + prop.entrySet());
			DataSamudayaProperties.put(prop);
			String containerusersshare = prop.getProperty(DataSamudayaConstants.CONTAINER_ALLOC_USERS_PLUS_SHARE);
			if (nonNull(containerusersshare)) {
				String[] cus = containerusersshare.split(",");
				int userswithshare = cus.length;
				if (userswithshare > 0 && userswithshare % 2 == 0) {
					int noofusers = userswithshare / 2;
					var userssharepercentage = new ConcurrentHashMap<String, User>();
					double sharepercentagetotal = 0;
					for (int usercount = 0; usercount < noofusers; usercount++) {
						int sharepercentage = Integer.valueOf(cus[usercount * 2 + 1]);
						String username = cus[usercount * 2];
						User user = new User(username, sharepercentage, new ConcurrentHashMap<>(),
								new ConcurrentHashMap<>());
						userssharepercentage.put(username, user);
						sharepercentagetotal += sharepercentage;
					}
					if (sharepercentagetotal > 100.0) {
						throw new Exception("Users share total not tally and it should be less that or equal to 100.0");
					}
					DataSamudayaUsers.put(userssharepercentage);
				} else {
					throw new Exception(
							"Container users share property [container.alloc.users.share] not properly formatted");
				}
			}
			try {
				StaticComponentContainer.Configuration.Default.setFileName(DataSamudayaProperties.get()
						.getProperty(DataSamudayaConstants.BURNINGWAVE_PROPERTIES, DataSamudayaConstants.BURNINGWAVE_PROPERTIES_DEFAULT));
			} catch (UnsupportedOperationException uoe) {
				log.error("Problem in loading burningwave properties, See the cause below", uoe);
			}
		} catch (Exception ex) {
			log.error("Problem in loading properties, See the cause below", ex);
			throw new Exception("Unable To Load Properties", ex);
		}
		log.debug("Exiting Utils.initializeProperties");
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
	 * Gets the kryo instance by registering the required objects for serialization.
	 * 
	 * @return kryo instance
	 */
	public static Kryo getKryoInstance() {
		Kryo kryo = new Kryo();
		kryo.setReferences(true);
		kryo.setRegistrationRequired(false);
		kryo.setDefaultSerializer(CompatibleFieldSerializer.class);
		kryo.register(Object.class);
		kryo.register(Object[].class);
		kryo.register(byte.class);
		kryo.register(byte[].class);
		kryo.register(String[].class, new StringArraySerializer());
		kryo.register(Integer[].class);
		kryo.register(Long[].class);
		kryo.register(Float[].class);
		kryo.register(Double[].class);
		kryo.register(Vector.class);
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
		kryo.register(Tuple2.class, new Serializer<Tuple2<?, ?>>() {
			@Override
			public void write(Kryo kryo, Output output, Tuple2<?, ?> tuple) {
				kryo.writeClassAndObject(output, tuple.v1());
				kryo.writeClassAndObject(output, tuple.v2());
			}

			@Override
			public Tuple2<?, ?> read(Kryo kryo, Input input, Class<? extends Tuple2<?, ?>> type) {
				Object v1 = kryo.readClassAndObject(input);
				Object v2 = kryo.readClassAndObject(input);
				return Tuple.tuple(v1, v2);
			}
		});
		kryo.register(Closure.class, new ClosureSerializer());
		kryo.register(JShell.class, new CompatibleFieldSerializer<JShell>(kryo, JShell.class));
		return kryo;
	}

	/**
	 * This method configures the log4j properties and obtains the properties from
	 * the classpath in the binary distribution for mesos.
	 * 
	 * @param propertyfile
	 * @throws Exception
	 */
	public static void loadPropertiesMesos(String propertyfile) throws Exception {
		log.debug("Entered Utils.loadPropertiesMesos");
		PropertyConfigurator
				.configure(Utils.class.getResourceAsStream(DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.LOG4J_PROPERTIES));
		var prop = new Properties();
		try (var fis = Utils.class.getResourceAsStream(DataSamudayaConstants.FORWARD_SLASH + propertyfile);) {
			prop.load(fis);
			prop.putAll(System.getProperties());
			log.debug("Properties: " + prop.entrySet());
			DataSamudayaProperties.put(prop);
		} catch (Exception ex) {
			log.error("Problem in loading properties, See the cause below", ex);
		}
		log.debug("Exiting Utils.loadPropertiesMesos");
	}

	/**
	 * This function creates and configures the jgroups channel object for the given
	 * input and returns it. This is used in jgroups mode of autonomous task
	 * execution with no scheduler behind it.
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

				public void viewAccepted(View clusterview) {
				}

				public void receive(Message msg) {
					synchronized (lock) {
						var rawbuffer = (byte[]) ((ObjectMessage) msg).getObject();
						try (var baos = new ByteArrayInputStream(rawbuffer); var input = new Input(baos)) {
							var object = kryo.readClassAndObject(input);
							if (object instanceof WhoIsRequest whoisrequest) {
								if (mapreql.containsKey(whoisrequest.getStagepartitionid())) {
									log.debug("Whois: " + whoisrequest.getStagepartitionid() + " Map Status: " + mapreql
											+ " Map Response Status: " + maprespl);
									whoisresp(msg, whoisrequest.getStagepartitionid(), jobidl,
											mapreql.get(whoisrequest.getStagepartitionid()), channel, networkaddress);
								}
							} else if (object instanceof WhoIsResponse whoisresponse) {
								log.debug("WhoisResp: " + whoisresponse.getStagepartitionid() + " Status: "
										+ whoisresponse.getStatus());
								maprespl.put(whoisresponse.getStagepartitionid(), whoisresponse.getStatus());
							} else if (object instanceof WhoAreRequest) {
								log.debug("WhoAreReq: ");
								whoareresponse(channel, msg.getSrc(), mapreql);
							} else if (object instanceof WhoAreResponse whoareresponse) {
								log.debug("WhoAreResp: ");
								maprespl.putAll(whoareresponse.getResponsemap());
							}
						} catch (Exception ex) {
							log.error("In JGroups Object deserialization error: {}", ex);
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
	public static void whois(JChannel channel, String stagepartitionid) throws Exception {
		log.debug("Entered Utils.whois");
		var whoisrequest = new WhoIsRequest();
		whoisrequest.setStagepartitionid(stagepartitionid);
		try (var baos = new ByteArrayOutputStream(); var output = new Output(baos);) {
			getKryo().writeClassAndObject(output, whoisrequest);
			output.flush();
			channel.send(new ObjectMessage(null, baos.toByteArray()));
		} finally {
		}
		log.debug("Exiting Utils.whois");
	}

	/**
	 * Request the status of the all the stages whoever are the executing the stage
	 * tasks. This method is used by the job scheduler in jgroups mode of stage task
	 * executions.
	 * 
	 * @param channel
	 * @throws Exception
	 */
	public static void whoare(JChannel channel) throws Exception {
		log.debug("Entered Utils.whoare");
		var whoarerequest = new WhoAreRequest();
		try (var baos = new ByteArrayOutputStream(); var output = new Output(baos);) {
			getKryo().writeClassAndObject(output, whoarerequest);
			output.flush();
			channel.send(new ObjectMessage(null, baos.toByteArray()));
		} finally {
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
			throws Exception {
		log.debug("Entered Utils.whoareresponse");
		var whoareresp = new WhoAreResponse();
		whoareresp.setResponsemap(maptosend);
		try (var baos = new ByteArrayOutputStream(); var output = new Output(baos);) {
			getKryo().writeClassAndObject(output, whoareresp);
			output.flush();
			channel.send(new ObjectMessage(address, baos.toByteArray()));
		} finally {
		}
		log.debug("Exiting Utils.whoareresponse");
	}

	/**
	 * Response of the whois request used by the task executors to execute the next
	 * stage tasks.
	 * 
	 * @param msg
	 * @param stagepartitionid
	 * @param jobid
	 * @param status
	 * @param jchannel
	 * @param networkaddress
	 * @throws Exception
	 */
	public static void whoisresp(Message msg, String stagepartitionid, String jobid, WhoIsResponse.STATUS status,
			JChannel jchannel, String networkaddress) throws Exception {
		log.debug("Entered Utils.whoisresp");
		var whoisresponse = new WhoIsResponse();
		whoisresponse.setStagepartitionid(stagepartitionid);
		whoisresponse.setStatus(status);
		try (var baos = new ByteArrayOutputStream(); var output = new Output(baos);) {
			getKryo().writeClassAndObject(output, whoisresponse);
			output.flush();
			jchannel.send(new ObjectMessage(msg.getSrc(), baos.toByteArray()));
		} finally {
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
				log.warn("Interrupted!", e);
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
	public static void renderGraphPhysicalExecPlan(Graph<Task, DAGEdge> graph, Writer writer) throws ExportException {
		log.debug("Entered Utils.renderGraphPhysicalExecPlan");
		ComponentNameProvider<Task> vertexIdProvider = jobstage -> {

			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				log.warn("Interrupted!", e);
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
		try (var stagegraphfile = new FileWriter(path
				+ DataSamudayaProperties.get().getProperty(DataSamudayaConstants.GRAPHFILEPEPLANNAME) + System.currentTimeMillis());) {
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
	 * This function returns the object by socket using the host port of the server
	 * and the input object to the server.
	 * 
	 * @param hp
	 * @param inputobj
	 * @return object
	 * @throws Exception
	 */
	public static Object getResultObjectByInput(String hp, Object inputobj, String jobid) throws Exception {		
		try {
			var hostport = hp.split(DataSamudayaConstants.UNDERSCORE);
			final Registry registry = LocateRegistry.getRegistry(hostport[0], Integer.parseInt(hostport[1]));
			StreamDataCruncher cruncher = (StreamDataCruncher) registry
					.lookup(DataSamudayaConstants.BINDTESTUB + DataSamudayaConstants.HYPHEN + jobid);
			return cruncher.postObject(inputobj);
		} catch (Exception ex) {
			log.error("Unable to read result Object: " + inputobj + " " + hp, ex);
			throw ex;
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
			String configfilepath = System.getProperty(DataSamudayaConstants.USERDIR) + DataSamudayaConstants.FORWARD_SLASH
					+ DataSamudayaProperties.get().getProperty(DataSamudayaConstants.JGROUPSCONF);
			log.info("Composing Jgroups for address latch {} with trail {}", bindaddr, configfilepath);
			var channel = new JChannel(configfilepath);
			return channel;
		} catch (Exception ex) {
			log.error("Unable to add Protocol Stack: ", ex);
		}
		return null;
	}

	/**
	 * This function returns the list of uri in string format for the list of Path
	 * objects.
	 * 
	 * @param paths
	 * @return list of uri in string format.
	 */
	public static List<String> getAllFilePaths(List<Path> paths) {
		return paths.stream().map(path -> path.toUri().toString()).collect(Collectors.toList());
	}

	/**
	 * This function returns the total length of files in long for the given file
	 * paths in hdfs.
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
	 * Stars embedded zookeeper server for given port, number of maximum connections
	 * and tick time.
	 * 
	 * @param clientport
	 * @param numconnections
	 * @param ticktime
	 * @return Zookeeper Server factory class.
	 * @throws Exception
	 */
	public static ServerCnxnFactory startZookeeperServer(int clientport, int numconnections, int ticktime)
			throws Exception {
		var dataDirectory = System.getProperty("java.io.tmpdir");
		var dir = new File(dataDirectory, "zookeeper").getAbsoluteFile();
		var server = new ZooKeeperServer(dir, dir, ticktime);
		ServerCnxnFactory scf = ServerCnxnFactory.createFactory(new InetSocketAddress(clientport), numconnections);
		scf.startup(server);
		return scf;
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
		for (var i = 0; i < values.size(); i++) {
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
		var os = (com.sun.management.OperatingSystemMXBean) java.lang.management.ManagementFactory
				.getOperatingSystemMXBean();
		var availablePhysicalMemorySize = os.getFreePhysicalMemorySize();
		if (nonNull(DataSamudayaProperties.get().get(DataSamudayaConstants.MEMORY_FROM_NODE))) {
			Long memoryavailableinmb = Long.valueOf((String) DataSamudayaProperties.get()
					.getProperty(DataSamudayaConstants.MEMORY_FROM_NODE, DataSamudayaConstants.MEMORY_FROM_NODE_DEFAULT))
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
			int availableprocessors = Integer.valueOf((String) DataSamudayaProperties.get()
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
		var os = (com.sun.management.OperatingSystemMXBean) java.lang.management.ManagementFactory
				.getOperatingSystemMXBean();
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
		for (var i = 0; i < values.size(); i++) {
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
				BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(hdfs.create(new Path(hdfsurl + filepath),
						Short.parseShort(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
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
	 * This function returns the jobid-stageid-taskid for the rdf object as input.
	 * 
	 * @param rdf
	 * @return jobid-stageid-taskid
	 * @throws Exception
	 */
	public static String getIntermediateInputStreamRDF(RemoteDataFetch rdf) throws Exception {
		log.debug("Entered Utils.getIntermediateInputStreamRDF");
		var path = rdf.getJobid() + DataSamudayaConstants.HYPHEN + rdf.getStageid() + DataSamudayaConstants.HYPHEN + rdf.getTaskid();
		log.debug("Returned Utils.getIntermediateInputStreamRDF");
		return path;
	}

	/**
	 * This function returns the jobid-stageid-taskid for the task object as input.
	 * 
	 * @param task
	 * @return jobid-stageid-taskid
	 * @throws Exception
	 */
	public static String getIntermediateInputStreamTask(Task task) throws Exception {
		log.debug("Entered Utils.getIntermediateInputStreamTask");
		var path = task.jobid + DataSamudayaConstants.HYPHEN + task.stageid + DataSamudayaConstants.HYPHEN + task.taskid;
		log.debug("Returned Utils.getIntermediateInputStreamTask");
		return path;
	}

	/**
	 * This function launches containers for sql console.
	 * 
	 * @param user
	 * @param jobid
	 * @return list of launchcontainers object.
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public static List<LaunchContainers> launchContainers(String user, String jobid) throws Exception {
		GlobalJobFolderBlockLocations.setIsResetBlocksLocation(true);
		if (nonNull(GlobalContainerLaunchers.get(user))) {
			var lcs = GlobalContainerLaunchers.get(user);
			var lcscloned = new ArrayList<LaunchContainers>();
			for (var lc : lcs) {
				var clonedlc = SerializationUtils.clone(lc);
				clonedlc.setJobid(jobid);
				lcscloned.add(clonedlc);
			}
			return lcscloned;
		}		
		var nrs = DataSamudayaNodesResources.get();
		var resources = nrs.values();
		int numavailable = resources.size();
		Iterator<Resources> res = resources.iterator();
		var globallaunchcontainers = new ArrayList<LaunchContainers>();
		var usersshare = DataSamudayaUsers.get();
		if (isNull(usersshare)) {
			throw new Exception(String.format(PipelineConstants.USERNOTCONFIGURED, user));
		}
		PipelineConfig pc = new PipelineConfig();
		int numberofcontainerpernode = Integer.parseInt(pc.getNumberofcontainers());
		for (int container = 0; container < numavailable; container++) {
			Resources restolaunch = res.next();
			var cpu = restolaunch.getNumberofprocessors() - 1;
			var usershare = usersshare.get(user);
			if (isNull(usershare)) {
				throw new Exception(String.format(PipelineConstants.USERNOTCONFIGURED, user));
			}
			if (nonNull(usershare.getIsallocated().get(restolaunch.getNodeport()))
					&& usershare.getIsallocated().get(restolaunch.getNodeport())) {
				throw new Exception(String.format(PipelineConstants.USERALLOCATEDSHARE, user));
			}
			cpu = cpu * usershare.getPercentage() / 100;
			cpu = cpu / numberofcontainerpernode;
			if(cpu == 0) {
				cpu = 1;
			}
			var actualmemory = restolaunch.getFreememory() - DataSamudayaConstants.GB;
			if (actualmemory < (128 * DataSamudayaConstants.MB)) {
				throw new Exception(PipelineConstants.MEMORYALLOCATIONERROR);
			}
			var memoryrequire = actualmemory * usershare.getPercentage() / 100;
			var heapmem = memoryrequire * Integer.valueOf(
					DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HEAP_PERCENTAGE, DataSamudayaConstants.HEAP_PERCENTAGE_DEFAULT))
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
			log.info("Chamber alloted with node: " + restolaunch.getNodeport() + " amidst ports: " + ports);
			
			var cla = new ContainerLaunchAttributes();			
			var crl = new ArrayList<ContainerResources>();
			for(Integer port:ports) {
				var crs = new ContainerResources();
				crs.setPort(port);
				crs.setCpu(cpu);			
				crs.setMinmemory(heapmem);
				crs.setMaxmemory(heapmem);
				crs.setDirectheap(directmem);
				crs.setGctype(DataSamudayaConstants.ZGC);
				crl.add(crs);
				String conthp = restolaunch.getNodeport().split(DataSamudayaConstants.UNDERSCORE)[0] + DataSamudayaConstants.UNDERSCORE
						+ port;
				GlobalContainerAllocDealloc.getHportcrs().put(conthp, crs);
				GlobalContainerAllocDealloc.getContainernode().put(conthp, restolaunch.getNodeport());
			}
			usershare.getIsallocated().put(restolaunch.getNodeport(), true);						
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
							log.info("Waiting for chamber " + tehost + DataSamudayaConstants.UNDERSCORE
									+ launchedcontainerports.get(index) + " to replete dispatch....");
							Thread.sleep(1000);
						} catch (InterruptedException e) {
							log.warn("Interrupted!", e);
							// Restore interrupted state...
							Thread.currentThread().interrupt();
						} catch (Exception e) {
							log.error(DataSamudayaConstants.EMPTY, e);
						}
					}
				}
				index++;
			}
			log.info(
					"Chamber dispatched node: " + restolaunch.getNodeport() + " with ports: " + launchedcontainerports);
		}
		GlobalContainerLaunchers.put(user, globallaunchcontainers);
		return globallaunchcontainers;
	}

	/**
	 * This function returns true if user exists with the allocation share
	 * percentage. returns false if no configuration for the given user exists.
	 * 
	 * @param user
	 * @return true or false
	 * @throws Exception
	 */
	public static Boolean isUserExists(String user) throws Exception {
		var usersshare = DataSamudayaUsers.get();
		var usershare = usersshare.get(user);
		if (isNull(usershare)) {
			return false;
		}
		return true;
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
	 * This method destroys containers for the given user and jobid.
	 * 
	 * @param user
	 * @param jobid
	 * @throws Exception
	 */
	public static void destroyContainers(String user, String jobid) throws Exception {
		var dc = new DestroyContainers();
		dc.setJobid(jobid);
		var usersshare = DataSamudayaUsers.get();
		if (isNull(usersshare)) {
			throw new Exception(String.format(PipelineConstants.USERNOTCONFIGURED, user));
		}
		var usershare = usersshare.get(user);
		var lcs = GlobalContainerLaunchers.get(user);
		lcs.stream().forEach(lc -> {
			try {
				Utils.getResultObjectByInput(lc.getNodehostport(), dc, DataSamudayaConstants.EMPTY);
				usershare.getIsallocated().put(lc.getNodehostport(), false);
			} catch (Exception e) {
				log.error(DataSamudayaConstants.EMPTY, e);
			}
		});
		GlobalContainerLaunchers.remove(user);
		GlobalJobFolderBlockLocations.remove(jobid);
	}

	static List<Object> objects = new ArrayList<>();

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
			throws Exception {
		objects.add(streamdatacruncher);
		Registry registry = LocateRegistry.createRegistry(port);
		StreamDataCruncher stub = (StreamDataCruncher) UnicastRemoteObject.exportObject(streamdatacruncher, port);
		registry.rebind(DataSamudayaConstants.BINDTESTUB + DataSamudayaConstants.HYPHEN + id, stub);
		return registry;
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
	 * This function returns the ssl server socket for the given port.
	 * 
	 * @param port
	 * @return ssl server socket object
	 * @throws Exception
	 */
	public static ServerSocket createSSLServerSocket(int port) throws Exception {
		KeyStore ks = KeyStore.getInstance("JKS");
		String password = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DATASAMUDAYA_KEYSTORE_PASSWORD);
		ks.load(new FileInputStream(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DATASAMUDAYA_JKS)), password.toCharArray());

		KeyManagerFactory kmf = KeyManagerFactory
				.getInstance(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DATASAMUDAYA_JKS_ALGO));
		kmf.init(ks, password.toCharArray());

		TrustManagerFactory tmf = TrustManagerFactory
				.getInstance(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DATASAMUDAYA_JKS_ALGO));
		tmf.init(ks);

		SSLContext sc = SSLContext.getInstance("TLS");
		TrustManager[] trustManagers = tmf.getTrustManagers();
		sc.init(kmf.getKeyManagers(), trustManagers, null);
		SSLServerSocketFactory ssf = sc.getServerSocketFactory();
		SSLServerSocket sslserversocket = (SSLServerSocket) ssf.createServerSocket();
		sslserversocket.bind(
				new InetSocketAddress(InetAddress.getByAddress(new byte[] { 0x00, 0x00, 0x00, 0x00 }), port), 256);
		return sslserversocket;
	}

	/**
	 * This function returns the ssl socket for the given port.
	 * 
	 * @param port
	 * @return ssl socket object
	 * @throws Exception
	 */
	public static Socket createSSLSocket(String host, int port) throws Exception {
		log.info("Constructing socket factory for the (host,port): (" + host + "," + port + ")");
		KeyStore ks = KeyStore.getInstance("JKS");
		String password = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DATASAMUDAYA_KEYSTORE_PASSWORD);
		ks.load(new FileInputStream(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DATASAMUDAYA_JKS)), password.toCharArray());

		KeyManagerFactory kmf = KeyManagerFactory
				.getInstance(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DATASAMUDAYA_JKS_ALGO));
		kmf.init(ks, password.toCharArray());

		TrustManagerFactory tmf = TrustManagerFactory
				.getInstance(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DATASAMUDAYA_JKS_ALGO));
		tmf.init(ks);

		SSLContext sc = SSLContext.getInstance("TLS");
		TrustManager[] trustManagers = tmf.getTrustManagers();
		sc.init(kmf.getKeyManagers(), trustManagers, null);

		SSLSocketFactory sf = sc.getSocketFactory();
		log.info("Constructing SSLSocket for the (host,port): (" + host + "," + port + ")");
		SSLSocket sslsocket = (SSLSocket) sf.createSocket(host, port);
		log.info("Kickoff SSLHandshake for the (host,port): (" + host + "," + port + ")");
		sslsocket.startHandshake();
		log.info("SSLHandshake concluded for the (host,port): (" + host + "," + port + ")");
		return sslsocket;
	}

	/**
	 * Write integer value to scheduler
	 * 
	 * @param os
	 * @param value
	 * @throws Exception
	 */
	public static void writeInt(OutputStream os, Integer value) throws Exception {
		try (var baos = new ByteArrayOutputStream(); var output = new Output(baos);) {
			Utils.getKryo().writeClassAndObject(output, value);
			output.flush();
			byte[] values = baos.toByteArray();
			DataOutputStream dos = new DataOutputStream(os);
			dos.writeInt(values.length);
			dos.write(values);
			dos.flush();
		}
	}

	/**
	 * Prints Information about Nodes and Containers
	 * @param lcs
	 * @param out
	 */
	public static void printNodesAndContainers(List<LaunchContainers> lcs, PrintWriter out) {
		for(LaunchContainers lc:lcs) {
			out.println();
			out.println(lc.getNodehostport()+":");
			for(ContainerResources crs:lc.getCla().getCr()) {
				out.print(DataSamudayaConstants.TAB);
				out.println("cpu: "+crs.getCpu()+" memory: "+(crs.getMaxmemory()/DataSamudayaConstants.MB+crs.getDirectheap()/DataSamudayaConstants.MB)+" mb port:"+crs.getPort());
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
	public static void writeDataStream(OutputStream os, byte[] outbyt) throws Exception {
		try (var baos = new ByteArrayOutputStream(); var output = new Output(baos);) {
			Utils.getKryo().writeClassAndObject(output, outbyt);
			output.flush();
			byte[] values = baos.toByteArray();
			DataOutputStream dos = new DataOutputStream(os);
			dos.writeInt(values.length);
			dos.write(values);
			dos.flush();
		}
	}

	/**
	 * This method sends the sql data to the ouput stream.
	 * 
	 * @param data
	 * @param out
	 */
	public static void printTableOrError(List data, PrintWriter out, JOBTYPE jobtype) {
		if (data.isEmpty()) {
			out.println("No data available to display.");
			return;
		}

		if (data.get(0) instanceof String errors) {
			for (Object error : data) {
				out.println(error);
			}
			return;
		} else if (data.get(0) instanceof DataCruncherContext dcc) {
			Map<String, Object> mapheadervalue = (Map<String, Object>) dcc.get("Reduce").iterator().next();
			String[] headers = mapheadervalue.keySet().toArray(new String[0]);
			// Initialize a two-dimensional array to hold the data

			// Print the table headers
			for (String header : headers) {
				out.printf("%-20s", header); // adjust width as needed
			}
			out.println();
			Iterator<Map<String, Object>> ite = dcc.get("Reduce").iterator();
			for (; ite.hasNext();) {
				Map<String, Object> row = (Map<String, Object>) ite.next();
				for (String header : headers) {
					out.printf("%-20s", row.get(header));
				}
				out.println();
			}
			return;
		} else if (data.get(0) instanceof Double value) {
			out.println(value);
			return;
		} else if (data.get(0) instanceof Integer value) {
			out.println(value);
			return;
		} else if (data.get(0) instanceof Float value) {
			out.println(value);
			return;
		} else if (data.get(0) instanceof Long value) {
			out.println(value);
			return;
		}
		
		if(jobtype == JOBTYPE.NORMAL) {
			printTable(data, out);
		} else if(jobtype == JOBTYPE.PIG) {
			printTablePig(data, out);
		}
	}
	
	private static void printTablePig(List<Map<String, Object>> tableData, PrintWriter out) {
		if (tableData.isEmpty()) {
            out.println("Table is empty.");
            out.flush();
            return;
        }
		Map<String, Object> mapforkeys = tableData.get(0);
        Set<String> keys = mapforkeys.keySet();
        out.print("(");
        out.flush();
        for(String key: keys) {
        	out.print(key);
        	out.print(",");
        	out.flush();
        }
        out.print(")");
        out.println();
        out.flush();
		for (Map<String, Object> row : tableData) {
			out.print("(");
	        out.flush();
			for(String key: keys) {
				out.print(row.get(key));
				out.print(",");
                out.flush();
            }
			out.print(")");
	        out.flush();
	        out.println();
            out.flush();
        }
	}

	/**
	 * Prints the result in map to output
	 * @param tableData
	 * @param out
	 */
	private static void printTable(List<Map<String, Object>> tableData, PrintWriter out) {
        if (tableData.isEmpty()) {
            out.println("Table is empty.");
            out.flush();
            return;
        }

                
        // Calculate column widths
        Map<String, Integer> columnWidths = new HashMap<>();
        Map<String, Object> mapforkeys = tableData.get(0);
        Set<String> keys = mapforkeys.keySet();
        for(String key: keys) {
        	columnWidths.put(key, key.length());
        }       
        for (Map<String, Object> row : tableData) {
            for (Map.Entry<String, Object> entry : row.entrySet()) {
                String key = entry.getKey();
                String value = String.valueOf(entry.getValue());
                int width = Math.max(columnWidths.getOrDefault(key, 0), value.length());
                columnWidths.put(key, width);
            }
        }

        // Print table header
        for (Map.Entry<String, Integer> entry : columnWidths.entrySet()) {
            out.printf("%-" + entry.getValue() + "s | ", entry.getKey());
            out.flush();
        }
        out.println();
        out.flush();

        // Print table rows
        for (Map<String, Object> row : tableData) {
            for (Map.Entry<String, Integer> entry : columnWidths.entrySet()) {
                String key = entry.getKey();
                Object value = row.get(key);
                out.printf("%-" + entry.getValue() + "s | ", value);
                out.flush();
            }
            out.println();
            out.flush();
        }
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
					log.info("port {} in use, so getting port again...", port + DataSamudayaConstants.PORT_OFFSET);
					continue;
				}
				return port;
			} catch (Exception e) {
				continue;
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
							// Remove the container from global container node map
							String node = GlobalContainerAllocDealloc.getContainernode().remove(te);
							Set<String> containers = GlobalContainerAllocDealloc.getNodecontainers().get(node);
							containers.remove(te);
							// Remove the container from the node and destroy it.
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
							} else {
								var usersshare = DataSamudayaUsers.get();
								var user = usersshare.get(job.getPipelineconfig().getUser());
								user.getIsallocated().remove(node);
							}
						} else {
							deallocateall = false;
						}
					}
				}
				if (deallocateall) {
					var dc = new DestroyContainers();
					dc.setJobid(job.getId());
					log.debug("Destroying Containers with id:" + job.getId() + " for the hosts: " + nodes);
					// Destroy all the containers from all the nodes
					for (var node : nodes) {
						Utils.getResultObjectByInput(node, dc, DataSamudayaConstants.EMPTY);
					}
				}
			}
		} catch (Exception ex) {
			log.error(PipelineConstants.DESTROYCONTAINERERROR, ex);
			throw new Exception(PipelineConstants.DESTROYCONTAINERERROR, ex);
		} finally {
			GlobalContainerAllocDealloc.getGlobalcontainerallocdeallocsem().release();
		}
	}

	static SimpleDateFormat format = new SimpleDateFormat("dd-MM-yyyy HH:mm:ss");
	
	/**
	 * Formats date from util date object 
	 * @param date
	 * @return formatted date
	 */
	public static String formatDate(Date date) {
		return format.format(date);
	}
	
	/**
	 * Generates path of intermediate result file system for given task
	 * @param task
	 * @return path of intermediate result file system
	 * @throws Exception
	 */
	public static String getIntermediateResultFS(Task task) throws Exception {
		return task.jobid + DataSamudayaConstants.HYPHEN + task.stageid + DataSamudayaConstants.HYPHEN + task.taskid;
	}
}
