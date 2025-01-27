package com.github.datasamudaya.stream.sql;

import static java.util.Objects.nonNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.ColumnMetadata;
import com.github.datasamudaya.common.ContainerException;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.LaunchContainers;
import com.github.datasamudaya.common.Job.JOBTYPE;
import com.github.datasamudaya.common.exceptions.RpcRegistryException;
import com.github.datasamudaya.common.utils.DataSamudayaMetricsExporter;
import com.github.datasamudaya.common.utils.Utils;

public class SQLServer {
	static Logger log = LoggerFactory.getLogger(SQLServer.class);
	static ServerSocket serverSocket;

	public static void main(String[] args) {
		try {
			start();
		} catch (Exception e) {
			log.error("Error starting SQLServer", e);
		}
	}

	/**
	 * Start the SQL server.
	 * 
	 * @throws Exception
	 */
	public static void start() throws Exception {
		ExecutorService executors = Executors
				.newFixedThreadPool(
						Integer.parseInt(
								DataSamudayaProperties.get().getProperty(DataSamudayaConstants.VIRTUALTHREADSPOOLSIZE,
										DataSamudayaConstants.VIRTUALTHREADSPOOLSIZE_DEFAULT)),
						Thread.ofVirtual().factory());

		serverSocket = new ServerSocket(Integer.valueOf(DataSamudayaProperties.get()
				.getProperty(DataSamudayaConstants.SQLPORT, DataSamudayaConstants.SQLPORT_DEFAULT)));

		executors.execute(() -> {
			while (true) {
				try {
					Socket sock = serverSocket.accept();
					handleClientConnection(sock, executors);
				} catch (Exception e) {
					log.error("Error accepting client connection", e);
				}
			}
		});
	}

	private static void handleClientConnection(Socket sock, ExecutorService executors) {
		executors.execute(() -> {
			String user = "";
			int numberofcontainers = 1;
			int cpupercontainer = 1;
			int memorypercontainer = 1024;
			int cpudriver = 1;
			int memorydriver = 1024;
			AtomicBoolean isdriverrequired = new AtomicBoolean(false);
			String scheduler = "";
			String tejobid = DataSamudayaConstants.EMPTY;
			AtomicBoolean iscontainerlaunched = new AtomicBoolean(false);
			AtomicBoolean isyarncontainerlaunched = new AtomicBoolean(false);
			SessionState sessionState = null;
			PrintWriter out = null;
			BufferedReader in = null;
			try {
				out = new PrintWriter(sock.getOutputStream(), true);
				in = new BufferedReader(new InputStreamReader(sock.getInputStream()));
				// Handle initial parameters from the client
				user = in.readLine();
				numberofcontainers = Integer.valueOf(in.readLine());
				cpupercontainer = Integer.valueOf(in.readLine());
				memorypercontainer = Integer.valueOf(in.readLine());
				cpudriver = Integer.valueOf(in.readLine());
				memorydriver = Integer.valueOf(in.readLine());
				isdriverrequired.set(Boolean.parseBoolean(in.readLine()));
				scheduler = in.readLine();
				tejobid = in.readLine();
				sessionState = Utils.startHiveSession(user);

				// Validate user
				if (!Utils.isUserExists(user)) {
					out.println("User " + user + " is not configured. Exiting...");
					out.println("Quit");
					throw new Exception("User " + user + " is not configured.");
				}

				// Simulating the launch of containers and resources
				List<LaunchContainers> containers = null;
				Map<String, Object> cpumemory = null;

				// Launching containers if required
				if (scheduler.equalsIgnoreCase(DataSamudayaConstants.EXECMODE_DEFAULT)
						|| scheduler.equalsIgnoreCase(DataSamudayaConstants.JGROUPS)) {
					containers = Utils.launchContainersExecutorSpecWithDriverSpec(user, tejobid, cpupercontainer,
							memorypercontainer, numberofcontainers, cpudriver, memorydriver, true);
					cpumemory = Utils.getAllocatedContainersResources(containers);
					out.println("User '" + user + "' connected with cpu " + cpumemory.get(DataSamudayaConstants.CPUS)
							+ " and memory " + cpumemory.get(DataSamudayaConstants.MEM) + " MB");
					Utils.printNodesAndContainers(containers, out);
					iscontainerlaunched.set(true);
				}
				var isjgroups = new AtomicBoolean(false);
				var isignite = new AtomicBoolean(false);
				var isyarn = new AtomicBoolean(false);
				// Handle different scheduler modes (e.g., YARN, JGroups, etc.)
				handleSchedulerModes(out, user, tejobid, cpupercontainer, memorypercontainer, numberofcontainers,
						cpudriver, memorydriver, scheduler, isdriverrequired, iscontainerlaunched, isyarncontainerlaunched
				, isjgroups, isignite, isyarn);
				out.println("Welcome to the SQL Server!");
				out.println("Type 'quit' to exit.");
				out.println("Done");
				// Start processing commands from the user
				interactWithUser(out, in, user, tejobid, cpupercontainer, memorypercontainer, numberofcontainers,
						cpudriver, memorydriver, sessionState, isdriverrequired, iscontainerlaunched, isyarncontainerlaunched, isjgroups, isignite, isyarn);

			} catch (Exception ex) {
				log.error("Error handling client connection", ex);
				out.println(ex.getMessage());
				out.println("Quit");
			} finally {
				try {
					cleanup(iscontainerlaunched, isyarncontainerlaunched, sessionState, user, tejobid);
					if(nonNull(out)) {
						out.close();
					}
					if(nonNull(in)) {
						in.close();
					}
					if(nonNull(sock)) {
						sock.close();
					}
				} catch (Exception e) {
				}
			}
		});
	}

	/**
	 * The method handles scheduler modes
	 * @param out
	 * @param user
	 * @param tejobid
	 * @param cpupercontainer
	 * @param memorypercontainer
	 * @param numberofcontainers
	 * @param cpudriver
	 * @param memorydriver
	 * @param scheduler
	 * @param isdriverrequired
	 * @param iscontainerlaunched
	 * @param isyarncontainerlaunched
	 * @param isjgroups
	 * @param isignite
	 * @param isyarn
	 * @throws Exception
	 */
	private static void handleSchedulerModes(PrintWriter out, String user, String tejobid, int cpupercontainer,
			int memorypercontainer, int numberofcontainers, int cpudriver, int memorydriver, String scheduler,
			AtomicBoolean isdriverrequired, AtomicBoolean iscontainerlaunched, AtomicBoolean isyarncontainerlaunched,
			AtomicBoolean isjgroups, AtomicBoolean isignite, AtomicBoolean isyarn) throws Exception {
		// Log the scheduler mode
		out.println("Scheduler mode: " + scheduler);

		String schedulerupcase = scheduler.toLowerCase();

		// Handle different scheduler modes
		switch (schedulerupcase) {
			case "yarn":
				handleYarnScheduler(out, user, tejobid, cpupercontainer, memorypercontainer, numberofcontainers, cpudriver,
						memorydriver, isdriverrequired, iscontainerlaunched, isjgroups, isignite, isyarn);
				isyarncontainerlaunched.set(true);
				break;

			case "jgroups":
				handleJGroupsScheduler(out, user, tejobid, cpupercontainer, memorypercontainer, numberofcontainers,
						cpudriver, memorydriver, isdriverrequired, iscontainerlaunched, isjgroups, isignite, isyarn);
				break;

			case "ignite":
				handleIgniteScheduler(out, user, tejobid, cpupercontainer, memorypercontainer, numberofcontainers,
						cpudriver, memorydriver, isdriverrequired, iscontainerlaunched, isjgroups, isignite, isyarn);
				break;

			case "standalone":
				handleStandaloneScheduler(out, user, tejobid, cpupercontainer, memorypercontainer, numberofcontainers,
						cpudriver, memorydriver, isdriverrequired, iscontainerlaunched, isjgroups, isignite, isyarn);
				break;

			default:
				out.println("Unknown scheduler mode: " + scheduler + "setting default standalone");
				handleStandaloneScheduler(out, user, tejobid, cpupercontainer, memorypercontainer, numberofcontainers,
						cpudriver, memorydriver, isdriverrequired, iscontainerlaunched, isjgroups, isignite, isyarn);
		}
	}

	/**
	 * The Yarn Scheduler mode is set
	 * @param out
	 * @param user
	 * @param tejobid
	 * @param cpupercontainer
	 * @param memorypercontainer
	 * @param numberofcontainers
	 * @param cpudriver
	 * @param memorydriver
	 * @param isdriverrequired
	 * @param iscontainerlaunched
	 * @param isjgroups
	 * @param isignite
	 * @param isyarn
	 * @throws Exception
	 */
	private static void handleYarnScheduler(PrintWriter out, String user, String tejobid, int cpupercontainer,
			int memorypercontainer, int numberofcontainers, int cpudriver, int memorydriver, AtomicBoolean isdriverrequired,
			AtomicBoolean iscontainerlaunched, AtomicBoolean isjgroups, AtomicBoolean isignite, AtomicBoolean isyarn) throws Exception {
		// Mocking YARN scheduler behavior
		out.println("YARN Scheduler: Allocating " + numberofcontainers + " containers with " + cpupercontainer
				+ " CPUs and " + memorypercontainer + " MB of memory each.");

		if (isdriverrequired.get()) {
			out.println("YARN Scheduler: Allocating driver with " + cpudriver + " CPUs and " + memorydriver
					+ " MB of memory.");
		}
		Utils.launchYARNExecutors(tejobid, cpupercontainer, memorypercontainer, numberofcontainers, DataSamudayaConstants.SQL_YARN_DEFAULT_APP_CONTEXT_FILE, isdriverrequired.get());
		// Additional YARN-specific logic can be added here, e.g., YARN API integration
		isjgroups.set(false);
		isignite.set(false);
		isyarn.set(true);
		out.println("YARN Scheduler: Containers and driver allocation complete.");
	}

	/**
	 * The jgroups scheduler mode is set.
	 * @param out
	 * @param user
	 * @param tejobid
	 * @param cpupercontainer
	 * @param memorypercontainer
	 * @param numberofcontainers
	 * @param cpudriver
	 * @param memorydriver
	 * @param isdriverrequired
	 * @param iscontainerlaunched
	 * @param isjgroups
	 * @param isignite
	 * @param isyarn
	 */
	private static void handleJGroupsScheduler(PrintWriter out, String user, String tejobid, int cpupercontainer,
			int memorypercontainer, int numberofcontainers, int cpudriver, int memorydriver, AtomicBoolean isdriverrequired,
			AtomicBoolean iscontainerlaunched, AtomicBoolean isjgroups, AtomicBoolean isignite, AtomicBoolean isyarn) {
		// Mocking JGROUPS scheduler behavior
		out.println("JGROUPS Scheduler: Setting up JGroups communication between containers.");

		out.println("JGROUPS Scheduler: Allocating " + numberofcontainers + " containers with " + cpupercontainer
				+ " CPUs and " + memorypercontainer + " MB of memory each.");

		if (isdriverrequired.get()) {
			out.println("JGROUPS Scheduler: Allocating driver with " + cpudriver + " CPUs and " + memorydriver
					+ " MB of memory.");
		}

		// Additional JGROUPS-specific logic can be added here, e.g., JGroups messaging
		// setup
		isjgroups.set(true);
		isignite.set(false);
		isyarn.set(false);
		out.println("JGROUPS Scheduler: Containers and driver allocation complete.");
	}

	/**
	 * Ignite Scheduler Mode is Set 
	 * @param out
	 * @param user
	 * @param tejobid
	 * @param cpupercontainer
	 * @param memorypercontainer
	 * @param numberofcontainers
	 * @param cpudriver
	 * @param memorydriver
	 * @param isdriverrequired
	 * @param iscontainerlaunched
	 * @param isjgroups
	 * @param isignite
	 * @param isyarn
	 */
	private static void handleIgniteScheduler(PrintWriter out, String user, String tejobid, int cpupercontainer,
			int memorypercontainer, int numberofcontainers, int cpudriver, int memorydriver, AtomicBoolean isdriverrequired,
			AtomicBoolean iscontainerlaunched, AtomicBoolean isjgroups, AtomicBoolean isignite, AtomicBoolean isyarn) {
		// Mocking Ignite scheduler behavior
		out.println("Ignite Scheduler: Setting up Ignite cluster with " + numberofcontainers + " nodes.");

		out.println("Ignite Scheduler: Allocating " + numberofcontainers + " containers with " + cpupercontainer
				+ " CPUs and " + memorypercontainer + " MB of memory each.");

		if (isdriverrequired.get()) {
			out.println("Ignite Scheduler: Allocating driver with " + cpudriver + " CPUs and " + memorydriver
					+ " MB of memory.");
		}

		// Additional Ignite-specific logic can be added here, e.g., Ignite node setup
		// and cluster configuration
		isjgroups.set(false);
		isignite.set(true);
		isyarn.set(false);
		out.println("Ignite Scheduler: Containers and driver allocation complete.");
	}

	/**
	 * Standalone scheduler mode is set
	 * @param out
	 * @param user
	 * @param tejobid
	 * @param cpupercontainer
	 * @param memorypercontainer
	 * @param numberofcontainers
	 * @param cpudriver
	 * @param memorydriver
	 * @param isdriverrequired
	 * @param iscontainerlaunched
	 * @param isjgroups
	 * @param isignite
	 * @param isyarn
	 */
	private static void handleStandaloneScheduler(PrintWriter out, String user, String tejobid, int cpupercontainer,
			int memorypercontainer, int numberofcontainers, int cpudriver, int memorydriver, AtomicBoolean isdriverrequired,
			AtomicBoolean iscontainerlaunched, AtomicBoolean isjgroups, AtomicBoolean isignite, AtomicBoolean isyarn) {
		// Mocking Standalone scheduler behavior
		out.println("Standalone Scheduler: Running on a single node.");

		out.println("Standalone Scheduler: Allocating " + numberofcontainers + " containers with " + cpupercontainer
				+ " CPUs and " + memorypercontainer + " MB of memory each.");

		if (isdriverrequired.get()) {
			out.println("Standalone Scheduler: Allocating driver with " + cpudriver + " CPUs and " + memorydriver
					+ " MB of memory.");
		}
		isjgroups.set(false);
		isignite.set(false);
		isyarn.set(false);
		// Standalone mode does not require cluster setup, so we assume everything is
		// running locally
		out.println("Standalone Scheduler: Containers and driver allocation complete.");
	}

	/**
	 * The method interacts with user and execute queries 
	 * @param out
	 * @param in
	 * @param user
	 * @param tejobid
	 * @param cpupercontainer
	 * @param memorypercontainer
	 * @param numberofcontainers
	 * @param cpudriver
	 * @param memorydriver
	 * @param sessionState
	 * @param isdriverrequired
	 * @param iscontainerlaunched
	 * @param isyarncontainerlaunched
	 * @param isjgroups
	 * @param isignite
	 * @param isyarn
	 */
	private static void interactWithUser(PrintWriter out, BufferedReader in, String user, String tejobid,
			int cpupercontainer,
			int memorypercontainer, int numberofcontainers, int cpudriver, int memorydriver,
			SessionState sessionState,
			AtomicBoolean isdriverrequired, AtomicBoolean iscontainerlaunched, AtomicBoolean isyarncontainerlaunched
		, AtomicBoolean isjgroups,
			AtomicBoolean isignite, AtomicBoolean isyarn) {
		String inputLine;
		String dbdefault = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.SQLDB,
				DataSamudayaConstants.SQLMETASTORE_DB);
		var atomdbdefault = new AtomicReference();
		atomdbdefault.set(dbdefault);
		while (true) {
			try {
				inputLine = in.readLine();
				if (inputLine == null) {
					break;
				}

				if ("quit".equalsIgnoreCase(inputLine)) {
					out.println("Quit");
					break;
				}

				out.println(Utils.formatDate(new Date()) + "> " + inputLine);
				inputLine = StringUtils.normalizeSpace(inputLine.trim());
				handleCommands(out, inputLine, user, atomdbdefault, tejobid, cpupercontainer, memorypercontainer, numberofcontainers, cpudriver,
						memorydriver, isdriverrequired, iscontainerlaunched, isyarncontainerlaunched, isjgroups, isignite, isyarn);
				out.println("Done");
			} catch (SocketException socketexception) {
				log.error("SocketException", socketexception);
				break;
			} catch (Exception exception) {
				log.error("Exception", exception);
			}
		}
	}

	/**
	 * The method handle commands from user
	 * @param out
	 * @param inputLine
	 * @param user
	 * @param dbdefault
	 * @param tejobid
	 * @param cpupercontainer
	 * @param memorypercontainer
	 * @param numberofcontainers
	 * @param cpudriver
	 * @param memorydriver
	 * @param isdriverrequired
	 * @param iscontainerlaunched
	 * @param isyarncontainerlaunched
	 * @param isjgroups
	 * @param isignite
	 * @param isyarn
	 * @throws Exception
	 */
	private static void handleCommands(PrintWriter out, String inputLine, String user, AtomicReference<String> dbdefault, String tejobid,
			int cpupercontainer,
			int memorypercontainer, int numberofcontainers, int cpudriver, int memorydriver,
			AtomicBoolean isdriverrequired,
			AtomicBoolean iscontainerlaunched, AtomicBoolean isyarncontainerlaunched,
			AtomicBoolean isjgroups, AtomicBoolean isignite, AtomicBoolean isyarn) throws Exception {
		if (inputLine.startsWith("setmode")) {
			handleSetMode(out, inputLine, user, dbdefault.get(), tejobid, cpupercontainer, memorypercontainer, numberofcontainers, cpudriver,
					memorydriver, isdriverrequired, iscontainerlaunched, isyarncontainerlaunched, isjgroups, isignite, isyarn);
		} else if (inputLine.startsWith("getmode")) {
			getMode(out, isjgroups, isignite, isyarn);
		} else if (inputLine.startsWith("use")) {
			dbdefault.set(StringUtils.normalizeSpace(inputLine.trim()).split(" ")[1]);
		} else if (inputLine.startsWith("getdb")) {
			out.println(dbdefault.get());
		} else if (inputLine.startsWith("create")) {
			handleCreate(out, inputLine, user, dbdefault.get());
		} else if (inputLine.startsWith("alter")) {
			handleAlter(out, inputLine, user, dbdefault.get());
		} else if (inputLine.startsWith("drop")) {
			handleDrop(out, inputLine, user, dbdefault.get());
		} else if (inputLine.startsWith("show")) {
			Utils.printTableOrError(TableCreator.showTables(user, dbdefault.get(), inputLine), out, JOBTYPE.NORMAL);
		} else if (inputLine.startsWith("explain")) {
			SelectQueryExecutor.explain(user, dbdefault.get(), inputLine.replaceFirst("explain", ""), out);
		} else if (inputLine.startsWith("describe")) {
			var columns = new ArrayList<ColumnMetadata>();
			TableCreator.getColumnMetadataFromTable(user, dbdefault.get(), inputLine.split(" ")[1], columns);
			for (ColumnMetadata colmetadata : columns) {
				out.println(colmetadata);
			}
		} else if (inputLine.startsWith("select")) {
			handleSelect(out, inputLine, user, dbdefault.get(), tejobid,
					isjgroups, isyarn, isignite, isdriverrequired, memorypercontainer);
		} else {
			out.println("Unknown command.");
		}
	}

	/**
	 * The create sql command is executed.
	 * @param out
	 * @param inputLine
	 * @param user
	 * @param dbdefault
	 * @throws Exception
	 */
	private static void handleCreate(PrintWriter out, String inputLine, String user, String dbdefault) throws Exception {
		out.println(TableCreator.createAlterTable(user, dbdefault, inputLine));
		out.println("CREATE command executed: " + inputLine);
	}

	/**
	 * The method executes alter command
	 * @param out
	 * @param inputLine
	 * @param user
	 * @param dbdefault
	 * @throws Exception
	 */
	private static void handleAlter(PrintWriter out, String inputLine, String user, String dbdefault) throws Exception {
		out.println(TableCreator.createAlterTable(user, dbdefault, inputLine));
		out.println("ALTER command executed: " + inputLine);
	}

	/**
	 * The method executes drop command
	 * @param out
	 * @param inputLine
	 * @param user
	 * @param dbdefault
	 * @throws Exception
	 */
	private static void handleDrop(PrintWriter out, String inputLine, String user, String dbdefault) throws Exception {
		out.println(TableCreator.dropTable(user, dbdefault, inputLine));
		out.println("DROP command executed: " + inputLine);
	}

	/**
	 * The method executes select query and prints result to user
	 * @param out
	 * @param inputLine
	 * @param user
	 * @param dbdefault
	 * @param tejobid
	 * @param isjgroups
	 * @param isyarn
	 * @param isignite
	 * @param isdriverrequired
	 * @param memorypercontainer
	 * @throws Exception
	 */
	private static void handleSelect(PrintWriter out, String inputLine, String user, String dbdefault, String tejobid,
			AtomicBoolean isjgroups, AtomicBoolean isyarn, AtomicBoolean isignite, AtomicBoolean isdriverrequired, long memorypercontainer) throws Exception {
		long starttime = System.currentTimeMillis();
		String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		List<List> results = null;
		if (isignite.get()) {
			DataSamudayaMetricsExporter.getNumberOfSqlQueriesCounter().inc();
			results = SelectQueryExecutor.executeSelectQueryIgnite(dbdefault, inputLine, user, jobid, tejobid, Long.valueOf((long) memorypercontainer) * DataSamudayaConstants.MB);
		} else {
			DataSamudayaMetricsExporter.getNumberOfSqlQueriesCounter().inc();
			results = SelectQueryExecutor.executeSelectQuery(dbdefault, inputLine, user, jobid, tejobid, isjgroups.get(), isyarn.get(), out, isdriverrequired.get());
		}
		double timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
		int partitionno = 1;
		long totalrecords = 0;
		for (List result : results) {
			out.println("Partition" + partitionno);
			totalrecords += Utils.printTableOrError(result, out, JOBTYPE.NORMAL);
			partitionno++;
		}
		out.printf("Total records processed %d", totalrecords);
		out.println("");
		out.println("Time taken " + timetaken + " seconds");
		out.println("");
		out.println("SELECT command executed: " + inputLine);
	}

	/**
	 * The methods prints current scheduler
	 * @param out
	 * @param isjgroups
	 * @param isignite
	 * @param isyarn
	 */
	private static void getMode(PrintWriter out, AtomicBoolean isjgroups,
			AtomicBoolean isignite, AtomicBoolean isyarn) {
		if (isignite.get()) {
			out.println("ignite");
		} else if (isjgroups.get()) {
			out.println("jgroups");
		} else if (isyarn.get()) {
			out.println("yarn");
		} else {
			out.println("standalone");
		}
	}

	/**
	 * The launched containers and session cleanup.
	 * @param iscontainerlaunched
	 * @param isyarncontainerlaunched
	 * @param sessionState
	 * @param user
	 * @param tejobid
	 * @throws Exception
	 */
	private static void cleanup(AtomicBoolean iscontainerlaunched, AtomicBoolean isyarncontainerlaunched,
			SessionState sessionState, String user, String tejobid)
			throws Exception {
		if (iscontainerlaunched.get()) {
			try {
				Utils.destroyContainers(user, tejobid);
			} catch (Exception ex) {
				log.error(DataSamudayaConstants.EMPTY, ex);
			}
		} else if (isyarncontainerlaunched.get()) {
			try {
				Utils.shutDownYARNContainer(tejobid);
			} catch (Exception ex) {
				log.error(DataSamudayaConstants.EMPTY, ex);
			}
		}
		if (nonNull(sessionState)) {
			try {
				Utils.endStartHiveSession(sessionState);
			} catch (Exception ex) {
				log.error(DataSamudayaConstants.EMPTY, ex);
			}
		}
	}

	/**
	 * The method sets current mode.
	 * @param out
	 * @param inputLine
	 * @param user
	 * @param dbdefault
	 * @param tejobid
	 * @param cpupercontainer
	 * @param memorypercontainer
	 * @param numberofcontainers
	 * @param cpudriver
	 * @param memorydriver
	 * @param isdriverrequired
	 * @param iscontainerlaunched
	 * @param isyarncontainerlaunched
	 * @param isjgroups
	 * @param isignite
	 * @param isyarn
	 * @throws ContainerException
	 * @throws InterruptedException
	 * @throws RpcRegistryException
	 */
	private static void handleSetMode(PrintWriter out, String inputLine, String user, String dbdefault, String tejobid,
			int cpupercontainer,
			int memorypercontainer, int numberofcontainers, int cpudriver, int memorydriver,
			AtomicBoolean isdriverrequired,
			AtomicBoolean iscontainerlaunched, AtomicBoolean isyarncontainerlaunched,
			AtomicBoolean isjgroups, AtomicBoolean isignite, AtomicBoolean isyarn) throws ContainerException, InterruptedException, RpcRegistryException {
		String[] mode = inputLine.split(" ");
		if (mode.length == 2) {
			if (mode[1].equalsIgnoreCase(DataSamudayaConstants.JGROUPS)) {
				isjgroups.set(true);
				isignite.set(false);
				isyarn.set(false);
				if (isyarncontainerlaunched.get()) {
					try {
						Utils.shutDownYARNContainer(tejobid);
					} catch (Exception ex) {
						log.error(DataSamudayaConstants.EMPTY, ex);
					}
					isyarncontainerlaunched.set(false);
				}
				if (!iscontainerlaunched.get()) {
					tejobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();

					List<LaunchContainers>  containers = Utils.launchContainersExecutorSpecWithDriverSpec(user, tejobid, cpupercontainer, memorypercontainer, numberofcontainers, cpudriver, memorydriver, true);
					Map<String, Object> cpumemory = Utils.getAllocatedContainersResources(containers);
					iscontainerlaunched.set(true);
					out.println("User '" + user + "' connected with cpu " + cpumemory.get(DataSamudayaConstants.CPUS) + " and memory " + cpumemory.get(DataSamudayaConstants.MEM) + " mb");
					Utils.printNodesAndContainers(containers, out);
					iscontainerlaunched.set(true);
				}
				out.println("jgroups mode set");
			} else if (mode[1].equalsIgnoreCase(DataSamudayaConstants.MODE_DEFAULT)) {
				isjgroups.set(false);
				isignite.set(true);
				isyarn.set(false);
				if (iscontainerlaunched.get()) {
					try {
						Utils.destroyContainers(user, tejobid);
					} catch (Exception ex) {
						log.error(DataSamudayaConstants.EMPTY, ex);
					}
					iscontainerlaunched.set(false);
				}
				if (isyarncontainerlaunched.get()) {
					try {
						Utils.shutDownYARNContainer(tejobid);
					} catch (Exception ex) {
						log.error(DataSamudayaConstants.EMPTY, ex);
					}
					isyarncontainerlaunched.set(false);
				}
				out.println("ignite mode set");
			} else if (mode[1].equalsIgnoreCase(DataSamudayaConstants.YARN)) {
				isjgroups.set(false);
				isignite.set(false);
				isyarn.set(true);
				if (iscontainerlaunched.get()) {
					try {
						Utils.destroyContainers(user, tejobid);
					} catch (Exception ex) {
						log.error(DataSamudayaConstants.EMPTY, ex);
					}
					iscontainerlaunched.set(false);
				}
				if (!isyarncontainerlaunched.get()) {
					try {
						tejobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
						Utils.launchYARNExecutors(tejobid, cpupercontainer, memorypercontainer, numberofcontainers, DataSamudayaConstants.SQL_YARN_DEFAULT_APP_CONTEXT_FILE, isdriverrequired.get());
					} catch (Exception ex) {
						log.error(DataSamudayaConstants.EMPTY, ex);
					}
					isyarncontainerlaunched .set(true);
				}
				out.println("yarn mode set");
			} else {
				isjgroups.set(false);
				isignite.set(false);
				isyarn.set(false);
				if (isyarncontainerlaunched.get()) {
					try {
						Utils.shutDownYARNContainer(tejobid);
					} catch (Exception ex) {
						log.error(DataSamudayaConstants.EMPTY, ex);
					}
					isyarncontainerlaunched.set(false);
				}
				if (!iscontainerlaunched.get()) {
					tejobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
					List<LaunchContainers> containers = Utils.launchContainersExecutorSpecWithDriverSpec(user, tejobid, cpupercontainer, memorypercontainer, numberofcontainers, cpudriver, memorydriver, true);
					Map<String, Object> cpumemory = Utils.getAllocatedContainersResources(containers);
					iscontainerlaunched.set(true);
					out.println("User '" + user + "' connected with cpu " + cpumemory.get(DataSamudayaConstants.CPUS) + " and memory " + cpumemory.get(DataSamudayaConstants.MEM) + " mb");
					Utils.printNodesAndContainers(containers, out);
					iscontainerlaunched.set(true);
				}
				out.println("jgroups, ignite and yarn mode unset");
			}
		} else {
			out.println("Invalid mode command.");
		}
	}
}
