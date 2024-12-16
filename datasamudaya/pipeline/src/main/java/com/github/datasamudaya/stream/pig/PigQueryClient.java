package com.github.datasamudaya.stream.pig;

import static java.util.Objects.nonNull;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.parser.QueryParserDriver;
import org.burningwave.core.assembler.StaticComponentContainer;
import org.jline.reader.EndOfFileException;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.UserInterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaConstants.STORAGE;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.GlobalContainerLaunchers;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.utils.DataSamudayaMetricsExporter;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.common.utils.ZookeeperOperations;

/**
 * This class is PIG client
 * 
 * @author arun
 *
 */
public class PigQueryClient {
	
	static {
		System.setProperty("log4j.configurationFile", 
				System.getenv(DataSamudayaConstants.DATASAMUDAYA_HOME) + DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.LOG4J2_PROPERTIES);
	}
	
	private static final Logger log = LoggerFactory.getLogger(PigQueryClient.class);

	/**
	 * Main method which starts sql client in terminal.
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		String datasamudayahome = System.getenv(DataSamudayaConstants.DATASAMUDAYA_HOME);
		var options = new Options();
		options.addOption(DataSamudayaConstants.CONF, true, DataSamudayaConstants.EMPTY);
		options.addOption(DataSamudayaConstants.USERPIG, true, DataSamudayaConstants.USERPIGREQUIRED);
		options.addOption(DataSamudayaConstants.PIGCONTAINERS, true, DataSamudayaConstants.EMPTY);
		options.addOption(DataSamudayaConstants.CPUPERCONTAINER, true, DataSamudayaConstants.EMPTY);
		options.addOption(DataSamudayaConstants.MEMORYPERCONTAINER, true, DataSamudayaConstants.EMPTY);
		options.addOption(DataSamudayaConstants.CPUDRIVER, true, DataSamudayaConstants.EMPTY);
		options.addOption(DataSamudayaConstants.MEMORYDRIVER, true, DataSamudayaConstants.EMPTY);
		options.addOption(DataSamudayaConstants.ISDRIVERREQUIRED, true, DataSamudayaConstants.EMPTY);
		options.addOption(DataSamudayaConstants.PIGWORKERMODE, true, DataSamudayaConstants.EMPTY);
		options.addOption(DataSamudayaConstants.DRIVER_LOCATION, true, DataSamudayaConstants.EMPTY);
		var parser = new DefaultParser();
		var cmd = parser.parse(options, args);
		String user;
		if (cmd.hasOption(DataSamudayaConstants.USERPIG)) {
			user = cmd.getOptionValue(DataSamudayaConstants.USERPIG);
		} else {
			var formatter = new HelpFormatter();
			formatter.printHelp(DataSamudayaConstants.ANTFORMATTER, options);
			return;
		}
		String config = null;
		if (cmd.hasOption(DataSamudayaConstants.CONF)) {
			config = cmd.getOptionValue(DataSamudayaConstants.CONF);
			Utils.initializeProperties(DataSamudayaConstants.EMPTY, config);
		} else {
			Utils.initializeProperties(datasamudayahome + DataSamudayaConstants.FORWARD_SLASH
					+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH,
					DataSamudayaConstants.DATASAMUDAYA_PROPERTIES);
		}
		int numberofcontainers = 1;
		if (cmd.hasOption(DataSamudayaConstants.PIGCONTAINERS)) {
			String containers = cmd.getOptionValue(DataSamudayaConstants.PIGCONTAINERS);
			numberofcontainers = Integer.valueOf(containers);

		} else {
			numberofcontainers = Integer
					.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.NUMBEROFCONTAINERS));
		}

		if (numberofcontainers <= 0) {
			throw new PigClientException("Number of containers cannot be less than 1");
		}

		boolean isdriverrequired = Boolean.parseBoolean(DataSamudayaProperties.get().getProperty(
				DataSamudayaConstants.IS_REMOTE_SCHEDULER, DataSamudayaConstants.IS_REMOTE_SCHEDULER_DEFAULT));

		if (cmd.hasOption(DataSamudayaConstants.ISDRIVERREQUIRED)) {
			String driverrequired = cmd.getOptionValue(DataSamudayaConstants.ISDRIVERREQUIRED);
			isdriverrequired = Boolean.parseBoolean(driverrequired);
		}

		int cpupercontainer = 1;
		if (cmd.hasOption(DataSamudayaConstants.CPUPERCONTAINER)) {
			String cpu = cmd.getOptionValue(DataSamudayaConstants.CPUPERCONTAINER);
			cpupercontainer = Integer.valueOf(cpu);

		}
		int memorypercontainer = 1024;
		if (cmd.hasOption(DataSamudayaConstants.MEMORYPERCONTAINER)) {
			String memory = cmd.getOptionValue(DataSamudayaConstants.MEMORYPERCONTAINER);
			memorypercontainer = Integer.valueOf(memory);

		}

		int cpudriver = 1;
		if (cmd.hasOption(DataSamudayaConstants.CPUDRIVER) && isdriverrequired) {
			String cpu = cmd.getOptionValue(DataSamudayaConstants.CPUDRIVER);
			cpudriver = Integer.valueOf(cpu);
		} else if (!isdriverrequired) {
			cpudriver = 0;
		}
		int memorydriver = 1024;
		if (cmd.hasOption(DataSamudayaConstants.MEMORYDRIVER) && isdriverrequired) {
			String memory = cmd.getOptionValue(DataSamudayaConstants.MEMORYDRIVER);
			memorydriver = Integer.valueOf(memory);
		} else if (!isdriverrequired) {
			memorydriver = 0;
		}

		String mode = DataSamudayaConstants.PIGWORKERMODE_DEFAULT;
		if (cmd.hasOption(DataSamudayaConstants.PIGWORKERMODE)) {
			mode = cmd.getOptionValue(DataSamudayaConstants.PIGWORKERMODE);
		}

		String driverlocation = null;
		boolean isclient = false;
		ZookeeperOperations zo = null;
		boolean isyarn = mode.equalsIgnoreCase(DataSamudayaConstants.YARN);
		boolean isignite = mode.equalsIgnoreCase(DataSamudayaConstants.EXECMODE_IGNITE);
		if (isdriverrequired && (mode.equalsIgnoreCase(DataSamudayaConstants.PIGWORKERMODE_DEFAULT) || isyarn || isignite)) {
			if (cmd.hasOption(DataSamudayaConstants.DRIVER_LOCATION)) {
				driverlocation = cmd.getOptionValue(DataSamudayaConstants.DRIVER_LOCATION);
				isclient = driverlocation
						.equalsIgnoreCase(DataSamudayaConstants.DRIVER_LOCATION_CLIENT);
				if (isclient && !isignite) {
					cpudriver = 0;
					memorydriver = 0;
					zo = new ZookeeperOperations();
					zo.connect();
					zo.createSchedulersLeaderNode(DataSamudayaConstants.EMPTY.getBytes(), event -> {
						log.debug("Node Created");
					});
					zo.watchNodes();
				}
			} else {
				driverlocation = DataSamudayaConstants.DRIVER_LOCATION_DEFAULT;
			}
		}

		StaticComponentContainer.Modules.exportAllToAll();
		// get the hostname of the sql server
		String hostName = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_HOST);
		// get the port number of the sql server
		int portNumber = Integer.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.PIGPORT,
				DataSamudayaConstants.PIGPORT_DEFAULT));

		int timeout = Integer.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.SO_TIMEOUT,
				DataSamudayaConstants.SO_TIMEOUT_DEFAULT));
		String teid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		while (true) {
			if ((isyarn || isignite) && isclient) {
				if (isyarn) {
					Utils.launchYARNExecutors(teid, cpupercontainer, memorypercontainer, numberofcontainers, DataSamudayaConstants.SQL_YARN_DEFAULT_APP_CONTEXT_FILE, isdriverrequired);
				}
				String messagestorefile = DataSamudayaProperties.get().getProperty(
						DataSamudayaConstants.PIGMESSAGESSTORE,
						DataSamudayaConstants.PIGMESSAGESSTORE_DEFAULT) + DataSamudayaConstants.UNDERSCORE
						+ user;
				processMessage(new PrintWriter(System.out, true),
						new BufferedReader(new InputStreamReader(System.in)), messagestorefile, isclient, teid, user, isyarn, isignite, memorypercontainer);
				break;
			} else {
				try (Socket sock = new Socket();) {
					sock.connect(new InetSocketAddress(hostName, portNumber), timeout);
					if (sock.isConnected()) {
						try (Socket socket = sock;
								PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
								BufferedReader in = new BufferedReader(
										new InputStreamReader(socket.getInputStream()));) {
							out.println(user);
							out.println(numberofcontainers);
							out.println(cpupercontainer);
							out.println(memorypercontainer);
							out.println(cpudriver);
							out.println(memorydriver);
							out.println(isdriverrequired);
							out.println(mode);
							out.println(teid);
							printServerResponse(in);
							String messagestorefile = DataSamudayaProperties.get().getProperty(
									DataSamudayaConstants.PIGMESSAGESSTORE,
									DataSamudayaConstants.PIGMESSAGESSTORE_DEFAULT) + DataSamudayaConstants.UNDERSCORE
									+ user;
							try {
								if (isclient) {
									var taskexecutors = zo.getTaskExectorsByJobId(teid);
									Map<String, Set<String>> lcs = (Map) taskexecutors.stream()
											.collect(Collectors.groupingBy(
													executor -> executor.split(DataSamudayaConstants.UNDERSCORE)[0],
													Collectors.mapping(executor -> executor,
															Collectors.toCollection(LinkedHashSet::new))));
									GlobalContainerLaunchers.put(user, teid, Utils.getLcs(lcs, teid, cpupercontainer));
									processMessage(new PrintWriter(System.out, true),
											new BufferedReader(new InputStreamReader(System.in)), messagestorefile,
											isclient, teid, user, isyarn, isignite, memorypercontainer);
								} else {
									processMessage(out, in, messagestorefile, isclient, teid, user, isyarn, isignite, memorypercontainer);
								}
							} catch (Exception ex) {
								log.error("Aborting Connection", ex);
							} finally {
								out.println("quit");
							}
							break;
						} catch (Exception ex) {
							log.error(DataSamudayaConstants.EMPTY, ex);
						}
					}
				} catch (Throwable ex) {
					log.error(DataSamudayaConstants.EMPTY, ex);
				}
				log.debug("Socket Timeout Occurred for host {} and port, retrying...", hostName, portNumber);
				Thread.sleep(2000);
			}
		}
		if (isyarn && isclient) {
			try {
				Utils.shutDownYARNContainer(teid);
			} catch (Exception ex) {
				log.error(DataSamudayaConstants.EMPTY, ex);
			}
		}
		if (nonNull(zo)) {
			zo.close();
		}
	}

	public static boolean printServerResponse(BufferedReader in) throws Exception {
		String serverresponse;
		while (nonNull(serverresponse = in.readLine())) {
			if ("Done".equals(serverresponse)) {
				break;
			} else if ("Quit".equals(serverresponse)) {
				return true;
			}
			System.out.println(serverresponse);
		}
		return false;
	}

	/**
	 * Processes the message from client to server and back to client.
	 * @param out
	 * @param in
	 * @param messagestorefile
	 * @param isclient
	 * @param teid
	 * @param user
	 * @throws Exception
	 */
	public static void processMessage(PrintWriter out, BufferedReader in, String messagestorefile, boolean isclient, String teid,
			String user, boolean isyarn, boolean isignite, long memorypercontainer) throws Exception {
		LineReader reader = Utils.getLineReaderTerminal(messagestorefile);
		List<String> pigQueriesToExecute = new ArrayList<>();
		List<String> pigQueries = new ArrayList<>();
		PipelineConfig pipelineconfig = new PipelineConfig();
		pipelineconfig.setLocal("false");
		pipelineconfig.setYarn("false");
		if (isyarn) {
			pipelineconfig.setYarn("true");
		}
		pipelineconfig.setMesos("false");
		pipelineconfig.setJgroups("false");
		if (isignite) {
			pipelineconfig.setMode(DataSamudayaConstants.MODE_DEFAULT);
			String memstr = String.valueOf(Long.valueOf((long) memorypercontainer) * DataSamudayaConstants.MB);
			pipelineconfig.setMinmem(memstr);
			pipelineconfig.setMaxmem(memstr);
		} else {
			pipelineconfig.setMode(DataSamudayaConstants.MODE_NORMAL);
		}
		pipelineconfig.setStorage(STORAGE.COLUMNARSQL);
		pipelineconfig.setIsremotescheduler(false);
		pipelineconfig.setPigoutput(new Output(System.out));
		pipelineconfig.setWriter(out);
		pipelineconfig.setJobname(DataSamudayaConstants.PIG);
		QueryParserDriver queryParserDriver = PigUtils.getQueryParserDriver("pig");
		while (true) {
			String inputLine = readLineWithHistory(reader);
			if (isclient) {
				if ("quit".equalsIgnoreCase(inputLine.trim())) {
					break;
				}
				out.println("\nProcessing input: " + inputLine);
				if (inputLine.startsWith("dump") || inputLine.startsWith("DUMP")) {
					if (nonNull(pipelineconfig)) {
						pipelineconfig.setSqlpigquery(inputLine);
					}
					inputLine = inputLine.replace(";", "");
					String[] dumpwithalias = inputLine.split(" ");
					long starttime = System.currentTimeMillis();
					String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
					pigQueriesToExecute.clear();
					pigQueriesToExecute.addAll(pigQueries);
					pigQueriesToExecute.add("\n");
					DataSamudayaMetricsExporter.getNumberOfPigQueriesDumpExecutedCounter().inc();
					LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
					PigQueryExecutor.executePlan(lp, false, dumpwithalias[1].trim(), user, jobid, teid, pipelineconfig);
					double timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
					out.println("Time taken " + timetaken + " seconds");
					out.println("");
				} else {
					long starttime = System.currentTimeMillis();
					if (nonNull(pipelineconfig)) {
						pipelineconfig.setSqlpigquery(inputLine);
					}
					pigQueriesToExecute.clear();
					pigQueriesToExecute.addAll(pigQueries);
					pigQueriesToExecute.add(inputLine);
					pigQueriesToExecute.add("\n");
					LogicalPlan lp = PigUtils.getLogicalPlan(pigQueriesToExecute, queryParserDriver);
					if (nonNull(lp)) {
						DataSamudayaMetricsExporter.getNumberOfPigQueriesCounter().inc();
						if (inputLine.startsWith("store") || inputLine.startsWith("STORE")) {
							String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
							PigQueryExecutor.executePlan(lp, true, DataSamudayaConstants.EMPTY, user, jobid, teid, pipelineconfig);
						}
						pigQueries.add(inputLine);
						pigQueries.add("\n");
					} else {
						out.println(String.format("Error In Pig Query for the current line: %s ", inputLine));
					}
					double timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
					out.println("Time taken " + timetaken + " seconds");
					out.println("");
				}
			} else {
				processInput(inputLine, out);
				boolean toquit = printServerResponse(in);
				if (toquit) {
					break;
				}
			}
			saveHistory(reader.getHistory());
		}

	}

	/**
	 * Reads line from user.
	 * @param reader
	 * @return line
	 * @throws Exception
	 */
	private static String readLineWithHistory(LineReader reader) throws Exception {
		String line = "";
		boolean lineRead = false;
		while (!lineRead) {
			try {
				line = reader.readLine("PIG> ");
				lineRead = true;
			}
			catch (UserInterruptException e) {
			}
			catch (EndOfFileException e) {
				break;
			}
		}
		return line;
	}

	/**
	 * Input sent to server.
	 * 
	 * @param input
	 * @param out
	 */
	private static void processInput(String input, PrintWriter out) {
		// Process the user's input here.
		System.out.println("\nProcessing input: " + input);
		out.println(input);
	}

	/**
	 * saves the history
	 * @param history
	 */
	private static void saveHistory(History history) {
		try {
			history.save();
		} catch (IOException e) {
			System.err.println("Error saving history: " + e.getMessage());
		}
	}
}
