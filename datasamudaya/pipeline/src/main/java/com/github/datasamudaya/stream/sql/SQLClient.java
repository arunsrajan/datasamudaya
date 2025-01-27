package com.github.datasamudaya.stream.sql;

import static java.util.Objects.nonNull;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;
import org.burningwave.core.assembler.StaticComponentContainer;
import org.jline.reader.EndOfFileException;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.UserInterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ai.chat.ChatResponse;
import org.springframework.ai.chat.messages.UserMessage;
import org.springframework.ai.chat.prompt.Prompt;
import org.springframework.ai.ollama.api.OllamaOptions;

import com.github.datasamudaya.common.ColumnMetadata;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.GlobalContainerLaunchers;
import com.github.datasamudaya.common.Job.JOBTYPE;
import com.github.datasamudaya.common.ServerUtils;
import com.github.datasamudaya.common.WebResourcesServlet;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.common.utils.ZookeeperOperations;

/**
 * This class is SQL client 
 * @author arun
 *
 */
public class SQLClient {
	
	static {
		System.setProperty("log4j.configurationFile", 
				System.getenv(DataSamudayaConstants.DATASAMUDAYA_HOME) + DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.LOG4J2_PROPERTIES);
	}
	
	private static final Logger log = LoggerFactory.getLogger(SQLClient.class);
	private static String currentsqlquery;
	private static String currentsqloutput;

	/**
	 * Main method which starts sql client in terminal.
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		String datasamudayahome = System.getenv(DataSamudayaConstants.DATASAMUDAYA_HOME);
		Utils.initializeProperties(datasamudayahome + DataSamudayaConstants.FORWARD_SLASH
				+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH, DataSamudayaConstants.DATASAMUDAYA_PROPERTIES);
		Utils.initializeOllamaChatClient();		
		var options = new Options();
		options.addOption(DataSamudayaConstants.CONF, true, DataSamudayaConstants.EMPTY);
		options.addOption(DataSamudayaConstants.USERSQL, true, DataSamudayaConstants.USERSQLREQUIRED);
		options.addOption(DataSamudayaConstants.SQLCONTAINERS, true, DataSamudayaConstants.EMPTY);
		options.addOption(DataSamudayaConstants.CPUPERCONTAINER, true, DataSamudayaConstants.EMPTY);
		options.addOption(DataSamudayaConstants.MEMORYPERCONTAINER, true, DataSamudayaConstants.EMPTY);
		options.addOption(DataSamudayaConstants.CPUDRIVER, true, DataSamudayaConstants.EMPTY);
		options.addOption(DataSamudayaConstants.MEMORYDRIVER, true, DataSamudayaConstants.EMPTY);
		options.addOption(DataSamudayaConstants.ISDRIVERREQUIRED, true, DataSamudayaConstants.EMPTY);
		options.addOption(DataSamudayaConstants.SQLWORKERMODE, true, DataSamudayaConstants.EMPTY);
		options.addOption(DataSamudayaConstants.DRIVER_LOCATION, true, DataSamudayaConstants.EMPTY);
		var parser = new DefaultParser();
		var cmd = parser.parse(options, args);
		String user;
		if (cmd.hasOption(DataSamudayaConstants.USERSQL)) {
			user = cmd.getOptionValue(DataSamudayaConstants.USERSQL);
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
			Utils.initializeProperties(
					datasamudayahome + DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH,
					DataSamudayaConstants.DATASAMUDAYA_PROPERTIES);
		}
		int numberofcontainers = 1;
		if (cmd.hasOption(DataSamudayaConstants.SQLCONTAINERS)) {
			String containers = cmd.getOptionValue(DataSamudayaConstants.SQLCONTAINERS);
			numberofcontainers = Integer.valueOf(containers);

		} else {
			numberofcontainers = Integer.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.NUMBEROFCONTAINERS));
		}

		if (numberofcontainers <= 0) {
			throw new SQLClientException("Number of containers cannot be less than 1");
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

		String mode = DataSamudayaConstants.SQLWORKERMODE_DEFAULT;
		if (cmd.hasOption(DataSamudayaConstants.SQLWORKERMODE)) {
			mode = cmd.getOptionValue(DataSamudayaConstants.SQLWORKERMODE);
		}

		String driverlocation = null;
		boolean isclient = false;
		ZookeeperOperations zo = null;
		boolean isyarn = mode.equalsIgnoreCase(DataSamudayaConstants.YARN);
		boolean isignite = mode.equalsIgnoreCase(DataSamudayaConstants.EXECMODE_IGNITE);
		if (isdriverrequired && (mode.equalsIgnoreCase(DataSamudayaConstants.SQLWORKERMODE_DEFAULT) || isyarn || isignite)) {
			if (cmd.hasOption(DataSamudayaConstants.DRIVER_LOCATION)) {
				driverlocation = cmd.getOptionValue(DataSamudayaConstants.DRIVER_LOCATION);
				isclient = driverlocation
						.equalsIgnoreCase(DataSamudayaConstants.DRIVER_LOCATION_CLIENT);
				if (isclient) {
					Utils.startHiveSession(user);
				}
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
		int portNumber = Integer
				.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.SQLPORT, DataSamudayaConstants.SQLPORT_DEFAULT));

		int timeout = Integer
				.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.SO_TIMEOUT, DataSamudayaConstants.SO_TIMEOUT_DEFAULT));
		String teid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
		while (true) {
			if ((isyarn || isignite) && isclient) {
				if (isyarn) {
					Utils.launchYARNExecutors(teid, cpupercontainer, memorypercontainer, numberofcontainers, DataSamudayaConstants.SQL_YARN_DEFAULT_APP_CONTEXT_FILE, false);
				}
				String messagestorefile = DataSamudayaProperties.get().getProperty(
						DataSamudayaConstants.SQLMESSAGESSTORE,
						DataSamudayaConstants.SQLMESSAGESSTORE_DEFAULT) + DataSamudayaConstants.UNDERSCORE
						+ user;
				processMessage(new PrintWriter(System.out, true),
						new BufferedReader(new InputStreamReader(System.in)), messagestorefile,
						isclient, teid, user, isyarn, isignite, memorypercontainer);
				break;
			} else {
				try (Socket sock = new Socket();) {
					sock.connect(new InetSocketAddress(hostName, portNumber), timeout);
					if (sock.isConnected()) {
						try (Socket socket = sock;
								OutputStream ostream = socket.getOutputStream();
								PrintWriter out = new PrintWriter(ostream, true);
								InputStream istream = socket.getInputStream();
								BufferedReader in = new BufferedReader(
										new InputStreamReader(istream));) {
							out.println(user);
							out.println(numberofcontainers);
							out.println(cpupercontainer);
							out.println(memorypercontainer);
							out.println(cpudriver);
							out.println(memorydriver);
							out.println(isdriverrequired);
							out.println(mode);
							out.println(teid);
							boolean toquit = printServerResponse(in);
							if (toquit) {
								break;
							}
							String messagestorefile = DataSamudayaProperties.get().getProperty(
									DataSamudayaConstants.SQLMESSAGESSTORE,
									DataSamudayaConstants.SQLMESSAGESSTORE_DEFAULT) + DataSamudayaConstants.UNDERSCORE
									+ user;
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
								processInput("quit", out);
								toquit = printServerResponse(in);
								if (toquit) {
									break;
								}
							} else {
								try {
									processMessage(out, in, messagestorefile, isclient, teid, user, isyarn, isignite, memorypercontainer);
								} catch (Exception ex) {
									log.debug("Aborting Connection");
									out.println("quit");
								}
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
	 * @throws Exception
	 */
	public static void processMessage(PrintWriter out, BufferedReader in, String messagestorefile, boolean isclient, String teid, String user, boolean isyarn, boolean isignite, long memorypercontainer) throws Exception {
		LineReader reader = Utils.getLineReaderTerminal(messagestorefile);
		String dbdefault = DataSamudayaProperties.get()
				.getProperty(DataSamudayaConstants.SQLDB, DataSamudayaConstants.SQLMETASTORE_DB);
		boolean ollamaenable = Boolean.parseBoolean(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.OLLAMA_ENABLE,
				DataSamudayaConstants.OLLAMA_ENABLE_DEFAULT));
		ServerUtils su = null;
		if (ollamaenable) {
			su = new ServerUtils();
			int ollamasqlport = Integer.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.OLLAMA_SQL_QUERY_PORT,
					DataSamudayaConstants.OLLAMA_SQL_QUERY_PORT_DEFAULT));
			su.init(ollamasqlport, new WebResourcesServlet(),
					DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.RESOURCES + DataSamudayaConstants.FORWARD_SLASH
							+ DataSamudayaConstants.ASTERIX,
					new WebResourcesServlet(), DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.FAVICON,
					new WebResourcesServlet(), DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.AIHTML,
					new SQLServlet(), DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.GENERATESQL,
					new SQLServlet(), DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.GENERATEMULTIPLESQL,
					new SQLServlet(), DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.GENERATEINSIGHTSSQL);
			su.start();
		}
		PrintWriter consoleout;
		if (ollamaenable && !isclient) {
			consoleout = new PrintWriter(System.out, true);
		} else {
			consoleout = out;
		}

		while (true) {
			String input = readLineWithHistory(reader);
			if (input.startsWith("ai")) {
				if (ollamaenable) {
					String[] args = input.split(" ");
					if (!"sql".equalsIgnoreCase(args[1]) && !"sqlmulti".equalsIgnoreCase(args[1])
							&& !"inference".equalsIgnoreCase(args[1])
							&& !"inferenceexec".equalsIgnoreCase(args[1])
							&& !"asciiarthistogram".equalsIgnoreCase(args[1])
							&& !"inferencequestion".equalsIgnoreCase(args[1])
							&& !"setmodel".equalsIgnoreCase(args[1])
							&& !"setinferencemodel".equalsIgnoreCase(args[1])) {
						consoleout.println();
						consoleout.println("Provide ai with parameter setmodel or setinferencemodel or sql or sqlmulti or inference or inferenceexec or asciiarthistogram or inferencequestion");
						continue;
					}
					if ("setmodel".equalsIgnoreCase(args[1])) {
						DataSamudayaProperties.get().put(DataSamudayaConstants.OLLAMA_MODEL_NAME,
								args[2]);
					} else if ("setinferencemodel".equalsIgnoreCase(args[1])) {
						DataSamudayaProperties.get().put(DataSamudayaConstants.OLLAMA_INFERENCE_MODEL_NAME,
								args[2]);
					} else if ("sql".equalsIgnoreCase(args[1])) {
						var columns = new ArrayList<ColumnMetadata>();
						TableCreator.getColumnMetadataFromTable(user, dbdefault, args[2], columns);
						List<String> columnsNames = columns.stream().map(ColumnMetadata::getColumnName).collect(Collectors.toList());
						String query = String.format(DataSamudayaConstants.SQL_QUERY_AGG_PROMPT, args[2], columnsNames.toString());
						consoleout.println();
						consoleout.println(query);
						ChatResponse response = Utils.ollamaChatClient.call(new Prompt(new UserMessage(query),
								OllamaOptions.create()
										.withTemperature(Float.parseFloat(DataSamudayaProperties.get().
												getProperty(DataSamudayaConstants.OLLAMA_MODEL_TEMPERATURE,
												DataSamudayaConstants.OLLAMA_MODEL_TEMPERATURE_DEFAULT)))
										.withModel(DataSamudayaProperties.get().
												getProperty(DataSamudayaConstants.OLLAMA_MODEL_NAME,
												DataSamudayaConstants.OLLAMA_MODEL_DEFAULT))));
						consoleout.println(response.getResult().getOutput().getContent());
					} else if ("sqlmulti".equalsIgnoreCase(args[1])) {
						if (!NumberUtils.isCreatable(args[2])) {
							consoleout.println();
							consoleout.println("The number of sql generated must be a number");
							continue;
						}
						var columns = new ArrayList<ColumnMetadata>();
						TableCreator.getColumnMetadataFromTable(user, dbdefault, args[3], columns);
						List<String> columnsNames = columns.stream().map(ColumnMetadata::getColumnName).collect(Collectors.toList());
						String query = String.format(DataSamudayaConstants.SQL_QUERY_MUL_AGG_PROMPT, args[2], args[3], columnsNames.toString());
						consoleout.println();
						consoleout.println(query);
						ChatResponse response = Utils.ollamaChatClient.call(new Prompt(new UserMessage(query),
								OllamaOptions.create()
										.withTemperature(Float.parseFloat(DataSamudayaProperties.get().
												getProperty(DataSamudayaConstants.OLLAMA_MODEL_TEMPERATURE,
												DataSamudayaConstants.OLLAMA_MODEL_TEMPERATURE_DEFAULT)))
										.withModel(DataSamudayaProperties.get().
												getProperty(DataSamudayaConstants.OLLAMA_MODEL_NAME,
												DataSamudayaConstants.OLLAMA_MODEL_DEFAULT))));
						consoleout.println(response.getResult().getOutput().getContent());
					} else if ("inference".equalsIgnoreCase(args[1])) {
						String query = String.format(DataSamudayaConstants.SQL_QUERY_INFERENCE_PROMPT, args[2], currentsqlquery);
						consoleout.println();
						consoleout.println(query);
						ChatResponse response = Utils.ollamaChatClient.call(new Prompt(new UserMessage(query),
								OllamaOptions.create()
										.withTemperature(Float.parseFloat(DataSamudayaProperties.get().
												getProperty(DataSamudayaConstants.OLLAMA_MODEL_TEMPERATURE,
												DataSamudayaConstants.OLLAMA_MODEL_TEMPERATURE_DEFAULT)))
										.withModel(DataSamudayaProperties.get().
												getProperty(DataSamudayaConstants.OLLAMA_INFERENCE_MODEL_NAME,
												DataSamudayaConstants.OLLAMA_INFERENCE_MODEL_NAME_DEFAULT))));
						consoleout.println(response.getResult().getOutput().getContent());
					} else if ("asciiarthistogram".equalsIgnoreCase(args[1])) {
						String query = String.format(DataSamudayaConstants.SQL_QUERY_ASCII_ART_HISTOGRAM_EXEC_PROMPT, currentsqlquery, currentsqloutput, args[2], args[3], args[4]);
						consoleout.println();
						consoleout.println(query);
						ChatResponse response = Utils.ollamaChatClient.call(new Prompt(new UserMessage(query),
								OllamaOptions.create()
										.withTemperature(Float.parseFloat(DataSamudayaProperties.get().
												getProperty(DataSamudayaConstants.OLLAMA_MODEL_TEMPERATURE,
												DataSamudayaConstants.OLLAMA_MODEL_TEMPERATURE_DEFAULT)))
										.withModel(DataSamudayaProperties.get().
												getProperty(DataSamudayaConstants.OLLAMA_MODEL_NAME,
												DataSamudayaConstants.OLLAMA_MODEL_DEFAULT))));
						consoleout.println(response.getResult().getOutput().getContent());
					} else if ("inferenceexec".equalsIgnoreCase(args[1])) {
						var sb = new StringBuffer();
						for (int count = 2;count < args.length;count++) {
							sb.append(args[count]).append(" ");
						}
						String query = String.format(DataSamudayaConstants.SQL_QUERY_INFERENCE_EXEC_PROMPT, sb.toString(), currentsqlquery, currentsqloutput);
						consoleout.println();
						consoleout.println(query);
						ChatResponse response = Utils.ollamaChatClient.call(new Prompt(new UserMessage(query),
								OllamaOptions.create()
										.withTemperature(Float.parseFloat(DataSamudayaProperties.get().
												getProperty(DataSamudayaConstants.OLLAMA_MODEL_TEMPERATURE,
												DataSamudayaConstants.OLLAMA_MODEL_TEMPERATURE_DEFAULT)))
										.withModel(DataSamudayaProperties.get().
												getProperty(DataSamudayaConstants.OLLAMA_INFERENCE_MODEL_NAME,
												DataSamudayaConstants.OLLAMA_INFERENCE_MODEL_NAME_DEFAULT))));
						consoleout.println(response.getResult().getOutput().getContent());
					} else if ("inferencequestion".equalsIgnoreCase(args[1])) {
						var inference = new StringBuffer();
						var columnnames = new StringBuffer();
						String tablename = args[2];
						var columns = new ArrayList<ColumnMetadata>();
						TableCreator.getColumnMetadataFromTable(user, dbdefault, tablename, columns);
						columns.stream().map(ColumnMetadata::getColumnName).forEach(colname -> columnnames.append(colname).append(", "));
						columnnames.deleteCharAt(columnnames.length() - 2);
						for (int count = 3;count < args.length;count++) {
							inference.append(args[count]).append(" ");
						}
						String query = String.format(DataSamudayaConstants.SQL_QUERY_PROMPT, inference.toString(), tablename, columnnames.toString());
						consoleout.println();
						consoleout.println(query);
						ChatResponse response = Utils.ollamaChatClient.call(new Prompt(new UserMessage(query),
								OllamaOptions.create()
										.withTemperature(Float.parseFloat(DataSamudayaProperties.get().
												getProperty(DataSamudayaConstants.OLLAMA_MODEL_TEMPERATURE,
												DataSamudayaConstants.OLLAMA_MODEL_TEMPERATURE_DEFAULT)))
										.withModel(DataSamudayaProperties.get().
												getProperty(DataSamudayaConstants.OLLAMA_INFERENCE_MODEL_NAME,
												DataSamudayaConstants.OLLAMA_INFERENCE_MODEL_NAME_DEFAULT))));
						consoleout.println(response.getResult().getOutput().getContent());
					}
					continue;
				}
				consoleout.println("Please enable Ollama AI in config file to use it");
				continue;
			}
			if (isclient) {
				if ("quit".equalsIgnoreCase(input.trim())) {
					break;
				}
				if (input.startsWith("use")) {
					dbdefault = StringUtils.normalizeSpace(input.trim()).split(" ")[1];
				} else if (input.startsWith("getdb")) {
					out.println(dbdefault);
				} else if (input.startsWith("create")
						|| input.startsWith("alter")) {
					out.println(TableCreator.createAlterTable(user, dbdefault, input));
				} else if (input.startsWith("drop")) {
					out.println(TableCreator.dropTable(user, dbdefault, input));
				} else if (input.startsWith("show")) {
					Utils.printTableOrError(TableCreator.showTables(user, dbdefault, input), out, JOBTYPE.NORMAL);
				} else if (input.startsWith("explain")) {
					SelectQueryExecutor.explain(user, dbdefault, input.replaceFirst("explain", ""), out);
				} else if (input.startsWith("describe")) {
					var columns = new ArrayList<ColumnMetadata>();
					TableCreator.getColumnMetadataFromTable(user, dbdefault, input.split(" ")[1], columns);
					for (ColumnMetadata colmetadata : columns) {
						out.println(colmetadata);
					}
				} else if (input.contains("select")) {
					out.println("\nProcessing input: " + input);
					out.flush();
					String jobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis()
							+ DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
					long starttime = System.currentTimeMillis();
					List<List> results;
					currentsqlquery = input;
					if (isignite) {
						results = SelectQueryExecutor.executeSelectQueryIgnite(dbdefault, input, user, jobid, teid, Long.valueOf((long) memorypercontainer) * DataSamudayaConstants.MB);
					} else {
						results = SelectQueryExecutor.executeSelectQuery(dbdefault, input, user, jobid, teid, false,
								isyarn, null, false);
					}
					double timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
					long totalrecords = 0;
					var buffer = new ByteArrayOutputStream();
					var oswriter = new OutputStreamWriter(buffer, StandardCharsets.UTF_8);
					var sqlwriter = new PrintWriter(oswriter, true);
					for (List result : results) {
						totalrecords += Utils.printTableOrError(result, out, JOBTYPE.NORMAL);
						Utils.printTableOrError(result, sqlwriter, JOBTYPE.NORMAL);
					}
					if (totalrecords > 0) {
						currentsqloutput = new String(buffer.toByteArray());
					}
					buffer.close();
					oswriter.close();
					sqlwriter.close();
					out.println("Total records " + totalrecords);
					out.println("Time taken " + timetaken + " seconds");
					out.println("");
					out.flush();
				}
			} else {
				processInput(input, out);
				boolean toquit = printServerResponse(in);
				if (toquit) {
					break;
				}
			}
			saveHistory(reader.getHistory());
		}
		if (ollamaenable && nonNull(su)) {
			su.stop();
			su.destroy();
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
				line = reader.readLine("SQL> ");
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
	 * @param input
	 * @param out
	 */
	private static void processInput(String input, PrintWriter out) {
		// Process the user's input here.
		System.out.println("\nProcessing input: " + input);
		out.println(input);
	}

	/**
	 * Save the messages to history file.
	 * @param messagestorefile
	 */
	private static void saveHistory(History history) {
		try {
			history.save();
		} catch (IOException e) {
			System.err.println("Error saving history: " + e.getMessage());
		}
	}

}
