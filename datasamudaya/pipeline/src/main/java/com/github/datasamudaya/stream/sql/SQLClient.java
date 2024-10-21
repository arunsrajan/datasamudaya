package com.github.datasamudaya.stream.sql;

import static java.util.Objects.nonNull;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.lang.reflect.Method;
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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.log4j.PropertyConfigurator;
import org.burningwave.core.assembler.StaticComponentContainer;
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
import com.github.datasamudaya.common.utils.UnixTerminal;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.common.utils.ZookeeperOperations;

import jline.TerminalFactory;
import jline.TerminalFactory.Flavor;
import jline.console.ConsoleReader;

/**
 * This class is SQL client 
 * @author arun
 *
 */
public class SQLClient {
	private static final Logger log = LoggerFactory.getLogger(SQLClient.class);
	private static final List<String> history = new ArrayList<>();
	private static int historyIndex;

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
		PropertyConfigurator.configure(datasamudayahome + DataSamudayaConstants.FORWARD_SLASH
				+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.LOG4J_PROPERTIES);
		TerminalFactory.registerFlavor(Flavor.UNIX, UnixTerminal.class);
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
		
		if(numberofcontainers <= 0) {
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
		} else if(!isdriverrequired){
			cpudriver = 0;
		}
		int memorydriver = 1024;
		if (cmd.hasOption(DataSamudayaConstants.MEMORYDRIVER) && isdriverrequired) {
			String memory = cmd.getOptionValue(DataSamudayaConstants.MEMORYDRIVER);
			memorydriver = Integer.valueOf(memory);
		} else if(!isdriverrequired){
			memorydriver = 0;
		}
		
		String mode = DataSamudayaConstants.SQLWORKERMODE_DEFAULT;
		if (cmd.hasOption(DataSamudayaConstants.SQLWORKERMODE)) {
			mode = cmd.getOptionValue(DataSamudayaConstants.SQLWORKERMODE);
		}
		
		String driverlocation = null;
		boolean isclient=false;
		ZookeeperOperations zo = null;
		boolean isyarn = mode.equalsIgnoreCase(DataSamudayaConstants.YARN);
		boolean isignite = mode.equalsIgnoreCase(DataSamudayaConstants.EXECMODE_IGNITE);
		if(isdriverrequired && (mode.equalsIgnoreCase(DataSamudayaConstants.SQLWORKERMODE_DEFAULT) || isyarn || isignite)) {
			if (cmd.hasOption(DataSamudayaConstants.DRIVER_LOCATION)) {
				driverlocation = cmd.getOptionValue(DataSamudayaConstants.DRIVER_LOCATION);
				isclient = driverlocation
				.equalsIgnoreCase(DataSamudayaConstants.DRIVER_LOCATION_CLIENT);
				if(isclient && !(isignite)) {
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
			if((isyarn || isignite) && isclient) {
				if(isyarn) {
					Utils.launchYARNExecutors(teid, cpupercontainer, memorypercontainer, numberofcontainers, DataSamudayaConstants.SQL_YARN_DEFAULT_APP_CONTEXT_FILE, isdriverrequired);
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
								boolean toquit = printServerResponse(in);
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
		if(isyarn && isclient) {
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
		loadHistory(messagestorefile);
		BuffereredConsoleReader reader = new BuffereredConsoleReader();
		reader.setHandleUserInterrupt(true);
		reader.setPrompt("\nSQL>");
		String dbdefault = DataSamudayaProperties.get()
				.getProperty(DataSamudayaConstants.SQLDB, DataSamudayaConstants.SQLMETASTORE_DB);
		boolean ollamaenable = Boolean.parseBoolean(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.OLLAMA_ENABLE, 
				DataSamudayaConstants.OLLAMA_ENABLE_DEFAULT));
		PrintWriter consoleout;
		if(ollamaenable && !isclient) {
			consoleout = new PrintWriter(System.out, true);
		} else {
			consoleout = out;
		}	
		
		while (true) {
			String input = readLineWithHistory(reader);
			if (input.startsWith(("ai"))) {
				if(ollamaenable) {
					String[] args = input.split(" ");
					if(!args[1].equalsIgnoreCase("sql") && !args[1].equalsIgnoreCase("sqlmulti")) {
						consoleout.println();
						consoleout.println("Provide options with parameter sql or sqlmulti");
						continue;
					}					
					if(args[1].equalsIgnoreCase("sql")) {
						var columns = new ArrayList<ColumnMetadata>();
						TableCreator.getColumnMetadataFromTable(dbdefault, args[2], columns);
						List<String> columnsNames = columns.stream().map(ColumnMetadata::getColumnName).collect(Collectors.toList());				
						String query = String.format(DataSamudayaConstants.SQL_QUERY_AGG_PROMPT, args[2],columnsNames.toString());
						consoleout.println();
						consoleout.println(query);
						ChatResponse response = Utils.ollamaChatClient.call(new Prompt(new UserMessage(query), 
								OllamaOptions.create()
								.withTemperature(Float.parseFloat(DataSamudayaProperties.get().
										getProperty(DataSamudayaConstants.OLLAMA_MODEL_TEMPERATURE,
												DataSamudayaConstants.OLLAMA_MODEL_TEMPERATURE_DEFAULT)))
								.withModel(DataSamudayaProperties.get().
										getProperty(DataSamudayaConstants.OLLAMA_MODEL_NAME,
												DataSamudayaConstants.OLLAMA_MODEL__DEFAULT))));
						consoleout.println(response.getResult().getOutput().getContent());
					}
					else if(args[1].equalsIgnoreCase("sqlmulti")) {
						if(!NumberUtils.isCreatable(args[2])) {
							consoleout.println();
							consoleout.println("The number of sql generated must be a number");
							continue;
						}
						var columns = new ArrayList<ColumnMetadata>();
						TableCreator.getColumnMetadataFromTable(dbdefault, args[3], columns);
						List<String> columnsNames = columns.stream().map(ColumnMetadata::getColumnName).collect(Collectors.toList());				
						String query = String.format(DataSamudayaConstants.SQL_QUERY_MUL_AGG_PROMPT, args[2],args[3],columnsNames.toString());
						consoleout.println();
						consoleout.println(query);
						ChatResponse response = Utils.ollamaChatClient.call(new Prompt(new UserMessage(query), 
								OllamaOptions.create()
								.withTemperature(Float.parseFloat(DataSamudayaProperties.get().
										getProperty(DataSamudayaConstants.OLLAMA_MODEL_TEMPERATURE,
												DataSamudayaConstants.OLLAMA_MODEL_TEMPERATURE_DEFAULT)))
								.withModel(DataSamudayaProperties.get().
										getProperty(DataSamudayaConstants.OLLAMA_MODEL_NAME,
												DataSamudayaConstants.OLLAMA_MODEL__DEFAULT))));
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
					out.println(TableCreator.createAlterTable(dbdefault, input));
				} else if (input.startsWith("drop")) {
					out.println(TableCreator.dropTable(dbdefault, input));
				} else if (input.startsWith("show")) {
					Utils.printTableOrError(TableCreator.showTables(dbdefault, input), out, JOBTYPE.NORMAL);
				} else if (input.startsWith("explain")) {
					SelectQueryExecutor.explain(dbdefault, input.replaceFirst("explain", ""), out);
				} else if (input.startsWith("describe")) {
					var columns = new ArrayList<ColumnMetadata>();
					TableCreator.getColumnMetadataFromTable(dbdefault, input.split(" ")[1], columns);
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
					if(isignite) {
						results = results = SelectQueryExecutor.executeSelectQueryIgnite(dbdefault, input, user, jobid, teid, Long.valueOf((long)memorypercontainer) * DataSamudayaConstants.MB);
					} else {
						results = SelectQueryExecutor.executeSelectQuery(dbdefault, input, user, jobid, teid, false,
							isyarn, out, false);
					}
					double timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
					long totalrecords = 0;
					for (List result : results) {
						totalrecords += Utils.printTableOrError(result, out, JOBTYPE.NORMAL);
					}
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
			saveHistory(messagestorefile);
		}

		reader.close();
	}

	/**
	 * Histroy stored in file will be loaded and when keys are pressed will
	 * be displayed to the user.
	 * @param reader
	 * @return messages like sql query from history or user typed text.
	 * @throws Exception
	 */
	private static String readLineWithHistory(BuffereredConsoleReader reader) throws Exception {
		String line = "";
		boolean lineRead = false;
		while (!lineRead) {
			StringBuilder sb = new StringBuilder();
			int key;
			System.out.print("\nSQL>");
			reader.setCursorPosition(0);
			reader.setConsoleBuffer(DataSamudayaConstants.EMPTY);
			reader.drawLine();
			reader.flush();
			while ((key = reader.readCharacter()) != '\r') {
				if (key == 27) { // Escape sequence
					key = reader.readCharacter();
					if (key == 91) { // Arrow key sequence
						key = reader.readCharacter();
						if (key == 65) { // Up arrow key
							if (historyIndex > 0) {
								historyIndex--;
								line = history.get(historyIndex);
								reader.setCursorPosition(0);
								reader.killLine();
								reader.setConsoleBuffer(line);
								reader.flush();
							}
						} else if (key == 66) { // Down arrow key
							if (historyIndex < history.size() - 1) {
								historyIndex++;
								line = history.get(historyIndex);
								reader.setCursorPosition(0);
								reader.killLine();
								reader.setConsoleBuffer(line);
								reader.flush();
							} else {
								historyIndex = history.size();
								line = "";
								reader.setCursorPosition(0);
								reader.killLine();
								reader.setConsoleBuffer(line);
								reader.flush();
							}
						} else if (key == 68 || key == 67) {
							if (key == 68) {
								int curPos = reader.getCursorBuffer().cursor;
								if (curPos > 0) {
									reader.setCursorPosition(curPos - 1);
									reader.flush();
								}
							} else if (key == 67) {
								int curPos = reader.getCursorBuffer().cursor;
								int bufferLen = reader.getCursorBuffer().buffer.length();
								if (curPos < bufferLen) {
									reader.setCursorPosition(curPos + 1);
									reader.flush();
								}
							}
						} else if (key == 49) {
							reader.setCursorPosition(0);
							reader.backspace();
							reader.flush();
						} else if (key == 52) {
							reader.setCursorPosition(reader.getCursorBuffer().length());
							reader.flush();
						} else if (key == 51) {
							int curPos = reader.getCursorBuffer().cursor;
							if (curPos >= 0 && curPos < reader.getCursorBuffer().length()) {
								reader.setCursorPosition(curPos + 1);
								reader.backspace();
								reader.flush();
								if (!sb.isEmpty() && curPos < sb.length()) {
									sb.deleteCharAt(curPos);
								}
							}
						} else {
							historyIndex = history.size();
							sb.append((char) key);
							reader.setConsoleBuffer(sb.toString());
							reader.flush();
						}
					} else {
						historyIndex = history.size();
						sb.append((char) 27);
						sb.append((char) key);
						reader.setConsoleBuffer(sb.toString());
						reader.flush();
					}
				} else if (key == 127 || key == 8) { // Backspace
					int curPos = reader.getCursorBuffer().cursor;
					if (curPos > 0) {
						reader.backspace();
						reader.setCursorPosition(curPos - 1);
						reader.flush();
						if (!sb.isEmpty() && sb.length() < curPos) {
							sb.deleteCharAt(curPos);
						}
					}
				} else if (key != 126) {
					historyIndex = history.size();
					sb.delete(0, sb.length());
					sb.append(reader.getCursorBuffer().toString());
					int curPos = reader.getCursorBuffer().cursor;
					sb.insert(curPos, (char) key);
					reader.setConsoleBuffer(sb.toString());
					reader.setCursorPosition(curPos + 1);
					reader.flush();

				}
			}
			line = sb.toString();
			if (!line.isEmpty()) {
				history.add(line);
			} else {
				history.add(reader.getCursorBuffer().toString());
				line = reader.getCursorBuffer().toString();
			}
			historyIndex = history.size();
			lineRead = true;
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
	 * The history from the files will be loaded.
	 * @param messagestorefile
	 */
	private static void loadHistory(String messagestorefile) {
		try (BufferedReader reader = new BufferedReader(new FileReader(messagestorefile))) {
			String line;
			while ((line = reader.readLine()) != null) {
				history.add(line);
			}
			historyIndex = history.size();
		} catch (IOException e) {
			System.err.println("Error loading history: " + e.getMessage());
		}
	}

	/**
	 * Save the messages to history file.
	 * @param messagestorefile
	 */
	private static void saveHistory(String messagestorefile) {
		try (PrintWriter writer = new PrintWriter(new FileWriter(messagestorefile))) {
			for (String line : history) {
				writer.println(line);
			}
		} catch (IOException e) {
			System.err.println("Error saving history: " + e.getMessage());
		}
	}

	/**
	 * This method is console reader with custom setConsoleBuffer method.
	 * @author arun
	 *
	 */
	private static class BuffereredConsoleReader extends ConsoleReader {

		public BuffereredConsoleReader() throws IOException {
			super();
		}

		public void setConsoleBuffer(String buffer) throws IOException {
			try {
				Method setBuffer = ConsoleReader.class.getDeclaredMethod("setBuffer", String.class);
				setBuffer.setAccessible(true);
				setBuffer.invoke(this, buffer);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
}
