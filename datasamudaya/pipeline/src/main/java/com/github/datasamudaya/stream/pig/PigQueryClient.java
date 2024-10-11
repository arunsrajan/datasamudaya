package com.github.datasamudaya.stream.pig;

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
import org.apache.log4j.PropertyConfigurator;
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.parser.QueryParserDriver;
import org.burningwave.core.assembler.StaticComponentContainer;
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

import jline.console.ConsoleReader;

/**
 * This class is PIG client
 * 
 * @author arun
 *
 */
public class PigQueryClient {
	private static final Logger log = LoggerFactory.getLogger(PigQueryClient.class);
	private static final List<String> history = new ArrayList<>();
	private static int historyIndex;

	/**
	 * Main method which starts sql client in terminal.
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		String datasamudayahome = System.getenv(DataSamudayaConstants.DATASAMUDAYA_HOME);
		PropertyConfigurator.configure(datasamudayahome + DataSamudayaConstants.FORWARD_SLASH
				+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.LOG4J_PROPERTIES);
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
		
		if(numberofcontainers <= 0) {
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
		
		String mode = DataSamudayaConstants.PIGWORKERMODE_DEFAULT;
		if (cmd.hasOption(DataSamudayaConstants.PIGWORKERMODE)) {
			mode = cmd.getOptionValue(DataSamudayaConstants.PIGWORKERMODE);
		}
		
		String driverlocation = null;
		boolean isclient=false;
		ZookeeperOperations zo = null;
		if(isdriverrequired && mode.equalsIgnoreCase(DataSamudayaConstants.PIGWORKERMODE_DEFAULT)) {
			if (cmd.hasOption(DataSamudayaConstants.DRIVER_LOCATION)) {
				driverlocation = cmd.getOptionValue(DataSamudayaConstants.DRIVER_LOCATION);
				isclient = driverlocation
				.equalsIgnoreCase(DataSamudayaConstants.DRIVER_LOCATION_CLIENT);
				if(isclient) {
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
			try (Socket sock = new Socket();) {
				sock.connect(new InetSocketAddress(hostName, portNumber), timeout);
				if (sock.isConnected()) {
					try (Socket socket = sock;
							PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
							BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));) {
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
								DataSamudayaConstants.PIGMESSAGESSTORE, DataSamudayaConstants.PIGMESSAGESSTORE_DEFAULT)
								+ DataSamudayaConstants.UNDERSCORE + user;
						try {
							if(isclient) {
								var taskexecutors = zo.getTaskExectorsByJobId(teid);
								Map<String, Set<String>> lcs = (Map) taskexecutors.stream().collect(
										Collectors.groupingBy(executor -> executor.split(DataSamudayaConstants.UNDERSCORE)[0], Collectors
												.mapping(executor -> executor, Collectors.toCollection(LinkedHashSet::new))));
								GlobalContainerLaunchers.put(user, teid, Utils.getLcs(lcs, teid, cpupercontainer));
								processMessage(new PrintWriter(System.out), 
										new BufferedReader(new InputStreamReader(System.in)), messagestorefile, isclient, teid, user);
							} else {
								processMessage(out, in, messagestorefile, isclient, teid, user);
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
			} finally {
				if(nonNull(zo)) {
					zo.close();
				}
			}
			log.debug("Socket Timeout Occurred for host {} and port, retrying...", hostName, portNumber);
			Thread.sleep(2000);
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
	public static void processMessage(PrintWriter out, BufferedReader in, String messagestorefile, boolean isclient, String teid, String user) throws Exception {
		loadHistory(messagestorefile);
		BuffereredConsoleReader reader = new BuffereredConsoleReader();
		reader.setHandleUserInterrupt(true);
		reader.setPrompt("\nPIG>");		
		List<String> pigQueriesToExecute = new ArrayList<>();
		List<String> pigQueries = new ArrayList<>();
		PipelineConfig pipelineconfig = new PipelineConfig();
		pipelineconfig.setLocal("false");
		pipelineconfig.setYarn("false");
		pipelineconfig.setMesos("false");
		pipelineconfig.setJgroups("false");
		pipelineconfig.setMode(DataSamudayaConstants.MODE_NORMAL);
		pipelineconfig.setStorage(STORAGE.COLUMNARSQL);
		pipelineconfig.setIsremotescheduler(false);
		pipelineconfig.setPigoutput(new Output(System.out));
		pipelineconfig.setWriter(out);
		pipelineconfig.setJobname(DataSamudayaConstants.PIG);
		QueryParserDriver queryParserDriver = PigUtils.getQueryParserDriver("pig");
		while (true) {
			String inputLine = readLineWithHistory(reader);
			if ("quit".equalsIgnoreCase(inputLine.trim())) {
				break;
			}
			if (isclient) {
				System.out.println("\nProcessing input: " + inputLine);
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
			saveHistory(messagestorefile);
		}

		reader.close();
	}

	/**
	 * Histroy stored in file will be loaded and when keys are pressed will be
	 * displayed to the user.
	 * 
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
			System.out.print("\nPIG>");
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
	 * The history from the files will be loaded.
	 * 
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
	 * 
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
	 * 
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
