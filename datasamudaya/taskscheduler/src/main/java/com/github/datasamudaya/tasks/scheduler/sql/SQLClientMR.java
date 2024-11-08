package com.github.datasamudaya.tasks.scheduler.sql;

import static java.util.Objects.nonNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetSocketAddress;
import java.net.Socket;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.log4j.PropertyConfigurator;
import org.burningwave.core.assembler.StaticComponentContainer;
import org.jline.reader.EndOfFileException;
import org.jline.reader.History;
import org.jline.reader.LineReader;
import org.jline.reader.UserInterruptException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.utils.Utils;

/**
 * This class is SQL client
 * 
 * @author arun
 *
 */
public class SQLClientMR {
	private static final Logger log = LoggerFactory.getLogger(SQLClientMR.class);

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
		options.addOption(DataSamudayaConstants.USERSQL, true, DataSamudayaConstants.USERSQLREQUIRED);
		options.addOption(DataSamudayaConstants.SQLCONTAINERS, true, DataSamudayaConstants.EMPTY);
		options.addOption(DataSamudayaConstants.CPUPERCONTAINER, true, DataSamudayaConstants.EMPTY);
		options.addOption(DataSamudayaConstants.MEMORYPERCONTAINER, true, DataSamudayaConstants.EMPTY);
		options.addOption(DataSamudayaConstants.SQLWORKERMODE, true, DataSamudayaConstants.EMPTY);
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
			Utils.initializeProperties(datasamudayahome + DataSamudayaConstants.FORWARD_SLASH
					+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH,
					DataSamudayaConstants.DATASAMUDAYA_PROPERTIES);
		}
		int numberofcontainers = 1;
		if (cmd.hasOption(DataSamudayaConstants.SQLCONTAINERS)) {
			String containers = cmd.getOptionValue(DataSamudayaConstants.SQLCONTAINERS);
			numberofcontainers = Integer.valueOf(containers);

		} else {
			numberofcontainers = Integer.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.NUMBEROFCONTAINERS));
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
		String mode = DataSamudayaConstants.SQLWORKERMODE_DEFAULT;
		if (cmd.hasOption(DataSamudayaConstants.SQLWORKERMODE)) {
			mode = cmd.getOptionValue(DataSamudayaConstants.SQLWORKERMODE);
		}
		StaticComponentContainer.Modules.exportAllToAll();
		// get the hostname of the sql server
		String hostName = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULER_HOST);
		// get the port number of the sql server
		int portNumber = Integer.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.SQLPORTMR,
				DataSamudayaConstants.SQLPORTMR_DEFAULT));

		int timeout = Integer.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.SO_TIMEOUT,
				DataSamudayaConstants.SO_TIMEOUT_DEFAULT));

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
						out.println(mode);
						printServerResponse(in);
						String messagestorefile = DataSamudayaProperties.get().getProperty(
								DataSamudayaConstants.SQLMESSAGESSTORE, DataSamudayaConstants.SQLMESSAGESSTORE_DEFAULT)
								+ DataSamudayaConstants.UNDERSCORE + user;
						try {
							processMessage(out, in, messagestorefile);
						} catch (Exception ex) {
							log.debug("Aborting Connection");
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
	 * 
	 * @param out
	 * @param in
	 * @param messagestorefile
	 * @throws Exception
	 */
	public static void processMessage(PrintWriter out, BufferedReader in, String messagestorefile) throws Exception {
		LineReader reader = Utils.getLineReaderTerminal(messagestorefile);
		while (true) {
			String input = readLineWithHistory(reader);
			if ("Quit".equals(input)) {
				break;
			}
			processInput(input, out);
			boolean toquit = printServerResponse(in);
			if (toquit) {
				break;
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
				line =  reader.readLine("SQL> ");
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
