package com.github.datasamudaya.common.utils;

import java.io.*;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.burningwave.core.assembler.StaticComponentContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;

import jline.console.ConsoleReader;

import static java.util.Objects.nonNull;

/**
 * JShell client console for the DataSamudaya.
 * @author arun
 *
 */
public class JShellClient {
	private static final Logger log = LoggerFactory.getLogger(JShellClient.class);

	public static void main(String[] args) throws Exception {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		Utils.initializeProperties(DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
				+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH, DataSamudayaConstants.DATASAMUDAYA_PROPERTIES);
		StaticComponentContainer.Modules.exportAllToAll();
		String hostName = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_HOST);
		// set the hostname of the sql server
		int portNumber = Integer
				.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.SHELLPORT, DataSamudayaConstants.SHELLPORT_DEFAULT));
		// set the port number of the sql server
		int timeout = Integer
				.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.SO_TIMEOUT, DataSamudayaConstants.SO_TIMEOUT_DEFAULT));
		while (true) {
			try (Socket sock = new Socket();) {
				sock.connect(new InetSocketAddress(hostName, portNumber), timeout);
				if (sock.isConnected()) {
					try (Socket socket = sock;
							PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
							BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));) {
						String messagestorefile = DataSamudayaProperties.get().getProperty(
								DataSamudayaConstants.SHELLMESSAGESSTORE, DataSamudayaConstants.SHELLMESSAGESSTORE_DEFAULT)
								+ DataSamudayaConstants.UNDERSCORE + args[0];
						printServerResponse(in);
						try {
							processMessage(out, in, messagestorefile);
						} catch (Exception ex) {
							log.error("Aborting Connection");
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
			log.debug("Socket Timeout Occurred for host {} and port {}, retrying...", hostName, portNumber);
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
	 * @throws Exception
	 */
	public static void processMessage(PrintWriter out, BufferedReader in, String messagestorefile) throws Exception {
		loadHistory(messagestorefile);
		BuffereredConsoleReader reader = new BuffereredConsoleReader();
		reader.setHandleUserInterrupt(true);
		reader.setPrompt("\nSHELL>");
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
			saveHistory(messagestorefile);
		}

		reader.close();
	}
	private static int historyIndex;
	private static final List<String> history = new ArrayList<>();

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
