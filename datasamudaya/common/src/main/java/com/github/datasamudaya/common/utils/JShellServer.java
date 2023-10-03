package com.github.datasamudaya.common.utils;

import static java.util.Objects.nonNull;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;

import jdk.jshell.JShell;
import jdk.jshell.Snippet;
import jdk.jshell.SnippetEvent;

/**
 * JShell server for executing client statements.
 * @author arun
 *
 */
public class JShellServer {

	static JShell jshell;
	static Logger log = LoggerFactory.getLogger(JShellServer.class);

	/**
	 * This method Starts the jshell server.
	 * @throws Exception
	 * @throws IOException
	 */
	public static void startJShell() throws Exception, IOException {
		ExecutorService executors = Executors.newFixedThreadPool(10);
		Integer port = Integer
				.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.SHELLPORT, DataSamudayaConstants.SHELLPORT_DEFAULT));
		System.out.println("DataSamudaya JShell started on port " + port);
		ServerSocket serverSocket = new ServerSocket(port);
		executors.execute(() -> {
			while (true) {
				Socket sock;
				try {
					sock = serverSocket.accept();
					executors.execute(() -> {
						JShell.Builder builder = JShell.builder();
						builder.fileManager(mapping -> mapping);
						try (JShell jshell = builder.build();
								Socket clientSocket = sock;
								PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);
								BufferedReader in = new BufferedReader(
										new InputStreamReader(clientSocket.getInputStream()));) {
							jshell.addToClasspath(System.getProperty("java.class.path"));
							out.println("Welcome to DataSamudaya JShell server!");
							out.println("Type 'quit' to exit.");
							out.println("Done");

							String line;
							while ((line = in.readLine()) != null) {
								if (line.equalsIgnoreCase("quit")) {
									out.println("Quit");
									break;
								}
								java.util.List<SnippetEvent> events = jshell.eval(line);
								for (SnippetEvent event : events) {
									if (event.status() == Snippet.Status.REJECTED && nonNull(event.exception())) {
										out.println(event.exception().getMessage());
									} else if (event.causeSnippet() == null) {
										out.println(event.value());
									}
								}
								out.println("Done");
							}
						} catch (Exception e) {
							log.error(DataSamudayaConstants.EMPTY, e);
						}
					});
				} catch (Exception e) {
					log.error(DataSamudayaConstants.EMPTY, e);
				}
			}
		});
	}
}
