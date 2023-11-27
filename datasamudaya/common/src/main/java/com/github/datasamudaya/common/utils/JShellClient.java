package com.github.datasamudaya.common.utils;

import java.io.*;
import java.net.Socket;
import java.net.URL;

import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.burningwave.core.assembler.StaticComponentContainer;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;

import static java.util.Objects.nonNull;

/**
 * JShell client console for the DataSamudaya.
 * @author arun
 *
 */
public class JShellClient {
	public static void main(String[] args) throws Exception {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		Utils.initializeProperties(DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
				+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH, DataSamudayaConstants.DATASAMUDAYA_PROPERTIES);
		StaticComponentContainer.Modules.exportAllToAll();
		String hostName = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_HOST);
		// set the hostname of the sql server
		int portNumber = Integer
				.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.SHELLPORT, DataSamudayaConstants.SHELLPORT_DEFAULT));
		// set the port number of the sql server

		Socket socket = new Socket(hostName, portNumber);
		PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
		BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
		BufferedReader stdIn = new BufferedReader(new InputStreamReader(System.in));
		String userInput;
		String serverResponse;
		outer: while (true) {
			while (nonNull(serverResponse = in.readLine())) {
				if ("Done".equals(serverResponse)) {
					break;
				} else if ("Quit".equals(serverResponse)) {
					break outer;
				}
				System.out.println("Server response: " + serverResponse);
			}
			userInput = getUserInput(stdIn);
			out.println(userInput);
		}

		out.close();
		in.close();
		stdIn.close();
		socket.close();
	}

	private static String getUserInput(BufferedReader stdIn) throws IOException {
		System.out.print("SHELL> ");
		return stdIn.readLine();
	}
}
