package com.github.datasamudaya.stream.pig;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.StringUtils;
import org.apache.pig.parser.QueryParserDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.LaunchContainers;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.DataSamudayaConstants.STORAGE;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.StreamPipeline;

/**
 * Pig server to process pig commands.
 * @author arun
 *
 */
public class PigQueryServer {
	static Logger log = LoggerFactory.getLogger(PigQueryServer.class);
	static ServerSocket serverSocket = null;		
	static QueryParserDriver queryParserDriver = null;	
	static PipelineConfig pipelineconfig = new PipelineConfig();
	/**
	 * Start the Pig server.
	 * @throws Exception
	 */
	public static void start() throws Exception {		
		ExecutorService executors = Executors.newFixedThreadPool(10);
		serverSocket = new ServerSocket(Integer.valueOf(DataSamudayaProperties.get()
				.getProperty(DataSamudayaConstants.PIGPORT,DataSamudayaConstants.PIGPORT_DEFAULT)));
		queryParserDriver = PigUtils.getQueryParserDriver("pig");
		pipelineconfig.setLocal("false");
		pipelineconfig.setYarn("false");
		pipelineconfig.setMesos("false");
		pipelineconfig.setJgroups("false");
		pipelineconfig.setMode(DataSamudayaConstants.MODE_NORMAL);
		pipelineconfig.setStorage(STORAGE.INMEMORY_DISK);
		executors.execute(() -> {
			while (true) {
				Socket sock;
				try {
					sock = serverSocket.accept();
					executors.execute(() -> {
						String user = "";
						String tejobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
						Map<String, Object> pigAliasExecutedObjectMap  = new ConcurrentHashMap<>();
						List<String> pigQueries = new ArrayList<>();
						List<String> pigQueriesToExecute = new ArrayList<>();
						boolean iscontainerlaunched = false;
						try (Socket clientSocket = sock;
								OutputStream ostream = clientSocket.getOutputStream();
								PrintWriter out = new PrintWriter(ostream, true);
								BufferedReader in = new BufferedReader(
										new InputStreamReader(clientSocket.getInputStream()));) {
							pipelineconfig.setPigoutput(ostream);
							user = in.readLine();
							if(!Utils.isUserExists(user)) {
								String usernotexistsmessage = "User "+user+" is not configured. Exiting...";
								out.println(usernotexistsmessage);
								out.println("Quit");
								throw new Exception(usernotexistsmessage);
							}
							List<LaunchContainers> containers = Utils.launchContainers(user, tejobid);
							var cpumemory = Utils.getAllocatedContainersResources(containers);
							out.println("User '"+user +"' connected with cpu "+cpumemory.get(DataSamudayaConstants.CPUS) +" and memory "+cpumemory.get(DataSamudayaConstants.MEM) +" mb");
							Utils.printNodesAndContainers(containers, out);
							out.println("Welcome to the Pig Server!");
							out.println("Type 'quit' to exit.");
							out.println("Done");
							iscontainerlaunched = true;
							String inputLine;
							boolean isjgroups = false;
							boolean isignite = false;
							boolean isyarn = false;
							String dbdefault = DataSamudayaProperties.get()
									.getProperty(DataSamudayaConstants.SQLDB, DataSamudayaConstants.SQLMETASTORE_DB);
							outer:
							while (true) {
								try {
									while ((inputLine = in.readLine()) != null) {
										if (inputLine.equalsIgnoreCase("quit")) {
											out.println("Quit");
											break outer;
										}
										out.println(Utils.formatDate(new Date()) + "> " + inputLine);
										inputLine = StringUtils.normalizeSpace(inputLine.trim());
										if (inputLine.startsWith("setmode")) {
											String[] mode = inputLine.split(" ");
											if(mode.length == 2) {
												pigAliasExecutedObjectMap.clear();
												if(mode[1].equalsIgnoreCase(DataSamudayaConstants.JGROUPS)) {
													isjgroups = true;
													isignite = false;
													isyarn = false;
													pipelineconfig.setLocal("false");
													pipelineconfig.setYarn("false");
													pipelineconfig.setMesos("false");
													pipelineconfig.setJgroups("true");
													pipelineconfig.setMode(DataSamudayaConstants.MODE_NORMAL);
													if(!iscontainerlaunched) {
														containers = Utils.launchContainers(user, tejobid);
														cpumemory = Utils.getAllocatedContainersResources(containers);
														iscontainerlaunched = true;
														out.println("User '"+user +"' connected with cpu "+cpumemory.get(DataSamudayaConstants.CPUS) +" and memory "+cpumemory.get(DataSamudayaConstants.MEM) +" mb");
													}
													out.println("jgroups mode set");
												} else if(mode[1].equalsIgnoreCase(DataSamudayaConstants.MODE_DEFAULT)) {
													isjgroups = false;
													isignite = true;
													isyarn = false;
													pipelineconfig.setLocal("false");
													pipelineconfig.setYarn("false");
													pipelineconfig.setMesos("false");
													pipelineconfig.setJgroups("false");
													pipelineconfig.setMode(DataSamudayaConstants.MODE_DEFAULT);
													if(iscontainerlaunched) {
														try {
															Utils.destroyContainers(user, tejobid);
														} catch (Exception ex) {
															log.error(DataSamudayaConstants.EMPTY, ex);
														}
														iscontainerlaunched = false;
													}
													out.println("ignite mode set");
												} else if(mode[1].equalsIgnoreCase(DataSamudayaConstants.YARN)) {
													isjgroups = false;
													isignite = false;
													isyarn = true;
													pipelineconfig.setLocal("false");
													pipelineconfig.setYarn("true");
													pipelineconfig.setMesos("false");
													pipelineconfig.setJgroups("false");
													pipelineconfig.setMode(DataSamudayaConstants.MODE_NORMAL);
													if(iscontainerlaunched) {
														try {
															Utils.destroyContainers(user, tejobid);
														} catch (Exception ex) {
															log.error(DataSamudayaConstants.EMPTY, ex);
														}
														iscontainerlaunched = false;
													}
													out.println("yarn mode set");
												} else {
													isjgroups = false;
													isignite = false;
													isyarn = false;
													pipelineconfig.setLocal("false");
													pipelineconfig.setYarn("false");
													pipelineconfig.setMesos("false");
													pipelineconfig.setJgroups("false");
													pipelineconfig.setMode(DataSamudayaConstants.MODE_NORMAL);
													if(!iscontainerlaunched) {
														containers = Utils.launchContainers(user, tejobid);
														cpumemory = Utils.getAllocatedContainersResources(containers);
														iscontainerlaunched = true;
														out.println("User '"+user +"' connected with cpu "+cpumemory.get(DataSamudayaConstants.CPUS) +" and memory "+cpumemory.get(DataSamudayaConstants.MEM) +" mb");
													}
													out.println("jgroups, ignite and yarn mode unset");
												}
											}
										} else if (inputLine.startsWith("getmode")) {
											if(isignite) {
												out.println("ignite");
											}
											else if (isjgroups) {
												out.println("jgroups");
											} else if (isyarn) {
												out.println("yarn");
											} else {
												out.println("standalone");
											}
										} else if (inputLine.startsWith("dump") || inputLine.startsWith("DUMP")) {
											inputLine = inputLine.replace(";", "");
											String[] dumpwithalias = inputLine.split(" ");
											long starttime = System.currentTimeMillis();
											String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();											
									    	PigUtils.executeDump((StreamPipeline<Map<String, Object>>) pigAliasExecutedObjectMap.get(dumpwithalias[1].trim()), user, jobid, tejobid, pipelineconfig);
											double timetaken = ((System.currentTimeMillis()-starttime)/1000.0);											
											out.println("Time taken " + timetaken +" seconds");
											out.println("");
										} else {
											long starttime = System.currentTimeMillis();
											String jobid = DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID();
											pigQueriesToExecute.clear();
											pigQueriesToExecute.addAll(pigQueries);
											pigQueriesToExecute.add(inputLine);
											pigQueriesToExecute.add("\n");
									    	PigQueryExecutor.execute(pigAliasExecutedObjectMap, queryParserDriver, pigQueriesToExecute, user, jobid, tejobid, pipelineconfig);
									    	pigQueries.add(inputLine);
									    	pigQueries.add("\n");
											double timetaken = ((System.currentTimeMillis()-starttime)/1000.0);											
											out.println("Time taken " + timetaken +" seconds");
											out.println("");
										}
										out.println("Done");
									}
								} catch(SocketException socketexception) {
									log.error(DataSamudayaConstants.EMPTY, socketexception);
									out.println(socketexception.getMessage());
									out.println("Done");
									break outer;
								} catch (Exception exception) {
					            	out.println(exception.getMessage());
									out.println("Done");
									log.error(DataSamudayaConstants.EMPTY, exception);
								}
							}							
						} catch (Exception ex) {
							log.error(DataSamudayaConstants.EMPTY, ex);							
						} finally {
							if(iscontainerlaunched) {
								try {
									Utils.destroyContainers(user, tejobid);
								} catch (Exception ex) {
									log.error(DataSamudayaConstants.EMPTY, ex);
								}
							}
						}
					});
				} catch (Exception e) {
					log.error(DataSamudayaConstants.EMPTY, e);
				}
			}
		});
	}
}
