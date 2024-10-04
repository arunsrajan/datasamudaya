package com.github.datasamudaya.stream.pig;

import static java.util.Objects.nonNull;

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
import org.apache.pig.newplan.logical.relational.LogicalPlan;
import org.apache.pig.parser.QueryParserDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.yarn.YarnSystemConstants;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaConstants.STORAGE;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.LaunchContainers;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.utils.DataSamudayaMetricsExporter;
import com.github.datasamudaya.common.utils.Utils;

/**
 * Pig server to process pig commands.
 * @author arun
 *
 */
public class PigQueryServer {
	static Logger log = LoggerFactory.getLogger(PigQueryServer.class);
	static ServerSocket serverSocket;
	static QueryParserDriver queryParserDriver;

	/**
	 * Start the Pig server.
	 * @throws Exception
	 */
	public static void start() throws Exception {
		ExecutorService executors = Executors.newFixedThreadPool(10);
		serverSocket = new ServerSocket(Integer.valueOf(DataSamudayaProperties.get()
				.getProperty(DataSamudayaConstants.PIGPORT, DataSamudayaConstants.PIGPORT_DEFAULT)));
		queryParserDriver = PigUtils.getQueryParserDriver("pig");
		executors.execute(() -> {
			while (true) {
				Socket sock;
				try {
					sock = serverSocket.accept();
					executors.execute(() -> {
						PipelineConfig pipelineconfig = new PipelineConfig();
						pipelineconfig.setLocal("false");
						pipelineconfig.setYarn("false");
						pipelineconfig.setMesos("false");
						pipelineconfig.setJgroups("false");
						pipelineconfig.setMode(DataSamudayaConstants.MODE_NORMAL);
						pipelineconfig.setStorage(STORAGE.COLUMNARSQL);
						String user = "";
						int numberofcontainers = 1;
						int cpupercontainer = 1;
						int memorypercontainer = 1024;
						int cpudriver = 1;
						int memorydriver = 1024;
						boolean isdriverrequired;
						String scheduler = "";
						String tejobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
						Map<String, Object> pigAliasExecutedObjectMap = new ConcurrentHashMap<>();
						List<String> pigQueries = new ArrayList<>();
						List<String> pigQueriesToExecute = new ArrayList<>();
						boolean isyarncontainerlaunched = false;
						boolean iscontainerlaunched = false;
						try (Socket clientSocket = sock;
								OutputStream ostream = clientSocket.getOutputStream();
								PrintWriter out = new PrintWriter(ostream, true);
								BufferedReader in = new BufferedReader(
										new InputStreamReader(clientSocket.getInputStream()));) {
							pipelineconfig.setPigoutput(ostream);
							pipelineconfig.setWriter(out);
							pipelineconfig.setJobname(DataSamudayaConstants.PIG);
							user = in.readLine();
							numberofcontainers = Integer.valueOf(in.readLine());
							cpupercontainer = Integer.valueOf(in.readLine());
							memorypercontainer = Integer.valueOf(in.readLine());
							cpudriver = Integer.valueOf(in.readLine());
							memorydriver = Integer.valueOf(in.readLine());
							isdriverrequired = Boolean.parseBoolean(in.readLine());
							scheduler = in.readLine();
							pipelineconfig.setIsremotescheduler(isdriverrequired);
							if (!Utils.isUserExists(user)) {
								String usernotexistsmessage = "User " + user + " is not configured. Exiting...";
								out.println(usernotexistsmessage);
								out.println("Quit");
								throw new Exception(usernotexistsmessage);
							}
							List<LaunchContainers> containers = null;
							Map<String, Object> cpumemory = null;
							if (scheduler.equalsIgnoreCase(DataSamudayaConstants.EXECMODE_DEFAULT)
									|| scheduler.equalsIgnoreCase(DataSamudayaConstants.JGROUPS)) {
								tejobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
								containers = Utils.launchContainersExecutorSpecWithDriverSpec(user, tejobid, cpupercontainer, memorypercontainer, numberofcontainers, cpudriver, memorydriver, true);
								cpumemory = Utils.getAllocatedContainersResources(containers);
								out.println("User '" + user + "' connected with cpu " + cpumemory.get(DataSamudayaConstants.CPUS) + " and memory " + cpumemory.get(DataSamudayaConstants.MEM) + " mb");
								Utils.printNodesAndContainers(containers, out);
								iscontainerlaunched = true;
							}
							Utils.setConfigForScheduler(scheduler, pipelineconfig);
							boolean isjgroups = false;
							boolean isignite = false;
							boolean isyarn = false;
							if (scheduler.equalsIgnoreCase(DataSamudayaConstants.JGROUPS)) {
								isjgroups = true;
								isignite = false;
								isyarn = false;
							} else if (scheduler.equalsIgnoreCase(DataSamudayaConstants.YARN)) {
								isjgroups = false;
								isignite = false;
								isyarn = true;
								tejobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
								Utils.launchYARNExecutors(tejobid, cpupercontainer, memorypercontainer, numberofcontainers, DataSamudayaConstants.SQL_YARN_DEFAULT_APP_CONTEXT_FILE, isdriverrequired);
								isyarncontainerlaunched = true;
							} else if (scheduler.equalsIgnoreCase(DataSamudayaConstants.STANDALONE)) {
								isjgroups = false;
								isignite = false;
								isyarn = false;
							} else if (scheduler.equalsIgnoreCase(DataSamudayaConstants.EXECMODE_IGNITE)) {
								isjgroups = false;
								isignite = true;
								isyarn = false;
								String memstr = String.valueOf(Long.valueOf((long)memorypercontainer) * DataSamudayaConstants.MB);
								pipelineconfig.setMinmem(memstr);
								pipelineconfig.setMaxmem(memstr);
							}
							out.println("Welcome to the Pig Server!");
							out.println("Type 'quit' to exit.");
							out.println("Done");
							String inputLine;
							outer:
							while (true) {
								try {
									while ((inputLine = in.readLine()) != null) {
										if ("quit".equalsIgnoreCase(inputLine)) {
											out.println("Quit");
											break outer;
										}
										out.println(Utils.formatDate(new Date()) + "> " + inputLine);
										inputLine = StringUtils.normalizeSpace(inputLine.trim());
										if (inputLine.startsWith("setmode")) {
											String[] mode = inputLine.split(" ");
											if (mode.length == 2) {
												pigAliasExecutedObjectMap.clear();
												if (mode[1].equalsIgnoreCase(DataSamudayaConstants.JGROUPS)) {
													isjgroups = true;
													isignite = false;
													isyarn = false;
													pipelineconfig.setLocal("false");
													pipelineconfig.setYarn("false");
													pipelineconfig.setMesos("false");
													pipelineconfig.setJgroups("true");
													pipelineconfig.setMode(DataSamudayaConstants.MODE_NORMAL);
													if (isyarncontainerlaunched) {
														try {
															Utils.shutDownYARNContainer(tejobid);
														} catch (Exception ex) {
															log.error(DataSamudayaConstants.EMPTY, ex);
														}
														isyarncontainerlaunched = false;
													}
													if (!iscontainerlaunched) {
														tejobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
														containers = Utils.launchContainersExecutorSpecWithDriverSpec(user, tejobid, cpupercontainer, memorypercontainer, numberofcontainers, cpudriver, memorydriver, true);
														cpumemory = Utils.getAllocatedContainersResources(containers);
														iscontainerlaunched = true;
														out.println("User '" + user + "' connected with cpu " + cpumemory.get(DataSamudayaConstants.CPUS) + " and memory " + cpumemory.get(DataSamudayaConstants.MEM) + " mb");
														Utils.printNodesAndContainers(containers, out);
														iscontainerlaunched = true;
													}
													out.println("jgroups mode set");
												} else if (mode[1].equalsIgnoreCase(DataSamudayaConstants.MODE_DEFAULT)) {
													isjgroups = false;
													isignite = true;
													isyarn = false;
													pipelineconfig.setLocal("false");
													pipelineconfig.setYarn("false");
													pipelineconfig.setMesos("false");
													pipelineconfig.setJgroups("false");
													pipelineconfig.setMode(DataSamudayaConstants.MODE_DEFAULT);
													String memstr = String.valueOf(Long.valueOf((long)memorypercontainer) * DataSamudayaConstants.MB);
													pipelineconfig.setMinmem(memstr);
													pipelineconfig.setMaxmem(memstr);
													if (isyarncontainerlaunched) {
														try {
															Utils.shutDownYARNContainer(tejobid);
														} catch (Exception ex) {
															log.error(DataSamudayaConstants.EMPTY, ex);
														}
														isyarncontainerlaunched = false;
													}
													if (iscontainerlaunched) {
														try {
															Utils.destroyContainers(user, tejobid);
														} catch (Exception ex) {
															log.error(DataSamudayaConstants.EMPTY, ex);
														}
														iscontainerlaunched = false;
													}
													out.println("ignite mode set");
												} else if (mode[1].equalsIgnoreCase(DataSamudayaConstants.YARN)) {
													isjgroups = false;
													isignite = false;
													isyarn = true;
													pipelineconfig.setLocal("false");
													pipelineconfig.setYarn("true");
													pipelineconfig.setMesos("false");
													pipelineconfig.setJgroups("false");
													pipelineconfig.setMode(DataSamudayaConstants.MODE_NORMAL);
													if (iscontainerlaunched) {
														try {
															Utils.destroyContainers(user, tejobid);
														} catch (Exception ex) {
															log.error(DataSamudayaConstants.EMPTY, ex);
														}
														iscontainerlaunched = false;
													}
													if (!isyarncontainerlaunched) {
														try {
															tejobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
															Utils.launchYARNExecutors(tejobid, cpupercontainer, memorypercontainer, numberofcontainers, DataSamudayaConstants.SQL_YARN_DEFAULT_APP_CONTEXT_FILE, isdriverrequired);
														} catch (Exception ex) {
															log.error(DataSamudayaConstants.EMPTY, ex);
														}
														isyarncontainerlaunched = true;
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
													if (isyarncontainerlaunched) {
														try {
															Utils.shutDownYARNContainer(tejobid);
														} catch (Exception ex) {
															log.error(DataSamudayaConstants.EMPTY, ex);
														}
														isyarncontainerlaunched = false;
													}
													if (!iscontainerlaunched) {
														tejobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
														containers = Utils.launchContainersExecutorSpecWithDriverSpec(user, tejobid, cpupercontainer, memorypercontainer, numberofcontainers, cpudriver, memorydriver, true);
														cpumemory = Utils.getAllocatedContainersResources(containers);
														iscontainerlaunched = true;
														out.println("User '" + user + "' connected with cpu " + cpumemory.get(DataSamudayaConstants.CPUS) + " and memory " + cpumemory.get(DataSamudayaConstants.MEM) + " mb");
														Utils.printNodesAndContainers(containers, out);
														iscontainerlaunched = true;
													}
													out.println("jgroups, ignite and yarn mode unset");
												}
											}
										} else if (inputLine.startsWith("getmode")) {
											if (isignite) {
												out.println("ignite");
											} else if (isjgroups) {
												out.println("jgroups");
											} else if (isyarn) {
												out.println("yarn");
											} else {
												out.println("standalone");
											}
										} else if (inputLine.startsWith("dump") || inputLine.startsWith("DUMP")) {
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
											PigQueryExecutor.executePlan(lp, false, dumpwithalias[1].trim(), user, jobid, tejobid, pipelineconfig);
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
													PigQueryExecutor.executePlan(lp, true, DataSamudayaConstants.EMPTY, user, jobid, tejobid, pipelineconfig);
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
										out.println("Done");
									}
								} catch (SocketException socketexception) {
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
							if (iscontainerlaunched) {
								try {
									Utils.destroyContainers(user, tejobid);
								} catch (Exception ex) {
									log.error(DataSamudayaConstants.EMPTY, ex);
								}
							}
							if (isyarncontainerlaunched) {
								try {
									Utils.shutDownYARNContainer(tejobid);
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

	private PigQueryServer() {
	}
}
