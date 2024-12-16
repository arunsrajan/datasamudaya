package com.github.datasamudaya.tasks.executor;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.ehcache.Cache;
import org.jooq.lambda.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.DataCruncherContext;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.FieldCollationDirection;
import com.github.datasamudaya.common.NodeIndexKey;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.utils.DiskSpillingContext;
import com.github.datasamudaya.common.utils.IteratorType;
import com.github.datasamudaya.common.utils.RemoteIteratorClose;
import com.github.datasamudaya.common.utils.RemoteIteratorHasNext;
import com.github.datasamudaya.common.utils.RemoteIteratorNext;
import com.github.datasamudaya.common.utils.RemoteIteratorTask;
import com.github.datasamudaya.common.utils.RequestType;
import com.github.datasamudaya.common.utils.Utils;

/**
 * The class iterates the list from cache which is a remote server.
 * @author arun
 *
 * @param <T>
 */
public class RemoteIteratorServer<T> {
	private final Cache<String, byte[]> cache;
	private final Map<String, Object> apptaskexecutormap;
	private static final Logger log = LoggerFactory.getLogger(RemoteIteratorServer.class);

	public RemoteIteratorServer(Cache<String, byte[]> cache, Map<String, Object> apptaskexecutormap) {
		this.cache = cache;
		this.apptaskexecutormap = apptaskexecutormap;
	}

	public Tuple2<ServerSocket, ExecutorService> start() {
		ExecutorService es = Executors.newFixedThreadPool(Integer.parseInt(DataSamudayaProperties.get()
				.getProperty(DataSamudayaConstants.VIRTUALTHREADSPOOLSIZE,
						DataSamudayaConstants.VIRTUALTHREADSPOOLSIZE_DEFAULT)), Thread.ofVirtual().factory());
		try {
			ServerSocket serverSocket = new ServerSocket(0);
			log.debug("Server listening on port {}", serverSocket.getLocalPort());
			es.execute(() -> {
				while (true) {
					final Socket clientSocket;
					try {
						if (serverSocket.isClosed()) {
							break;
						}
						clientSocket = serverSocket.accept();
					} catch (Exception e) {
						log.error(DataSamudayaConstants.EMPTY, e);
						continue;
					}
					log.debug("Client connected");
					es.execute(() -> {
						Kryo kryo = Utils.getKryoInstance();
						InputStream baistream = null;
						SnappyInputStream sis = null;
						Input inputfile = null;
						List currentList = null;
						Task task = null;
						List<FieldCollationDirection> lfcds = null;
						IteratorType iteratortype = null;
						DiskSpillingContext dsc = null;
						try (Socket socket = clientSocket;
							InputStream istream = socket.getInputStream();
							Input input = new Input(istream);
							OutputStream ostream = socket.getOutputStream();
							Output output = new Output(ostream);) {
							int indexperlist = 0;
							int totindex = 0;
							log.debug("Input Stream and Output Stream Obtained");
							while (true) {
								if (socket.isClosed()) {
									break;
								}
								Object deserobj = kryo.readClassAndObject(input);
								if (deserobj instanceof RemoteIteratorClose) {
									break;
								} else if (deserobj instanceof RemoteIteratorTask rlit) {
									task = rlit.getTask();
									lfcds = rlit.getFcsc();
									iteratortype = rlit.getIteratortype();
									log.debug("Obtaining Cache for File {} with FCD {}", task.jobid + DataSamudayaConstants.HYPHEN + task.stageid + DataSamudayaConstants.HYPHEN + task.taskid, lfcds);
									byte[] bt = cache.get(task.jobid + DataSamudayaConstants.HYPHEN + task.stageid + DataSamudayaConstants.HYPHEN + task.taskid);
									if (isNull(bt)) {
										Object taskmapcomred = apptaskexecutormap.get(task.jobid + task.stageid + task.taskid);
										if (nonNull(taskmapcomred)) {
											if (taskmapcomred instanceof TaskExecutorMapper tem) {
												dsc = (DiskSpillingContext) tem.ctx;
											} else if (taskmapcomred instanceof TaskExecutorCombiner tec) {
												dsc = (DiskSpillingContext) tec.ctx;
											} else if (taskmapcomred instanceof TaskExecutorReducer ter) {
												dsc = (DiskSpillingContext) ter.ctx;
											}
											if (dsc.isSpilled()) {
												String filepath = Utils.getLocalFilePathForMRTask(dsc.getTask(), DataSamudayaConstants.EMPTY);
												log.debug("Obtaining input file {}", filepath);
												baistream = new FileInputStream(filepath);
												sis = new SnappyInputStream(baistream);
												inputfile = new Input(sis);
												dsc = null;
												log.debug("Obtaining Input {} for input file {}", baistream, filepath);
											}
										} else {
											baistream = new FileInputStream(Utils.getLocalFilePathForTask(task, rlit.getAppendwithpath(), rlit.isAppendintermediate(), rlit.isLeft(), rlit.isRight()));
											sis = new SnappyInputStream(baistream);
											inputfile = new Input(sis);
											log.debug("Obtaining Input {}", baistream);
										}
									} else {
										baistream = new ByteArrayInputStream(bt);
										sis = new SnappyInputStream(baistream);
										inputfile = new Input(sis);
										log.debug("Obtaining Input {}", baistream);
									}
									indexperlist = 0;
									totindex = 0;
								} else if (deserobj instanceof RemoteIteratorHasNext rlit) {
									boolean isavailable = nonNull(inputfile) && inputfile.available() > 0
											|| nonNull(currentList) && indexperlist < currentList.size()
											|| nonNull(dsc);
									kryo.writeClassAndObject(output, isavailable);
									output.flush();
								} else if (deserobj instanceof RemoteIteratorNext rlin) {
									if (rlin.getRequestType() == RequestType.CONTEXT) {
										if (nonNull(dsc) && !dsc.isSpilled()) {
											DataCruncherContext ctx = dsc.getContext();
											kryo.writeClassAndObject(output, Utils.convertObjectToBytesCompressed(ctx.get(rlin.getKey()), null));
											dsc = null;
										} else if (iteratortype == IteratorType.DISKSPILLITERATOR) {
											DataCruncherContext ctx = (DataCruncherContext) Utils.getKryoInstance().readClassAndObject(inputfile);
											kryo.writeClassAndObject(output, Utils.convertObjectToBytesCompressed(ctx.get(rlin.getKey()), null));
										}
										output.flush();
									} else {
										if (isNull(currentList) || indexperlist >= currentList.size()) {
											Object data = kryo.readClassAndObject(inputfile);
											currentList = (List) (data instanceof Set ? new ArrayList<>((Collection) data) : (List) data);
											indexperlist = 0;
										}
										if (rlin.getRequestType() == RequestType.ELEMENT) {
											if (iteratortype == IteratorType.SORTORUNIONORINTERSECTION) {
												Object objfromfile = currentList.get(indexperlist);
												kryo.writeClassAndObject(output, getObjectToNik(lfcds, totindex, task, objfromfile));
											} else {
												Object objfromfile = currentList.get(indexperlist);
												kryo.writeClassAndObject(output, objfromfile);
											}
											output.flush();
											indexperlist++;
											totindex++;
										} else if (rlin.getRequestType() == RequestType.LIST) {
											// Send the entire list
										if (iteratortype == IteratorType.SORTORUNIONORINTERSECTION) {
												List<NodeIndexKey> niks = new ArrayList<>();
												for (Object obj :currentList) {
													niks.add(getObjectToNik(lfcds, totindex, task, obj));
													totindex++;
												}
												kryo.writeClassAndObject(output, Utils.convertObjectToBytesCompressed(niks, null));
											} else if (iteratortype == IteratorType.DISKSPILLITERATOR) {
												kryo.writeClassAndObject(output, Utils.convertObjectToBytesCompressed(currentList, null));
											}
											output.flush();
											indexperlist = currentList.size(); // Ensure we mark as done
										}
									}
								}
							}
						} catch (Exception e) {
							log.error(DataSamudayaConstants.EMPTY, e);
						} finally {
							if (nonNull(inputfile)) {
								inputfile.close();
							}
							if (nonNull(sis)) {
								try {
									sis.close();
								} catch (IOException e) {

								}
							}
							if (nonNull(baistream)) {
								try {
									baistream.close();
								} catch (IOException e) {

								}
							}
						}
					});
				}
			});
			return new Tuple2<ServerSocket, ExecutorService>(serverSocket, es);
		} catch (Exception e) {
			log.error(DataSamudayaConstants.EMPTY, e);
		}
		return null;
	}

	protected NodeIndexKey getObjectToNik(List<FieldCollationDirection> lfcds, int totindex, Task task, Object objfromfile) {
		NodeIndexKey nik = new NodeIndexKey();
		if (objfromfile instanceof Tuple2 tup2) {
			nik.setIndex(totindex);
			nik.setNode(task.getHostport());
			if (nonNull(lfcds)) {
				nik.setKey(Utils.getKeyFromNodeIndexKey(lfcds, (Object[]) tup2.v1()));
			} else {
				nik.setValue(objfromfile);
			}
		} else if (objfromfile instanceof NodeIndexKey nikfromfile) {
			nik.setIndex(totindex);
			nik.setNode(task.getHostport());
			if (nonNull(lfcds)) {
				nik.setKey(Utils.getKeyFromNodeIndexKey(lfcds, (Object[]) nikfromfile.getValue()));
			} else {
				nik.setValue(nikfromfile.getValue());
			}
		} else {
			nik.setIndex(totindex);
			nik.setNode(task.getHostport());
			if (nonNull(lfcds)) {
				nik.setKey(Utils.getKeyFromNodeIndexKey(lfcds, (Object[]) objfromfile));
			} else {
				nik.setValue(objfromfile);
			}
		}
		return nik;
	}

}
