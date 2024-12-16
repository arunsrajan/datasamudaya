package com.github.datasamudaya.common.utils;

import static java.util.Objects.nonNull;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Iterator;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.FieldCollationDirection;
import com.github.datasamudaya.common.SorterPort;
import com.github.datasamudaya.common.Task;

import net.jpountz.lz4.LZ4BlockInputStream;

/**
 * The class is client iterates the remote list from cache.
 * @author arun
 *
 * @param <T>
 */
public class RemoteIteratorClient<T> implements Iterator<T>, Closeable {

	private static final Logger log = LoggerFactory.getLogger(RemoteIteratorClient.class);

	private InputStream inputStream;
	private LZ4BlockInputStream lbis;
	private Input input;
	private boolean isclosed;
	private OutputStream outputStream;
	private Output output;
	private int sorterport;
	private Task task;
	private RequestType requesttype;
	Socket socket;
	Kryo kryo = Utils.getKryo();
	RemoteIteratorHasNext hasnext = new RemoteIteratorHasNext();
	RemoteIteratorNext nextelem = new RemoteIteratorNext();

	public RemoteIteratorClient(Task task, String appendwithpath, boolean appendintermediate, boolean left, boolean right, List<FieldCollationDirection> fcsc, RequestType requesttype, IteratorType iteratortype, boolean ismr, Object key) throws Exception {
		try {
			log.debug("Trying to split host and port {}", task.getHostport());
			String[] hostport = task.getHostport().split(DataSamudayaConstants.UNDERSCORE);
			log.debug("Connecting To Host {} And Port {} For Fetching And Sorting with task executor id {}", hostport[0], hostport[1], task.teid);
			sorterport = (int) Utils.getResultObjectByInput(task.getHostport(), new SorterPort(), task.teid);
			log.debug("Connecting To Host {} And Port {} For Fetching And Sorting", hostport[0], sorterport);
			socket = new Socket(hostport[0], sorterport);
			inputStream = socket.getInputStream();
			input = new Input(inputStream);
			outputStream = socket.getOutputStream();
			output = new Output(outputStream);
			this.task = task;
			kryo.writeClassAndObject(output, new RemoteIteratorTask(task, appendwithpath, appendintermediate, left, right, ismr, fcsc, iteratortype));
			output.flush();
			this.requesttype = requesttype;
			nextelem.setRequestType(requesttype);
			nextelem.setKey(key);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean hasNext() {
		kryo.writeClassAndObject(output, hasnext);
		output.flush();
		return (boolean) kryo.readClassAndObject(input);
	}

	@Override
	public T next() {
		kryo.writeClassAndObject(output, nextelem);
		output.flush();
		return (T) kryo.readClassAndObject(input);
	}

	public int getSorterport() {
		return sorterport;
	}

	@Override
	public void close() throws IOException {
		kryo.writeClassAndObject(output, new RemoteIteratorClose());
		output.flush();
		if (nonNull(input)) {
			input.close();
		}
		if (nonNull(lbis)) {
			lbis.close();
		}
		if (nonNull(inputStream)) {
			inputStream.close();
		}
		if (nonNull(output)) {
			output.close();
		}
		if (nonNull(outputStream)) {
			outputStream.close();
		}

		if (nonNull(socket)) {
			log.debug("Closing Connection At Client");
			socket.close();
		}
		isclosed = true;
	}

	public boolean isclosed() {
		return isclosed;
	}

}
