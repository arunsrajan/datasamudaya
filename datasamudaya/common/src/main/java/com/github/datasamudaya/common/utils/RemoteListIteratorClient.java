package com.github.datasamudaya.common.utils;

import static java.util.Objects.nonNull;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Iterator;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.Task;

import net.jpountz.lz4.LZ4BlockInputStream;

/**
 * The class is client iterates the remote list from cache.
 * @author arun
 *
 * @param <T>
 */
public class RemoteListIteratorClient<T> implements Iterator<T>, Closeable {
	private InputStream inputStream;
	private LZ4BlockInputStream lbis;
	private Input input;
	
	private OutputStream outputStream;
	private Output output;
	
	private Task task;
	
	Socket socket;
	Kryo kryo = Utils.getKryo();
	RemoteListIteratorHasNext hasnext = new RemoteListIteratorHasNext();
	RemoteListIteratorNext nextelem = new RemoteListIteratorNext();
	public RemoteListIteratorClient(Task task) {
		try {
			String[] hostport = task.getHostport().split(DataSamudayaConstants.EMPTY);
			socket = new Socket(hostport[0], Integer.parseInt(hostport[1]));
			inputStream = socket.getInputStream();
			lbis = new LZ4BlockInputStream(inputStream);
			input = new Input(lbis);
			outputStream = socket.getOutputStream();
			output = new Output(outputStream);
			this.task = task;
			kryo.writeClassAndObject(output, new RemoteListIteratorTask(task));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	@Override
	public boolean hasNext() {
		kryo.writeClassAndObject(output, hasnext);
		return (boolean) kryo.readClassAndObject(input);
	}

	@Override
	public T next() {
		kryo.writeClassAndObject(output, nextelem);
		return (T) kryo.readClassAndObject(input);
	}

	@Override
	public void close() throws IOException {

		if(nonNull(input)) {
			input.close();
		}
		if(nonNull(lbis)) {
			lbis.close();
		}
		if(nonNull(inputStream)) {
			inputStream.close();
		}
		if(nonNull(output)) {
			output.close();
		}
		if(nonNull(outputStream)) {
			outputStream.close();
		}

		if (nonNull(socket)) {
			socket.close();
		}

	}
}
