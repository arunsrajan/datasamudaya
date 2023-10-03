package com.github.datasamudaya.common;

import java.rmi.Remote;
import java.rmi.RemoteException;

/**
 * Interface for rmi using which class implements and binds to rmi server.
 * @author arun
 *
 */
public interface StreamDataCruncher extends Remote{
	public Object postObject(Object object)throws RemoteException;
}
