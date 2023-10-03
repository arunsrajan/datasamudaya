package com.github.datasamudaya.common;

import org.apache.pig.PigServer;

public class GlobalPigServer {
	
	private static PigServer pigserver;
	
	private GlobalPigServer() {}
	
	public static PigServer getPigServer() {
		return pigserver;
	}
	
	public static void setPigServer(PigServer pigserver) {
		GlobalPigServer.pigserver = pigserver;
	}

}
