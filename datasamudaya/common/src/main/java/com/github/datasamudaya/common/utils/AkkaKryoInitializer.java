package com.github.datasamudaya.common.utils;

import io.altoo.akka.serialization.kryo.DefaultKryoInitializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.altoo.akka.serialization.kryo.serializer.scala.ScalaKryo;


/**
 * Kryo Initialization for Akka Actor system
 * @author arun
 *
 */
public class AkkaKryoInitializer extends DefaultKryoInitializer {

	private final Logger log = LoggerFactory.getLogger(AkkaKryoInitializer.class);

	@Override
	public void preInit(ScalaKryo kryo) {
		log.debug("Instantiating and configuring Scala Kryo start");
		Utils.configureScalaKryo(kryo);
		log.debug("Instantiating and configuring Scala Kryo end");
	}

}
