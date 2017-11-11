package com.collectionsofcode.infinispan.embedded.distributedexecution;

import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

public class SlaveNodeApp {

	public static void main(String args[]) throws Exception {
		System.out.println("Running SlaveNode Application");
		System.setProperty("java.net.preferIPv4Stack", "true");
		
		//Initialize Cache Manager
		EmbeddedCacheManager manager = new DefaultCacheManager("infinispan-7.2-config.xml");
		
		//Use the cache
		manager.getCache("personCache");
	}
}
