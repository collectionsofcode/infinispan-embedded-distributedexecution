package com.collectionsofcode.infinispan.embedded.distributedexecution;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.infinispan.Cache;
import org.infinispan.distexec.DefaultExecutorService;
import org.infinispan.distexec.DistributedTask;
import org.infinispan.distexec.DistributedTaskBuilder;
import org.infinispan.manager.DefaultCacheManager;
import org.infinispan.manager.EmbeddedCacheManager;

public class MasterNodeApp {

	public static void main(String args[]) throws Exception {
		System.out.println("Running MasterNode Application");
		System.setProperty("java.net.preferIPv4Stack", "true");

		//Initialize Cache Manager
		EmbeddedCacheManager manager = new DefaultCacheManager("infinispan-7.2-config.xml"); 
		
		//Use the cache
		Cache<String, Person> cache = manager.getCache("personCache"); 

		//Put Person Object to Cache
		for(int i = 0 ; i< 5 ; i++){
			Person person = new Person(generateId(), "Singgih "+i, randomAge());
			cache.put(person.getId(), person);
			System.out.println("Success put person "+person +" to cache");
		}

		//Running the Distributed Execution Framework
		DefaultExecutorService des = new DefaultExecutorService(cache, Executors.newFixedThreadPool(5)); 
		DistributedTaskBuilder<List<Person>> taskBuilder = des.createDistributedTaskBuilder(new AgeIncrementerDistributedCallable());
		taskBuilder.failoverPolicy(DefaultExecutorService.RANDOM_NODE_FAILOVER); 
		taskBuilder.timeout(10, TimeUnit.MINUTES);

		DistributedTask<List<Person>> distributedTask = taskBuilder.build();
		List<Future<List<Person>>> futureResults = des.submitEverywhere(distributedTask, cache.keySet().toArray());

		//Collects the result
		List<Person> results = new ArrayList<Person>();
		for(Future<List<Person>> result : futureResults){
			results.addAll(result.get());
		}
		
		for(Person person : results){
			System.out.println("Result : " +person);
		}
		
	}
	
	/**
	 * Generate Persons ID
	 * @return generated id
	 */
	private static String generateId(){
		return UUID.randomUUID().toString();
	}

	/**
	 * For the purpose of this example, we use Math.random for the persons age
	 * @return age
	 */
	private static Integer randomAge(){
		return (int) (Math.random() * 10 + 11);	
	}
}
