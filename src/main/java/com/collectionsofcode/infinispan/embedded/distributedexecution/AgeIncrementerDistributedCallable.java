package com.collectionsofcode.infinispan.embedded.distributedexecution;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.infinispan.Cache;
import org.infinispan.distexec.DistributedCallable;

/**
 * DistributedCallable class to Increment person's age by 1
 */
public class AgeIncrementerDistributedCallable implements DistributedCallable<String, Person, List<Person>>, Serializable{

	private static final long serialVersionUID = 1186795304237364116L;

	Cache<String, Person> cache;
	Set<String> inputKeys;
	
	private static final int AGE_TO_INCREMENT = 1;
	
	@Override
	public List<Person> call() throws Exception {
		
		List<Person> results = new ArrayList<Person>();
		for(String key : inputKeys){

			Person person = cache.get(key);
			int incrementedAge = person.getAge() + AGE_TO_INCREMENT;
			person.setAge(incrementedAge);
			results.add(person);
		}

		System.out.println("Total processed data on each node : " +results.size());
		
		return results;
	}

	@Override
	public void setEnvironment(Cache<String, Person> cache, Set<String> inputKeys) {
		this.cache = cache;
		this.inputKeys = inputKeys;
	}

}
