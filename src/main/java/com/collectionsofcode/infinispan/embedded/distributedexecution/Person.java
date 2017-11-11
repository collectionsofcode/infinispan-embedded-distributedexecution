package com.collectionsofcode.infinispan.embedded.distributedexecution;

import java.io.Serializable;

/**
 * @author <a href="mailto:singgih.prasetyo@sigma.co.id">Singgih Suryo Prasetyo</a> since Feb 21, 2017 10:40:41 AM
 * @version $Id: $
 * 
 */
public class Person implements Serializable{
	
	private static final long serialVersionUID = 1L;
	
	private String id;
	private String name;
	private int age;
	
	public Person(String id, String name, int age) {
		this.id = id;
		this.name = name;
		this.age = age;
	}
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public int getAge() {
		return age;
	}
	public void setAge(int age) {
		this.age = age;
	}
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("{\"id\"=\"");
		builder.append(id);
		builder.append("\", \"name\"=\"");
		builder.append(name);
		builder.append("\", \"age\"=\"");
		builder.append(age);
		builder.append("\"}");
		return builder.toString();
	}
	

}
