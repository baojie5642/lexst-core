/**
 * 
 */
package com.lexst.sql;

import java.util.*;


public class NamingHost {
	
	private String naming;
	
	private List<String> array;

	/**
	 * 
	 */
	public NamingHost() {
		super();
		array = new ArrayList<String>();
	}
	
	public NamingHost(String naming) {
		this();
		this.setNaming(naming);
	}

	public void setNaming(String s) {
		this.naming = s;
	}

	public String getNaming() {
		return this.naming;
	}
	
	public boolean addIP(String ip) {
		return array.add(ip);
	}
	
	public boolean addAllIP(Collection<String> all) {
		return array.addAll(all);
	}

	public List<String> listIP() {
		return array;
	}
	
	public String[] getAllHost() {
		int size = array.size();
		if (size == 0) return null;
		String[] s = new String[size];
		return array.toArray(s);
	}

	public boolean containsIP(String ip) {
		return array.contains(ip);
	}
}
