/**
 * 
 */
package com.lexst.sql;

import java.util.*;

import com.lexst.db.schema.*;

public final class SpaceHost {
	
	private Space space;
	
	private List<String> array = new ArrayList<String>();

	/**
	 * 
	 */
	public SpaceHost() {
		super();
	}
	
	/**
	 * @param s
	 */
	public SpaceHost(Space s) {
		this();
		this.setSpace(s);
	}
	
	/**
	 * @param db
	 * @param table
	 */
	public SpaceHost(String db, String table) {
		this(new Space(db, table));
	}

	public void setSpace(Space s) {
		space = new Space(s);
	}
	public Space getSpace() {
		return space;
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