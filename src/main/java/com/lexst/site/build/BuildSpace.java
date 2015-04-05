/**
 * 
 */
package com.lexst.site.build;

import java.io.*;
import java.util.*;

import com.lexst.db.schema.*;
import com.lexst.util.naming.*;
import com.lexst.db.chunk.*;

public class BuildSpace implements Serializable {
	
	private static final long serialVersionUID = 1L;
	
	/* task naming */
	private Naming naming;
	
	private Map<Space, ChunkIdentitySet> mapSpace = new TreeMap<Space, ChunkIdentitySet>();

	/**
	 * 
	 */
	public BuildSpace() {
		super();
	}

	/**
	 * @param naming
	 */
	public BuildSpace(Naming naming) {
		this();
		this.setNaming(naming);
	}
	
	public void setNaming(Naming s) {
		this.naming = new Naming(s);
	}

	public Naming getNaming() {
		return this.naming;
	}	

	public boolean isEmpty() {
		return mapSpace.isEmpty();
	}

	public int size() {
		return mapSpace.size();
	}
	
	public Set<Space> keySet() {
		return mapSpace.keySet();
	}

	public boolean exists(Space space) {
		return mapSpace.get(space) != null;
	}
	
	public boolean exists(Space space, long chunkId) {
		ChunkIdentitySet array = mapSpace.get(space);
		if (array != null) {
			return array.exists(chunkId);
		}
		return false;
	}
	
	public ChunkIdentitySet find(Space space) {
		return mapSpace.get(space);
	}

	public boolean add(Space space) {
		ChunkIdentitySet set = mapSpace.get(space);
		if (set != null) {
			return false;
		}
		set = new ChunkIdentitySet();
		return mapSpace.put(space, set) == null;
	}
	
	public boolean add(Space space, long chunkId) {
		ChunkIdentitySet set = mapSpace.get(space);
		if(set == null) {
			set = new ChunkIdentitySet();
			mapSpace.put(space, set);
		}
		return set.add(chunkId);
	}
	
	public int add(Space space, long[] chunkIds) {
		ChunkIdentitySet set = mapSpace.get(space);
		if (set == null) {
			set = new ChunkIdentitySet();
			mapSpace.put(space, set);
		}
		return set.add(chunkIds);
	}

	public boolean remove(Space space) {
		ChunkIdentitySet set = mapSpace.remove(space);
		return set != null;
	}
	
	public boolean remove(Space space, long chunkid) {
		ChunkIdentitySet set = mapSpace.get(space);
		if(set == null) return false;
		boolean success = set.remove(chunkid);
		if (set.isEmpty()) mapSpace.remove(space);
		return success;
	}
	
	public int remove(Space space, long[] chunkIds) {
		int count = 0;
		ChunkIdentitySet set = mapSpace.get(space);
		if (set != null) {
			count = set.remove(chunkIds);
			if (set.isEmpty()) mapSpace.remove(space);
		}
		return count;
	}

}