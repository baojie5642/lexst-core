/**
 * 
 */
package com.lexst.site.build;

import java.util.*;

import com.lexst.db.schema.*;
import com.lexst.site.*;
import com.lexst.util.naming.*;


public class BuildSite extends RankSite {

	private static final long serialVersionUID = 1L;

	/* naming -> task space */
	private Map<Naming, BuildSpace> mapNaming = new HashMap<Naming, BuildSpace>();
	
	/**
	 * 
	 */
	public BuildSite() {
		super(Site.BUILD_SITE);
		super.setRank(RankSite.SLAVE_SITE);
	}

	public boolean isEmpty() {
		return mapNaming.isEmpty();
	}

	public int size() {
		return mapNaming.size();
	}

	/**
	 * clear all
	 */
	public void clear() {
		mapNaming.clear();
	}
	
	/**
	 * naming set
	 * @return
	 */
	public Set<Naming> keySet() {
		return mapNaming.keySet();
	}
	
	/**
	 * find task space
	 * @param naming
	 * @return
	 */
	public BuildSpace find(Naming naming) {
		return mapNaming.get(naming);
	}
	
	/**
	 * @param naming
	 * @return
	 */
	public boolean exists(Naming naming) {
		return mapNaming.get(naming) != null;
	}
	
	/**
	 * @param naming
	 * @param space
	 * @param chunkId
	 * @return
	 */
	public boolean exists(Naming naming, Space space, long chunkId) {
		BuildSpace ts = mapNaming.get(naming);
		if (ts != null) {
			return ts.exists(space, chunkId);
		}
		return false;
	}

	/**
	 * add naming object
	 * @param naming
	 * @return
	 */
	public boolean add(Naming naming) {
		BuildSpace ts = mapNaming.get(naming);
		if (ts == null) {
			ts = new BuildSpace(naming);
			return mapNaming.put(naming, ts) == null;
		}
		return false;
	}
	
	/**
	 * @param naming
	 * @param space
	 * @return
	 */
	public boolean add(Naming naming, Space space) {
		BuildSpace ts = mapNaming.get(naming);
		if (ts != null) return true;
		// save
		ts = new BuildSpace(naming);
		return mapNaming.put(naming, ts) == null;
	}

	/**
	 * @param naming
	 * @param space
	 * @param chunkId
	 * @return
	 */
	public boolean add(Naming naming, Space space, long chunkId) {
		int size = add(naming, space, new long[] { chunkId });
		return size == 1;
	}

	/**
	 * add naming object
	 * @param naming
	 * @param space
	 * @param chunkIds
	 * @return
	 */
	public int add(Naming naming, Space space, long[] chunkIds) {
		BuildSpace ts = mapNaming.get(naming);
		if(ts == null) {
			ts = new BuildSpace(naming);
			mapNaming.put(naming, ts);
		}
		return ts.add(space, chunkIds);
	}

	/**
	 * remove a naming object
	 * @param naming
	 * @return
	 */
	public boolean remove(Naming naming) {
		BuildSpace chunks = mapNaming.remove(naming);
		return chunks != null;
	}
	
	/**
	 * @param naming
	 * @param space
	 * @return
	 */
	public boolean remove(Naming naming, Space space) {
		BuildSpace bs = mapNaming.get(naming);
		if (bs == null) return false;
		return bs.remove(space);
	}

	/**
	 * @param naming
	 * @param space
	 * @param chunkIds
	 * @return
	 */
	public int remove(Naming naming, Space space, long[] chunkIds) {
		BuildSpace bs = mapNaming.get(naming);
		if (bs == null) return 0;
		return bs.remove(space, chunkIds);
	}
}