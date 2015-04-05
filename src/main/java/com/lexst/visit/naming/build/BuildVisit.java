/**
 * 
 */
package com.lexst.visit.naming.build;

import com.lexst.db.chunk.*;
import com.lexst.visit.*;

public interface BuildVisit extends Visit {

	/**
	 * build a naming object
	 * @param naming
	 * @return
	 * @throws VisitException
	 */
	boolean build(String naming) throws VisitException;
	
	/**
	 * check build object
	 * @param db
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	boolean isBuilding(String db, String table) throws VisitException;
	
	/**
	 * get chunk information
	 * @param db
	 * @param table
	 * @return
	 * @throws VisitException
	 */
	Chunk[] findChunkInfo(String db, String table) throws VisitException;
	

}