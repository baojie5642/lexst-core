/**
 * 
 */
package com.lexst.visit.impl.build;

import com.lexst.build.*;
import com.lexst.db.chunk.*;
import com.lexst.db.schema.*;
import com.lexst.visit.*;
import com.lexst.visit.naming.build.*;

public class BuildVisitImpl implements BuildVisit {

	/**
	 * 
	 */
	public BuildVisitImpl() {
		super();
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.Visit#nothing()
	 */
	@Override
	public void nothing() throws VisitException {
		Launcher.getInstance().nothing();
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.build.BuildVisit#build(java.lang.String)
	 */
	@Override
	public boolean build(String naming) throws VisitException {
		return Launcher.getInstance().execute(naming);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.visit.naming.build.BuildVisit#isBuilding(java.lang.String, java.lang.String)
	 */
	@Override
	public boolean isBuilding(String db, String table) throws VisitException {
		Space space = new Space(db, table);
		return Launcher.getInstance().isBuilding(space);
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.build.BuildVisit#findChunkInfo(java.lang.String, java.lang.String)
	 */
	@Override
	public Chunk[] findChunkInfo(String db, String table) throws VisitException {
		Space space = new Space(db, table);
		return Launcher.getInstance().findChunkInfo(space);
	}

}