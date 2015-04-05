/**
 * 
 */
package com.lexst.remote.client.build;

import java.io.*;
import java.lang.reflect.*;

import com.lexst.db.chunk.*;
import com.lexst.db.schema.*;
import com.lexst.fixp.*;
import com.lexst.remote.client.*;
import com.lexst.util.host.*;
import com.lexst.visit.*;
import com.lexst.visit.naming.build.*;


public class BuildClient extends RemoteClient implements BuildVisit {

	private static Method methodNothing;
	private static Method methodBuild;
	private static Method methodIsBuilding;
	private static Method methodFindChunkInfo;
	
	static {
		try {
			methodNothing = (BuildVisit.class).getMethod("nothing", new Class<?>[0]);
			methodBuild = (BuildVisit.class).getMethod("build", new Class<?>[]{String.class});
			methodIsBuilding = (BuildVisit.class).getMethod("isBuilding", new Class<?>[] { String.class, String.class });
			methodFindChunkInfo = (BuildVisit.class).getMethod("findChunkInfo", new Class<?>[] { String.class, String.class });
		} catch (NoSuchMethodException exp) {
			throw new NoSuchMethodError("stub class initialization failed");
		}
	}
	
	/**
	 * 
	 */
	public BuildClient(boolean stream) {
		super(stream, BuildVisit.class.getName());
	}

	/**
	 * @param host
	 */
	public BuildClient(boolean stream, SocketHost host) {
		this(stream);
		this.setRemote(host);
	}
	
	/* (non-Javadoc)
	 * @see com.lexst.visit.Visit#nothing()
	 */
	@Override
	public void nothing() throws VisitException {
		this.refreshTime();
		super.invoke(BuildClient.methodNothing, null);
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.build.BuildVisit#build(java.lang.String)
	 */
	@Override
	public boolean build(String naming) throws VisitException {
		this.refreshTime();
		Object[] params = new Object[] { naming };
		Object param = super.invoke(BuildClient.methodBuild, params);
		return ((Boolean) param).booleanValue();
	}
	
	/**
	 * 
	 * @param space
	 * @return
	 * @throws VisitException
	 */
	public boolean isBuilding(Space space) throws VisitException {
		return isBuilding(space.getSchema(), space.getTable());
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.lexst.visit.naming.build.BuildVisit#isBuilding(java.lang.String, java.lang.String)
	 */
	@Override
	public boolean isBuilding(String db, String table) throws VisitException {
		this.refreshTime();
		Object[] params = new Object[] { db, table };
		Object param = super.invoke(BuildClient.methodIsBuilding, params);
		return ((Boolean) param).booleanValue();
	}
	
	/**
	 * 
	 * @param space
	 * @return
	 * @throws VisitException
	 */
	public Chunk[] findChunkInfo(Space space) throws VisitException {
		return findChunkInfo(space.getSchema(), space.getTable());
	}

	/* (non-Javadoc)
	 * @see com.lexst.visit.naming.build.BuildVisit#findChunkInfo(java.lang.String, java.lang.String)
	 */
	@Override
	public Chunk[] findChunkInfo(String db, String table) throws VisitException {
		this.refreshTime();
		Object[] params = new Object[] { db, table };
		Object param = super.invoke(BuildClient.methodFindChunkInfo, params);
		return (Chunk[]) param;
	}

	/**
	 * download a chunk, from build site
	 * @param space
	 * @param chunkid
	 * @return
	 * @throws java.io.IOException
	 */
	public Stream download(Space space, long chunkid, long breakpoint) throws IOException {
		Command cmd = new Command(Request.DATA, Request.DOWNLOAD_CHUNK);
		Stream request = new Stream(cmd);
		request.addMessage(new Message(Key.CHUNK_ID, chunkid));
		if (breakpoint > 0) request.addMessage(new Message(Key.CHUNK_BREAKPOINT, breakpoint));
		request.addMessage(new Message(Key.SCHEMA, space.getSchema()));
		request.addMessage(new Message(Key.TABLE, space.getTable()));
		return super.executeStream(request, false);
	}
}