/**
 * 
 */
package com.lexst.build;

import java.io.*;
import java.util.*;

import org.w3c.dom.*;

import com.lexst.algorithm.TaskEventListener;
import com.lexst.algorithm.marshaleduce.*;
import com.lexst.build.effect.*;
import com.lexst.data.*;
import com.lexst.data.effect.*;
import com.lexst.db.chunk.*;
import com.lexst.db.schema.*;
import com.lexst.fixp.*;
import com.lexst.log.client.*;
import com.lexst.remote.client.build.*;
import com.lexst.remote.client.home.*;
import com.lexst.site.build.*;
import com.lexst.thread.*;
import com.lexst.util.datetime.*;
import com.lexst.util.host.*;
import com.lexst.util.naming.*;
import com.lexst.visit.*;
import com.lexst.visit.impl.build.*;
import com.lexst.xml.*;
import com.lexst.util.lock.MutexLock;

public class Launcher extends JobLauncher implements BuildInbox, TaskEventListener {
	
	private static Launcher selfHandle = new Launcher();
	
	private MutexLock lock = new MutexLock();
	
	// local site
	private BuildSite local = new BuildSite();
	
//	/* task naming -> task project */
//	private Map<Naming, Project> mapProject = new TreeMap<Naming, Project>();

	// task naming -> task object
	private Map<Naming, BuildTask> mapTask = new HashMap<Naming, BuildTask>();

	// node store directory
	private String nodePath;

	private FreeIdPuddle puddle = new FreeIdPuddle();
		
	/**
	 * 
	 */
	private Launcher() {
		super();
		super.setExitVM(true);
		streamImpl = new BuildStreamInvoker();
		packetImpl = new BuildPacketInvoker(fixpPacket);
	}
	
	/**
	 * @return
	 */
	public static Launcher getInstance() {
		return Launcher.selfHandle;
	}
	
	/* (non-Javadoc)
	 * @see com.lexst.build.task.BuildInbox#getHome()
	 */
	@Override
	public SiteHost getHome() {
		return home;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.build.task.BuildInbox#getLocal()
	 */
	@Override
	public BuildSite getLocal() {
		return this.local;
	}
	
	/*
	 * (non-Javadoc)
	 * @see com.lexst.build.task.BuildInbox#setLogin(boolean)
	 */
	@Override
	public void setLogin(boolean b) {
		this.setOperate(BasicLauncher.RELOGIN);
	}

	/*
	 * (non-Javadoc)
	 * @see com.lexst.build.task.BuildInbox#removeTask(com.lexst.util.naming.Naming)
	 */
	@Override
	public boolean removeTask(Naming naming) {
		lock.lockSingle();
		try {
			return mapTask.remove(naming) != null;
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			lock.unlockSingle();
		}
		return false;
	}

	/**
	 * empty call
	 */
	public void nothing() {
		
	}
	
	/**
	 * re-build data
	 * @param naming
	 * @return
	 */
	public boolean execute(String naming) {
		Logger.info("Launcher.execute, naming is %s", naming);
		
		Naming s = new Naming(naming);
//		Project project = mapProject.get(s);
//		if(project == null) {
//			Logger.error("Launcher.execute, cannot find naming task '%s'", naming);
//			return false;
//		}
//		
//		BuildTask task = null;
//		lock.lockSingle();
//		try {
//			if (!mapTask.containsKey(s)) {
//				task = (BuildTask) Class.forName(project.getTaskClass()).newInstance();
//				task.setProject(project);
//				task.setInbox(this);
//				mapTask.put(s, task);
//			}
//		} catch (InstantiationException exp) {
//			Logger.error(exp);
//		} catch (IllegalAccessException exp) {
//			Logger.error(exp);
//		} catch (ClassNotFoundException exp) {
//			Logger.error(exp);
//		} catch (Throwable exp) {
//			Logger.fatal(exp);
//		} finally {
//			lock.unlockSingle();
//		}
		
		BuildTask task = BuildTaskPool.getInstance().find(s);		
		boolean success = (task != null);
		if (success) {
			task.setInbox(this);
			lock.lockSingle();
			try {
				mapTask.put(s, task);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			} finally {
				lock.unlockSingle();
			}
			success = task.convert();
			if (!success) {
				lock.lockSingle();
				try {
					mapTask.remove(new Naming(naming));
				} catch (Throwable exp) {
					Logger.fatal(exp);
				} finally {
					lock.unlockSingle();
				}
			}
		}
		return success;
		
//		if (task == null) return false;
//		Naming naming = new Naming(name);
//		task.setNaming(naming);
//		task.setResource(resource);
//		mapTask.put(naming, task);
		
//		Naming naming = new Naming(naming);
//		BuildTask task = mapTask.get(naming);
//		if (task != null) {
//			return task.convert();
//		}
//		return false;
	}

	/**
	 * check space
	 * @param space
	 * @return
	 */
	public boolean isBuilding(Space space) {
		Logger.info("Launcher.isBuilding, space is '%s'", space);
		
		lock.lockMulti();
		try {
			for (Naming naming : mapTask.keySet()) {
				BuildTask task = mapTask.get(naming);
				Project project = task.getProject();
				Table table = project.getTable(space);
				if (table != null) return true;
			}
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			lock.unlockMulti();
		}
		return false;
	}

	/**
	 * call jni, get chunk information 
	 * @param space
	 * @return
	 */
	public Chunk[] findChunkInfo(Space space) {
		byte[] db = space.getSchema().getBytes();
		byte[] table = space.getTable().getBytes();

		// call jni, get all chunkid
		long[] chunkIds = Install.getChunkIds(db, table);
		if (chunkIds == null || chunkIds.length == 0) {
			Logger.warning("Launcher.findChunkInfo, cannot find '%s' chunk", space);
			return null;
		}
		// get chunk information
		ArrayList<Chunk> array = new ArrayList<Chunk>();
		for (long chunkid : chunkIds) {
			// find chunk filename
			byte[] path = Install.findChunkPath(db, table, chunkid);
			if (path == null || path.length == 0) {
				Logger.warning("Launcher.findChunkInfo, cannot find '%s' - %x path", space, chunkid);
				continue;
			}
			String filename = new String(path);
			File file = new File(filename);
			if (file.exists() && file.isFile()) {
				long length = file.length();
				long modified = file.lastModified();
				Chunk info = new Chunk(chunkid, length, modified);
				array.add(info);
			}
		}
		int size = array.size();
		
		Logger.info("Launcher.findChunkInfo, '%s' chunk size %d", space, size);
		
		if (size == 0) return null;
		Chunk[] infos = new Chunk[size];
		return array.toArray(infos);
	}

	/**
	 * @param client
	 */
	private boolean loadTime(HomeClient client) {
		boolean nullable = (client == null);
		if (nullable) client = bring(home);
		if (client == null) return false;

		boolean success = false;
		for (int i = 0; i < 3; i++) {
			try {
				if(client.isClosed()) client.reconnect();
				long time = client.currentTime();
				Logger.info("Launcher.loadTime, set time %d", time);
				if (time != 0L) {
					int ret = SystemTime.set(time);
					success = (ret == 0);
					break;
				}
			} catch (VisitException exp) {
				Logger.error(exp);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			}
			client.close();
			this.delay(500);
		}
		if(nullable) complete(client);
		Logger.note("Launcher.loadTime", success);
		return success;
	}

	/**
	 * login to hoem site
	 * @param client
	 * @return
	 */
	private boolean login(HomeClient client) {
		Logger.info("Launcher.login, %s to %s", local.getHost(), home);
		boolean nullable = (client == null);
		if (nullable) client = bring(home);
		if (client == null) {
			Logger.error("Launcher.login, cannot connect %s", home);
			return false;
		}

		boolean success = false;
		for (int i = 0; i < 3; i++) {
			try {
				if (client.isClosed()) client.reconnect();
				success = client.login(local);
				break;
			} catch (VisitException exp) {
				Logger.error(exp);
			} catch (IOException exp) {
				Logger.error(exp);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			}
			client.close();
			this.delay(1000);
		}
		if (nullable) complete(client);
		return success;
	}
	
	/**
	 * logout from home site
	 * @param client
	 */
	private boolean logout(HomeClient client) {
		Logger.info("Launcher.logout, %s from %s", local.getHost(), home);
		
		boolean nullable = (client == null);
		if (nullable) client = bring(home);
		if (client == null) {
			Logger.error("Launcher.logout, cannot connect %s", home);
			return false;
		}
		
		boolean success = false;
		for (int i = 0; i < 3; i++) {
			try {
				if (client.isClosed()) client.reconnect();
				success = client.logout(local.getType(), local.getHost());
				break;
			} catch (IOException exp) {
				Logger.error(exp);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			}
			client.close();
			delay(1000);
		}
		if (nullable) complete(client);
		return success;
	}
	
	/**
	 * relogin site
	 * @param client
	 * @return
	 */
	private boolean relogin(HomeClient client) {
		Logger.info("Launcher.relogin, %s to %s", local.getHost(), home);
		
		boolean nullable = (client == null);
		if (nullable) client = bring(home);
		if (client == null) {
			Logger.error("Launcher.relogin, cannot connect %s", home);
			return false;
		}

		boolean success = false;
		for (int i = 0; i < 3; i++) {
			try {
				if (client.isClosed()) client.reconnect();
				success = client.relogin(local);
				break;
			} catch (IOException exp) {
				Logger.error(exp);
			} catch (Throwable exp) {
				Logger.fatal(exp);
			}
			client.close();
			this.delay(5000);
		}
		if (nullable) complete(client);
		return success;
	}

	/**
	 * start jni
	 */
	private boolean loadJNI() {
		int ret = Install.launch();
		return (ret == 0);
	}

	/**
	 * stop jni
	 */
	private void stopJNI() {
		// flush to disk
		long[] chunkIds = Install.getFreeChunkIds();
		for (int i = 0; chunkIds != null && i < chunkIds.length; i++) {
			puddle.add(chunkIds[i]);
		}
		this.flushIdentity();
		// stop jni
		Install.stop();
	}
	
	/**
	 * load pool service
	 * 
	 * @return
	 */
	private boolean loadPool() {
		Logger.info("Launcher.loadPool, loading ...");
		BuildTaskPool.getInstance().setTaskEventListener(this);
		return BuildTaskPool.getInstance().start();
	}

	/**
	 * stop pool service
	 */
	private void stopPool() {
		Logger.info("Launcher.stopPool, stop all...");
		BuildTaskPool.getInstance().stop();
		while (BuildTaskPool.getInstance().isRunning()) {
			this.delay(200);
		}
	}
	
	private boolean loadTable(HomeClient client) {
		boolean success = false;
		boolean nullable = (client == null);
		try {
			if (nullable) client = bring(home);			
			for (Naming naming : BuildTaskPool.getInstance().listNaming()) {
				Project project = BuildTaskPool.getInstance().findProject(naming);
				for (Space space : project.getSpaces()) {
					Table table = client.findTable(space);
					if (table == null) {
						Logger.error("Launcher.loadTable, cannot find table '%s'", space);
						return false;
					}
					project.setTable(space, table);
				}
			}
			success = true;
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			if (nullable) complete(client);
		}
		return success;
	}

//	/**
//	 * start build task and jni service
//	 * @param client
//	 * @return
//	 */
//	private boolean loadTask() {
//		for (Naming naming : mapTask.keySet()) {
//			BuildTask task = mapTask.get(naming);
//			task.setInbox(this);
//			// start task
//			boolean success = task.start();
//			Logger.note(success, "Launcher.loadTask, load task '%s'", naming);
//		}
//		return true;
//	}

//	/**
//	 * check BuildTask, reload it
//	 */
//	private void reloadTasks() {
//		int count = 0;
//		for(Naming naming : mapTask.keySet()) {
//			BuildTask task = mapTask.get(naming);
//			if(task.isRunning()) continue;
//			// re-start task
//			boolean success = task.start();
//			Logger.note(success, "Launcher.reloadTask, load task '%s'", naming);
//			if (success) count++;
//		}
//		// when reload success, login to home
//		if (count > 0) {
//			setLogin(true);
//		}
//	}

	/**
	 * stop build task and jni service
	 */
	private void stopTask() {
		// stop task
		lock.lockSingle();
		try {
			for (BuildTask task : mapTask.values()) {
				task.halt();
			}
		} catch (Throwable exp) {
			Logger.fatal(exp);
		} finally {
			lock.unlockSingle();
		}
		
		// wait...
		while(!mapTask.isEmpty()) {
			this.delay(500);
		}
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#init()
	 */
	@Override
	public boolean init() {
		// slave site
		Install.setRank(BuildSite.SLAVE_SITE);
		// connect to home site
		HomeClient client = bring(home);
		if (client == null) return false;
		//1. load log
		boolean	success = loadLog(local.getType(), client);
		Logger.note("Launcher.init, load log", success);
		//2. get refresh time
		if(success) {
			success = loadTimeout(local.getType(), client);
			Logger.note(success, "Launcher.init, set site timeout %d", getSiteTimeout());
			if(!success) stopLog();
		}
		//3. update system time
		if(success) {
			loadTime(client);
		}
		//4. load listen
		if (success) {
			Class<?>[] cls = { BuildVisitImpl.class };
			success = loadListen(cls, local.getHost());
			Logger.note("Launcher.init, load listen", success);
			if (!success) stopLog();
		}
		
		// 5. load pool
		if (success) {
			success = loadPool();
			Logger.note("Launcher.init, load pool", success);
			if (!success) stopLog();
		}
		
		//6. load table
		if(success) {
			success = loadTable(client);
			Logger.note("Launcher.init, load table", success);
			if(!success) {
				stopListen();
				stopLog();
				stopPool();
			}
		}
		//7. load jni service
		if(success) {
			success = loadJNI();
			Logger.note("Launcher.init, load JNI", success);
			if (!success) {
				stopListen();
				stopLog();
			}
		}
		//8. login to home
		if (success) {
			success = login(client);
			Logger.note("Launcher.init, login", success);
			if (!success) {
				stopTask();
				stopJNI();
				stopListen();
				stopLog();
			}
		}
		// close home-client
		complete(client);
		
//		local.add( BuildTaskPool.getInstance().listNaming() );
		
		return success;
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#process()
	 */
	@Override
	public void process() {
		Logger.info("Launcher.process, site into...");
		long time2 = System.currentTimeMillis() + 2000;

		while (!isInterrupted()) {
			long end = System.currentTimeMillis() + 1000;
			
			// when site timeout or relogin is true
			if (super.isLoginOperate()) {
				this.setOperate(BasicLauncher.NONE);
				this.refreshEndTime();
				this.login(null);
			} else if(super.isReloginOperate()) {
				this.setOperate(BasicLauncher.NONE);
				this.refreshEndTime();
				this.relogin(null);
			} else if (isMaxSiteTimeout()) {
				this.refreshEndTime();
				this.relogin(null);
			} else if (this.isSiteTimeout()) {
				this.hello(local.getType(), home); // active to home
			}
			
			// count chunk id
			if (System.currentTimeMillis() >= time2) {
				checkChunkId();
				time2 += 2000;
			}

			long timeout = end - System.currentTimeMillis();
			if (timeout > 0) delay(timeout);
		}
		Logger.info("Launcher.process, site exit");
	}

	/* (non-Javadoc)
	 * @see com.lexst.thread.VirtualThread#finish()
	 */
	@Override
	public void finish() {
		// site logout
		logout(null);
		// stop all build task
		stopTask();
		// stop jni
		stopJNI();
		// stop pool
		stopPool();
		// stop listen
		stopListen();
		// stop log
		stopLog();
	}

	/* (non-Javadoc)
	 * @see com.lexst.algorithm.TaskEventListener#updateTask()
	 */
	@Override
	public void updateTask() {
		setOperate(BasicLauncher.RELOGIN);
	}

	/**
	 * check chunk identity
	 */
	private void checkChunkId() {
		int size = Install.countFreeChunkIds();
		if (size >= 5) return;
		// when missing, download from home site
		long[] allkey = applyChunkIds(50);
		// save to jni
		for (int i = 0; allkey != null && i < allkey.length; i++) {
			Install.addChunkId(allkey[i]);
		}
	}
	

	private long[] applyChunkIds(int num) {
		long[] chunkIds = puddle.poll(num);
		if (chunkIds != null && chunkIds.length >= num) {
			return chunkIds;
		}
		
		int left = (num - (chunkIds == null ? 0 : chunkIds.length));
		HomeClient client = bring(home);
		if (client == null) return chunkIds;
		ArrayList<Long> array = new ArrayList<Long>();
		
		for (int i = 0; chunkIds != null && i < chunkIds.length; i++) {
			array.add(chunkIds[i]);
		}
		try {
			long[] allkey = client.pullSingle(left);
			for (int i = 0; allkey != null && i < allkey.length; i++) {
				array.add(allkey[i]);
			}
		} catch (VisitException exp) {
			Logger.error(exp);
		} catch (Throwable exp) {
			Logger.fatal(exp);
		}
		// exit and close
		this.complete(client);
		
		// sort
		Collections.sort(array);
		chunkIds = new long[array.size()];
		for (int i = 0; i < array.size(); i++) {
			chunkIds[i] = array.get(i);
		}
		return chunkIds;
	}
	
	/**
	 * upload chunk to other site
	 * @param request
	 * @param resp
	 * @return
	 * @throws java.io.IOException
	 */
	public boolean upload(Stream request, OutputStream resp) throws IOException {
		// chunk identity
		Message msg = request.findMessage(Key.CHUNK_ID);
		if (msg == null) return false;
		long chunkId = msg.longValue();
		// chunk resume breakpoint
		long breakpoint = 0L;
		msg = request.findMessage(Key.CHUNK_BREAKPOINT);
		if(msg != null) breakpoint = msg.longValue();
		// space
		msg = request.findMessage(Key.SCHEMA);
		if (msg == null) return false;
		String db = msg.stringValue();
		msg = request.findMessage(Key.TABLE);
		if (msg == null) return false;
		String table = msg.stringValue();
		Space space = new Space(db, table);
		Logger.info("Launcher.upload, space:'%s' chunkid:%x  breakpoint:%d", space, chunkId, breakpoint);

		// find chnunk and send to remote site
		BuildUploader uploader = new BuildUploader();
		byte[] b = Install.findChunkPath(db.getBytes(), table.getBytes(), chunkId);
		// when error, send a null
		if (b == null || b.length == 0) {
			Logger.error("Launcher.upload, cannot find '%s' - %x path", space, chunkId);
			uploader.execute(space, chunkId, breakpoint, null, resp);
			return false;
		}
		String filename = new String(b);
		boolean success = uploader.execute(space, chunkId, breakpoint, filename, resp);
		Logger.note(success, "Launcher.upload, send '%s' to %s", filename, request.getRemote());
		return success;
	}
	
	/**
	 * create local directory
	 * @param s
	 * @return
	 */
	private boolean createPath(String s) {
		char c = File.separatorChar;
		s = s.replace('\\', c);
		s = s.replace('/', c);
		if (s.charAt(s.length() - 1) != c) s += c;
		File dir = new File(nodePath = s);
		// create directory
		if (dir.exists() && dir.isDirectory()) {
			return true;
		}
		return dir.mkdirs();
	}
	
	private File buildFile(String filename) {
		String path = String.format("%s%s%s", nodePath, File.separator, filename);
		return new File(path);
	}
	
	private boolean loadIdentity() {
		File file = buildFile(IdentityPuddle.filename);
		if(!file.exists()) return true;
		byte[] data = readFile(file);
		if(data == null) return false;
		puddle.parseXML(data);
		// flush chunk id to jni
		long[] chunkIds = puddle.pollAll();
		for (int i = 0; chunkIds != null && i < chunkIds.length; i++) {
			Install.addChunkId(chunkIds[i]);
		}
		return true;
	}
	
	private boolean flushIdentity() {
		long[] chunkIds = Install.getFreeChunkIds();
		Logger.info("Launcher.flushIdentity, chunk identity count %d", (chunkIds == null ? -1 : chunkIds.length));
		for (int i = 0; chunkIds != null && i < chunkIds.length; i++) {
			puddle.add(chunkIds[i]);
		}
		byte[] data = puddle.buildXML();
		// flush to disk
		File file = buildFile(IdentityPuddle.filename);
		return flushFile(file, data);
	}

//	private boolean splitTask(String filename) {
//		XMLocal xml = new XMLocal();
//		Document doc = xml.loadXMLSource(filename);
//		if (doc == null) {
//			return false;
//		}
//		Element elem = (Element) doc.getElementsByTagName("chunk-path").item(0);
//		String build = xml.getValue(elem, "build");
//		String cache = xml.getValue(elem, "cache");
//		String[] paths = xml.getXMLValues(elem.getElementsByTagName("store"));
//		int ret = Install.setBuildRoot(build.getBytes());
//		Logger.note(ret == 0, "Launcher.splitTask, load build path %s", build);
//		if (ret != 0) return false;
//		ret = Install.setCacheRoot(cache.getBytes());
//		Logger.note(ret == 0, "Launcher.splitTask, load cache path %s", cache);
//		if (ret != 0) return false;
//		for (String path : paths) {
//			ret = Install.setChunkRoot(path.getBytes());
//			Logger.note(ret == 0, "Launcher.splitTask, load store path %s", path);
//			if (ret != 0) return false;
//		}
//
//		NodeList list =	doc.getElementsByTagName("task");
//		int size = list.getLength();
//		for (int i = 0; i < size; i++) {
//			elem = (Element) list.item(i);
//			// task naming
//			String name = xml.getValue(elem, "naming");
//			// project class name
//			String project_class = xml.getValue(elem, "project-class");
//			// task class name
//			String task_class = xml.getValue(elem, "task-class");
//			// resource
//			String resource = xml.getValue(elem, "resource");
//
//			boolean success = false;
//			try {
//				Naming naming = new Naming(name);
//				if(!mapProject.containsKey(naming)) {
//					Project project = (Project)Class.forName(project_class).newInstance();
//					project.setNaming(naming);
//					project.setTaskClass(task_class);
//					project.setResource(resource);
//					mapProject.put(naming, project);
//					success = true;
//				}
//			} catch (InstantiationException exp) {
//				Logger.error(exp);
//			} catch (IllegalAccessException exp) {
//				Logger.error(exp);
//			} catch (ClassNotFoundException exp) {
//				Logger.error(exp);
//			}
//			Logger.note(success, "Launcher.splitTask, load '%s' - '%s' - '%s' - '%s'",
//					name, project_class, task_class, resource);			
//
//			if(!success) return false;
//		}
//		return true;
//	}
	
	private boolean loadLocal(final String filename) {
		XMLocal xml = new XMLocal();
		Document document = xml.loadXMLSource(filename);
		if (document == null) {
			return false;
		}
		// home host
		SiteHost host = splitHome(document);
		home.set(host);
		// local host
		host = splitLocal(document);
		local.setHost(host);
		
		// create configure path
		String path = xml.getXMLValue(document.getElementsByTagName("data-path"));
		if(!createPath(path)) {
			Logger.error("Launcher.loadLocal, cannot create directory %s", path);
			return false;
		}
		
		// resovle security configure file
		if(!super.loadSecurity(document)) {
			Logger.error("Launcher.loadLocal, cannot parse security file");
			return false;
		}
		
		// split shutdown
		boolean success = loadShutdown(document);
		Logger.note("Launcher.loadLocal, split shutdown", success);
		if(success) { 
//			String s = xml.getXMLValue(document.getElementsByTagName("task-file"));
//			Logger.info("Launcher.loadLocal, task filename %s", s);
//			success = splitTask(s);
			
			String s = xml.getXMLValue(document.getElementsByTagName("task-root"));
			Logger.info("Launcher.loadLocal, task filename %s", s);
			if (s != null && s.length() > 0) {
				BuildTaskPool.getInstance().setRoot(s);
			}
		}
		Logger.info("Launcher.loadLocal, split task", success);
		// load log
		if(success) {
			success = Logger.loadXML(filename);	
		}
		Logger.note("Launcher.loadLocal, load log", success);
		return success;
	}

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args == null || args.length < 1) {
			Logger.error("parameters missing!");
			Logger.gushing();
			return;
		}
		// init jni store library
		int ret = Install.initialize();
		if(ret != 0) {
			Logger.error("initialize failed, program will exit!");
			Logger.gushing();
			return;
		}
		// load local resource
		String filename = args[0];
		boolean success = Launcher.getInstance().loadLocal(filename);
		Logger.note("Launcher.main, load local", success);
		// load chunk identity
		if(success) {
			success = Launcher.getInstance().loadIdentity();
			Logger.note("Launcher.main, load identity", success);
		}
		// start job
		if (success) {
			success = Launcher.getInstance().start();
			Logger.note("Launcher.main, start service", success);
		}
		if(!success) {
			Logger.gushing();
		}
	}
}