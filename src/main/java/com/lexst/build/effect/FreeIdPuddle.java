/**
 * 
 */
package com.lexst.build.effect;

import java.util.*;

import org.w3c.dom.*;

import com.lexst.util.effect.*;


public class FreeIdPuddle extends Effect {
	
	public final static String filename = "chunkid.xml";

	// chunkid set
	private LinkedList<Long> array;
	
	/**
	 * default consturct
	 */
	public FreeIdPuddle() {
		super();
		array = new LinkedList<Long>();
	}
	
	public boolean isEmpty() {
		return array.isEmpty();
	}
	
	public int size() {
		return array.size();
	}
	

	/**
	 * poll a chunk identity
	 * @return
	 */
	public long poll() {
		super.lockSingle();
		try {
			if (!array.isEmpty()) {
				return array.poll();
			}
		} catch (Throwable exp) {

		} finally {
			super.unlockSingle();
		}
		return 0L;
	}
	
	public long[] poll(int num) {
		long[] chunkIds = null;
	
		super.lockSingle();
		try {
			int size = array.size();
			if (size == 0) return null;
			if (num > size) num = size;
			chunkIds = new long[num];
			for (int i = 0; i < num; i++) {
				chunkIds[i] = array.poll();
			}
		} catch (Throwable exp) {

		} finally {
			super.unlockSingle();
		}
		return chunkIds;
	}
	
	public long[] pollAll() {
		long[] chunkIds = null;
		super.lockSingle();
		try {
			int size = array.size();
			if (size > 0) {
				chunkIds = new long[size];
				for (int i = 0; i < size; i++) {
					chunkIds[i] = array.get(i);
				}
				array.clear();
			}
		} catch (Throwable exp) {
			
		} finally{
			super.unlockSingle();
		}
		return chunkIds;
	}
	
	public boolean add(long chunkId) {
		super.lockSingle();
		try {
			if (chunkId != 0L && !array.contains(chunkId)) {
				array.add(chunkId);
				return true;
			}
		} catch (Throwable exp) {

		} finally {
			super.unlockSingle();
		}
		return false;
	}

	public int add(long[] ids) {
		int count = 0;
		for (int i = 0; ids != null && i < ids.length; i++) {
			if(add(ids[i])) count++;
		}
		return count;
	}
	
	public long[] apply(int num) {
		if(num < 1) return null;
		super.lockSingle();
		try {
			if(array.isEmpty()) return null;
			int size = (array.size() > num ? num : array.size());
			long[] values = new long[size];
			for (int i = 0; i < size; i++) {
				values[i] = array.poll();
			}
			return values;
		} catch (Throwable exp) {

		} finally {
			super.unlockSingle();
		}
		return null;
	}
	
	public boolean exists(long chunkId) {
		return array.contains(chunkId);
	}

	public byte[] buildXML() {
		StringBuilder buff = new StringBuilder(10240);
		while (!array.isEmpty()) {
			long chunkId = array.poll();
			String s = element("chunkid", chunkId);
			buff.append(s);
		}
		String body = element("app", buff.toString());
		return toUTF8(Effect.xmlHead + body);
	}

	public boolean parseXML(byte[] bytes) {
		com.lexst.xml.XMLocal xml = new com.lexst.xml.XMLocal();
		Document doc = xml.loadXMLSource(bytes);
		if(doc == null) {
			return false;
		}
		
		NodeList list =	doc.getElementsByTagName("chunkid");
		int len = list.getLength();
		for(int i = 0; i <len; i++) {
			Element elem = (Element) list.item(i);
			String s = elem.getTextContent();
			long chunkId = Long.parseLong(s);
			array.add(chunkId);
		}
		return true;
	}
}
