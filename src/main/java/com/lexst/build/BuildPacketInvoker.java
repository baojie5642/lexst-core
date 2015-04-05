/**
 * 
 */
package com.lexst.build;

import com.lexst.fixp.*;
import com.lexst.fixp.monitor.*;
import com.lexst.invoke.*;
import com.lexst.thread.*;
import com.lexst.util.host.*;

public class BuildPacketInvoker implements PacketInvoker {

	private IPacketListener listener;
	
	/**
	 * 
	 */
	public BuildPacketInvoker(IPacketListener reply) {
		super();
		this.listener = reply;
	}

	/* (non-Javadoc)
	 * @see com.lexst.invoke.PacketInvoker#invoke(com.lexst.fixp.Packet)
	 */
	@Override
	public Packet invoke(Packet packet) {
		Command cmd = packet.getCommand();
		if (cmd.isRequest()) {
			return this.apply(packet);
		} else if (cmd.isResponse()) {
			return this.reply(packet);
		}
		return null;
	}

	private Packet apply(Packet packet) {
		Packet resp = null;
		Command cmd = packet.getCommand();
		byte major = cmd.getMajor();
		byte minor = cmd.getMinor();

		if (cmd.isShutdown()) {
			shutdown(packet);
		} else if (cmd.isComeback()) {
			Launcher.getInstance().comeback();
		} else if (major == Request.NOTIFY && minor == Request.SCANHUB) {
			// start thread, scan home site
			HubCrawler crawler = new HubCrawler();
			crawler.detect(packet);
		} else if(major == Request.NOTIFY && minor == Request.TRANSFER_HUB) {
			this.transfer(packet);
		}

		return resp;
	}
	
	private Packet reply(Packet packet) {
		Packet reply = null;
		Command cmd = packet.getCommand();
		short code = cmd.getResponse();
		
		switch (code) {
		case Response.ISEE:
			Launcher.getInstance().refreshEndTime();
			break;
		case Response.NOTLOGIN:
			Launcher.getInstance().setOperate(BasicLauncher.LOGIN);
			break;
		}
		
		return reply;
	}
	
	private void shutdown(Packet request) {
		SocketHost remote = request.getRemote();
		for (int i = 0; i < 3; i++) {
			Command cmd = new Command(Response.OKAY);
			Packet resp = new Packet(cmd);
			resp.addMessage(Key.SPEAK, "see you next time!");
			// send shutdown reply
			listener.send(remote, resp);
		}
		// stop launcher
		Launcher.getInstance().stop();
	}
	
	private void transfer(Packet packet) {
		String server_address = packet.findChar(Key.LOCAL_ADDRESS);
		if (server_address == null)	return;

		try {
			SiteHost host = new SiteHost(server_address);
			Launcher.getInstance().setHub(host);
			Launcher.getInstance().setOperate(BasicLauncher.LOGIN);
		} catch (Throwable exp) {
			return;
		}
	}
}