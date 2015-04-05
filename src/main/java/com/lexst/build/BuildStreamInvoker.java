/**
 * 
 */
package com.lexst.build;

import java.io.*;

import com.lexst.fixp.*;
import com.lexst.invoke.*;
import com.lexst.util.host.*;

public class BuildStreamInvoker implements StreamInvoker {

	/**
	 * 
	 */
	public BuildStreamInvoker() {
		super();
	}
	
	/**
	 * build response packet
	 * @param code
	 * @return
	 */
	private Stream buildResp(SocketHost remote, short code) {
		Command cmd = new Command(code);
		return new Stream(remote, cmd);
	}
	
	/**
	 * @param reply
	 * @param resp
	 * @throws java.io.IOException
	 */
	private void flush(Stream reply, OutputStream resp) throws IOException {
		byte[] b = reply.build();
		resp.write(b, 0, b.length);
		resp.flush();
	}

	/* (non-Javadoc)
	 * @see com.lexst.invoke.StreamInvoker#invoke(com.lexst.fixp.Stream, java.io.OutputStream)
	 */
	@Override
	public void invoke(Stream request, OutputStream resp) throws IOException {
		Command cmd = request.getCommand();
		byte major = cmd.getMajor();
		byte minor = cmd.getMinor();
		
		Stream reply = null;
		if (major == Request.DATA && minor == Request.DOWNLOAD_CHUNK) {
			// upload chunk to data site
			Launcher.getInstance().upload(request, resp);
			return;
		}

		if (reply == null) {
			reply = buildResp(request.getRemote(), Response.UNSUPPORT);
		}
		this.flush(reply, resp);
	}

}