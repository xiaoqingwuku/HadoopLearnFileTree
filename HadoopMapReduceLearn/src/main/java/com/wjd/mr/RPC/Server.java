package com.wjd.mr.RPC;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class Server {
	public static void main(String[] args) throws Exception {
		// hadoop 1.0
		// org.apache.hadoop.ipc.RPC.Server server =
		// org.apache.hadoop.ipc.RPC.getServer(new Operator(), conf.ADDR,
		// conf.PORT,
		// new Configuration());
		System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");
		org.apache.hadoop.ipc.RPC.Server server = new RPC.Builder(new Configuration()).setProtocol(ClientProtocol.class)
				.setInstance(new ClientProtocolImpl()).setBindAddress(conf.ADDR).setPort(conf.PORT).setNumHandlers(5).build();
		server.start();

	}
}
