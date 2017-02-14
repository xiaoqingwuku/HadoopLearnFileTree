package com.wjd.mr.RPC;



import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

public class Client {
	public static void main(String[] args) throws Exception {
		//System.setProperty("hadoop.home.dir", "C:\\hadoop-common-2.2.0-bin-master");
		ClientProtocol proxy = (ClientProtocol) RPC.getProxy(ClientProtocol.class, conf.version,
				new InetSocketAddress(conf.ADDR, conf.PORT), new Configuration());
		int result = proxy.add(5, 6);
		System.out.println("===========result==========="+result);
		String echoResult = proxy.echo("result");
		System.out.println("===========result==========="+echoResult);
	}
}
