package com.wjd.mr.RPC;

import java.io.IOException;

import org.apache.hadoop.ipc.VersionedProtocol;

interface ClientProtocol extends org.apache.hadoop.ipc.VersionedProtocol {
	// 版本号，默认情况下，不同版本号的RPC Client和Server之间不能相互通信
	public static final long versionID = 123456L;

	String echo(String value) throws IOException;

	int add(int v1, int v2) throws IOException;
}
