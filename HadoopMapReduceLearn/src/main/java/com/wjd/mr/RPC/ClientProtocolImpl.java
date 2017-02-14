package com.wjd.mr.RPC;

import java.io.IOException;

import org.apache.hadoop.ipc.ProtocolSignature;

public class ClientProtocolImpl implements ClientProtocol {

	// 重载的方法，用于获取自定义的协议版本号，
	public long getProtocolVersion(String protocol, long clientVersion) {
		return ClientProtocol.versionID;
	}

	// 重载的方法，用于获取协议签名
	public ProtocolSignature getProtocolSignature(String protocol, long clientVersion, int hashcode) {
		return new ProtocolSignature(ClientProtocol.versionID, null);
	}

	public String echo(String value) throws IOException {
		return value;
	}

	public int add(int v1, int v2) throws IOException {
		return v1 + v2;
	}

}
