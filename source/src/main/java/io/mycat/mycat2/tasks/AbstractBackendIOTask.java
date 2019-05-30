package io.mycat.mycat2.tasks;

import java.io.IOException;
import java.nio.channels.SelectionKey;

import io.mycat.mysql.ComQueryState;
import io.mycat.mysql.MySQLPacketInf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.AbstractMySQLSession;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mysql.packet.ErrorPacket;
import io.mycat.proxy.NIOHandler;
import io.mycat.proxy.ProxyBuffer;

/**
 * 默认抽象类
 * 
 * @author wuzhihui
 *
 */
public abstract class AbstractBackendIOTask<T extends AbstractMySQLSession> implements NIOHandler<T> {
	protected final static Logger logger = LoggerFactory.getLogger(AbstractBackendIOTask.class);
	protected AsynTaskCallBack<T> callBack;
	protected T session;
	protected ProxyBuffer prevProxyBuffer;
	protected ErrorPacket errPkg;
	protected boolean useNewBuffer = false;

	public AbstractBackendIOTask(T session, boolean useNewBuffer) {
		setSession(session, useNewBuffer);
	}

	public void setSession(T session, boolean useNewBuffer) {
		setSession(session, useNewBuffer, true);
	}

	public void setSession(T session, boolean useNewBuffer, boolean useNewCurHandler) {
		this.useNewBuffer = useNewBuffer;
		if (useNewBuffer) {
			MySQLPacketInf curPacketInf = session.curPacketInf;
//			if (!curPacketInf.isSingleProxyBuffer()){
//				throw new RuntimeException("!curPacketInf.isSingleProxyBuffer()");
//			}
			prevProxyBuffer = curPacketInf.getProxyBuffer();
			session.curPacketInf.setProxyBuffer( curPacketInf.allocNewProxyBuffer());
			session.setCurBufOwner(true);
		}
		if (session != null && useNewCurHandler) {
			this.session = session;
			session.setCurNIOHandler(this);
		}
	}

	protected void finished(boolean success) throws IOException {
		if (useNewBuffer) {
			this.session.setCurBufOwner(false);
			revertPreBuffer();
		}
		callBack.finished(session, this, success, this.errPkg);
	}

	protected void revertPreBuffer() {
		session.curPacketInf.recycleAllocedBuffer(session.curPacketInf.getProxyBuffer());
		session.curPacketInf.setProxyBuffer(this.prevProxyBuffer);
		this.prevProxyBuffer = null;
	}

	public void onConnect(SelectionKey theKey, MySQLSession userSession, boolean success, String msg)
			throws IOException {
		logger.warn("not implemented onConnect event {}",this);
	}

	public void setCallback(AsynTaskCallBack<T> callBack) {
		this.callBack = callBack;

	}

	public AsynTaskCallBack<T> getCallback() {
		return this.callBack;

	}

	@Override
	public void onSocketClosed(T userSession, boolean normal) {
		logger.warn("not implemented onConnect event {}",this);
	}

	@Override
	public void onSocketWrite(T session) throws IOException {
		session.writeToChannel();

	}

	@Override
	public void onWriteFinished(T s) throws IOException {
		s.curPacketInf.reset();
		s.change2ReadOpts();
	}

}
