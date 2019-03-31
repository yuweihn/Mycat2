package io.mycat.mycat2.cmds;

import java.io.IOException;

import io.mycat.mysql.ComQueryState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.MycatSession;

public class ComQuitCmd implements MySQLCommand {

	private static final Logger logger = LoggerFactory.getLogger(ComQuitCmd.class);

	public static final ComQuitCmd INSTANCE = new ComQuitCmd();

	@Override
	public boolean procssSQL(MycatSession session) throws IOException {
		return true;
	}

	@Override
	public boolean onBackendResponse(MySQLSession session) throws IOException {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean onBackendClosed(MySQLSession session, boolean normal) throws IOException {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean onFrontWriteFinished(MycatSession session) throws IOException {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public boolean onBackendWriteFinished(MySQLSession session) throws IOException {
		// TODO Auto-generated method stub
		return true;
	}

	@Override
	public void clearResouces(MycatSession session, boolean sessionCLosed) {
		if (sessionCLosed) {
			session.unbindBackends();
			session.close(true, "client closed");
		}
	}

}
