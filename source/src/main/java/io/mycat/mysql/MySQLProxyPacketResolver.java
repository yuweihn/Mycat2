package io.mycat.mysql;

import io.mycat.mycat2.MySQLCommand;
import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.cmds.judge.JudgeUtil;
import io.mycat.mysql.packet.EOFPacket;
import io.mycat.mysql.packet.MySQLPacket;
import io.mycat.mysql.packet.OKPacket;
import io.mycat.proxy.ProxyBuffer;
import io.mycat.proxy.ProxyReactorThread;
import io.mycat.util.ParseUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

import static io.mycat.mycat2.MySQLCommand.COM_STMT_CLOSE;
import static io.mycat.mysql.MySQLPayloadType.*;

/*
cjw
294712221@qq.com
 */
public class MySQLProxyPacketResolver {
    private final static Logger logger = LoggerFactory.getLogger(MySQLProxyPacketResolver.class);


}
