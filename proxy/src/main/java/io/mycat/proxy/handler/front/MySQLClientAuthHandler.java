/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.proxy.handler.front;

import io.mycat.beans.mysql.MySQLAutoCommit;
import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.beans.mysql.MySQLPayloadWriter;
import io.mycat.beans.mysql.MySQLVersion;
import io.mycat.beans.mysql.packet.AuthPacket;
import io.mycat.beans.mysql.packet.AuthSwitchRequestPacket;
import io.mycat.beans.mysql.packet.HandshakePacket;
import io.mycat.beans.mysql.packet.MySQLPacket;
import io.mycat.config.MySQLServerCapabilityFlags;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;
import io.mycat.proxy.handler.MycatHandler;
import io.mycat.proxy.handler.NIOHandler;
import io.mycat.proxy.monitor.MycatMonitor;
import io.mycat.proxy.session.MycatSession;
import io.mycat.proxy.session.MycatSessionManager;
import io.mycat.proxy.session.MycatUser;
import io.mycat.util.CachingSha2PasswordPlugin;
import io.mycat.util.MysqlNativePasswordPluginUtil;
import io.mycat.util.StringUtil;

import java.util.Map;

import static io.mycat.beans.mysql.MySQLErrorCode.ER_ACCESS_DENIED_ERROR;

/**
 * @author jamie12221 date 2019-05-07 13:58
 *
 * 服务器发送给客户端的验证处理器
 **/
public class MySQLClientAuthHandler implements NIOHandler<MycatSession> {

    private static final MycatLogger LOGGER = MycatLoggerFactory
        .getLogger(MySQLClientAuthHandler.class);
    public byte[] seed;
    public MycatSession mycat;
    private boolean finished = false;
    private AuthPacket auth ;
    public String clientAuthPluginName = CachingSha2PasswordPlugin.PROTOCOL_PLUGIN_NAME;
    public boolean isChangeAuthPlugin = false;
    private MycatSessionManager mycatSessionManager;

    public MySQLClientAuthHandler(MycatSessionManager mycatSessionManager) {
        this.mycatSessionManager = mycatSessionManager;
    }

    public void setMycatSession(MycatSession mycatSession) {
        this.mycat = mycatSession;
    }

    @Override
    public void onSocketRead(MycatSession mycat) {
        try {
            if (mycat.getCurNIOHandler() != this) {
                return;
            }
            if (!mycat.readFromChannel()) {
                return;
            }
//            MycatSecurityConfig securityManager = runtime.getSecurityManager();
            byte[] password = new byte[]{};
            if(!isChangeAuthPlugin) {
                //密码读取与验证
                this.auth = readResponseAuthPacket(mycat);
                if (true) {
                    String authPluginName = auth.getAuthPluginName();
                    int capabilities = auth.getCapabilities();
                    //切换auth_plugin
                    if (MySQLServerCapabilityFlags.isPluginAuth(capabilities)
                            && !authPluginName.equals(clientAuthPluginName)) {
                        //发送切换包的auth_response
                        isChangeAuthPlugin = true;
                        AuthSwitchRequestPacket authSwitchRequestPacket = new AuthSwitchRequestPacket();
                        clientAuthPluginName = StringUtil.isEmpty(authPluginName)?MysqlNativePasswordPluginUtil.PROTOCOL_PLUGIN_NAME:authPluginName;
                        authSwitchRequestPacket.setAuthPluginName(clientAuthPluginName);
                        authSwitchRequestPacket.setStatus((byte)0xfe);
                        authSwitchRequestPacket.setAuthPluginData(new String(seed));

                        MySQLPayloadWriter mySQLPayloadWriter = new MySQLPayloadWriter(1024);
                        authSwitchRequestPacket.writePayload(mySQLPayloadWriter);
                        mycat.writeBytes(mySQLPayloadWriter.toByteArray(),true);
                        return;
                    }
                    //握手包中的加密密码
                    password = auth.getPassword();
                }
            }  else {
                MySQLPacket mySQLPacket = mycat.currentProxyPayload();
                password = mySQLPacket.readNULStringBytes();
            }

            String username = auth.getUsername();
            int maxPacketSize = auth.getMaxPacketSize();
            String database = auth.getDatabase();

            int characterSet = auth.getCharacterSet();
            Map<String, String> attrs = auth.getClientConnectAttrs();


            //login connect direct
//            if (!securityManager.isIgnorePassword()) {
//                String password = runtime.getSecurityManager()
//                        .getPasswordByUserName(username);
//
//                if (!checkPassword(password, input)) {
//                    String message = "password is wrong!";
//                    failture(mycat, message);
//                    return;
//                }
//            }
            MycatUser user = new MycatUser(username,new String(password),mycat.channel().socket().getRemoteSocketAddress().toString());
            int capabilities = auth.getCapabilities();
            if (MySQLServerCapabilityFlags.isCanUseCompressionProtocol(capabilities)) {
                String message = "Can Not Use Compression Protocol!";
                failture(mycat, message);
                mycat.lazyClose(true, message);
                return;
            }
          if (user == null) {
            String message = "user name is not existed";
            failture(mycat, message);
            return;
          }
            mycat.setUser(user);
            mycat.setSchema(database);
            mycat.setServerCapabilities(auth.getCapabilities());
            mycat.setAutoCommit(MySQLAutoCommit.ON);
            mycat.setIsolation(MySQLIsolation.READ_UNCOMMITTED);
            mycat.setCharset(characterSet);
            finished = true;
            mycatSessionManager.initCommandDispatcher(mycat);
            mycat.writeOkEndPacket();
        } catch (Exception e) {
            LOGGER.error("",e);
            MycatMonitor.onAuthHandlerReadException(mycat,e);
            onClear(mycat);
            failture(mycat, e);
        }
    }

    private AuthPacket readResponseAuthPacket(MycatSession mycat) {
        MySQLPacket mySQLPacket = mycat.currentProxyPayload();
        AuthPacket auth = new AuthPacket();
        auth.readPayload(mySQLPacket);
        mycat.resetCurrentProxyPayload();
        return auth;
    }

    public void failture(MycatSession mycat, String message) {
        mycat.setLastMessage(message);
        mycat.writeErrorEndPacketBySyncInProcessError(mycat.getNextPacketId(), ER_ACCESS_DENIED_ERROR);
    }

    public void failture(MycatSession mycat, Exception e) {
        mycat.setLastMessage(e);
        mycat.writeErrorEndPacketBySyncInProcessError(mycat.getNextPacketId(), ER_ACCESS_DENIED_ERROR);
    }

    @Override
    public void onSocketWrite(MycatSession mycat) {
        try {
            mycat.writeToChannel();
        } catch (Exception e) {
            LOGGER.error("{}", e);
            onClear(mycat);
            mycat.close(false, e);
        }
    }

    @Override
    public void onWriteFinished(MycatSession mycat) {
        if (!finished) {
            mycat.currentProxyBuffer().reset();
            mycat.change2ReadOpts();
        } else {
            onClear(mycat);
            mycat.switchNioHandler(MycatHandler.INSTANCE);
        }
    }

    @Override
    public void onException(MycatSession session, Exception e) {
        MycatMonitor.onAuthHandlerException(session,e);
        LOGGER.error("{}", e);
        onClear(mycat);
        mycat.close(false, e);
    }

    public void onClear(MycatSession session) {
        session.onHandlerFinishedClear();
        MycatMonitor.onAuthHandlerClear(session);
    }

    public void sendAuthPackge() {
        byte[][] seedParts = MysqlNativePasswordPluginUtil.nextSeedBuild();
        this.seed = seedParts[2];
        HandshakePacket hs = new HandshakePacket();
        hs.setProtocolVersion(MySQLVersion.PROTOCOL_VERSION);
        hs.setServerVersion(new String(MySQLVersion.SERVER_VERSION));
        hs.setConnectionId(mycat.sessionId());
        hs.setAuthPluginDataPartOne(new String(seedParts[0]));
        int serverCapabilities = MySQLServerCapabilityFlags.getDefaultServerCapabilities();
        mycat.setServerCapabilities(serverCapabilities);
        hs.setCapabilities(new MySQLServerCapabilityFlags(serverCapabilities));
        hs.setHasPartTwo(true);
        hs.setCharacterSet(8);
        hs.setStatusFlags(2);
        hs.setAuthPluginDataLen(21); // 有插件的话，总长度必是21, seed
        hs.setAuthPluginDataPartTwo(new String(seedParts[1]));
//    hs.setAuthPluginName(MysqlNativePasswordPluginUtil.PROTOCOL_PLUGIN_NAME);
        hs.setAuthPluginName(clientAuthPluginName);
        MySQLPayloadWriter mySQLPayloadWriter = new MySQLPayloadWriter();
        hs.writePayload(mySQLPayloadWriter);
        mycat.setPacketId(-1);//使用获取的packetId变为0
        mycat.writeBytes(mySQLPayloadWriter.toByteArray(),true);
    }


    private boolean checkPassword(String rightPassword, byte[] password) {
        if (rightPassword == null || rightPassword.length() == 0) {
            return (password == null || password.length == 0);
        }
        if (password == null || password.length == 0) {
            return false;
        }
//        if(clientAuthPluginName.equals(MysqlNativePasswordPluginUtil.PROTOCOL_PLUGIN_NAME)) {
            byte[] encryptPass = MysqlNativePasswordPluginUtil.scramble411(rightPassword, seed);
            if (checkBytes(password, encryptPass)) {
                return true;
            }
//        } else if(clientAuthPluginName.equals(CachingSha2PasswordPlugin.PROTOCOL_PLUGIN_NAME)){
            encryptPass = CachingSha2PasswordPlugin.scrambleCachingSha2(rightPassword, seed);
      return checkBytes(password, encryptPass);
//        } else {
//            throw new RuntimeException(String.format("unknow auth plugin %s", clientAuthPluginName));
//        }
    }

    private boolean checkBytes(byte[] encryptPass, byte[] password) {
        if (encryptPass != null && (encryptPass.length == password.length)) {
            int i = encryptPass.length;
            while (i-- != 0) {
                if (encryptPass[i] != password[i]) {
                    return false;
                }
            }
            return true;
        } else {
            return false;
        }
    }

}
