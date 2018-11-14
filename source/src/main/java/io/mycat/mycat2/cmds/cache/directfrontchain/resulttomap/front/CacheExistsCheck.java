package io.mycat.mycat2.cmds.cache.directfrontchain.resulttomap.front;

import io.mycat.mycat2.MycatSession;
import io.mycat.mycat2.beans.SqlCacheBean;
import io.mycat.mycat2.cmds.cache.mapcache.CacheManager;
import io.mycat.mycat2.cmds.sqlCmds.CacheMapFileCommand;
import io.mycat.mycat2.common.ChainExecInf;
import io.mycat.mycat2.common.SeqContextList;
import io.mycat.mycat2.console.SessionKey;
import io.mycat.mysql.packet.QueryPacket;
import io.mycat.proxy.ProxyBuffer;

import java.io.IOException;

/**
 * 用于缓存的逻辑检查操作
 * 
 * @since 2017年9月18日 下午4:15:50
 * @version 0.0.1
 * @author liujun
 */
public class CacheExistsCheck implements ChainExecInf {

	/**
	 * 实例对象
	 */
	public static final CacheExistsCheck INSTANCE = new CacheExistsCheck();

	@Override
	public boolean invoke(SeqContextList seqList) throws Exception {

		MycatSession mycatSession = (MycatSession) seqList.getSession();

		String sql = (String) mycatSession.getAttrMap().get(SessionKey.CACHE_SQL_STR);

		// 检查缓存是否存在
		boolean exists = CacheManager.INSTANCE.cacheExists(sql);

		// 当缓存不存在时，创建缓存
		if (!exists) {

			long cacheTime = (long) mycatSession.getAttrMap()
					.get(SessionKey.CACHE_TIMEOUT);

			// 标识当前缓存放入开始
			boolean upd = CacheManager.INSTANCE.begin(sql);

			if (upd) {
				// 添加缓存操作
				addCache(mycatSession, sql, true, (int) cacheTime);
			}

			return true;
		}
		// 如果缓存已经存在，则检查可用性
		else {
			SqlCacheBean sqlBean = CacheManager.INSTANCE.getCacheBean(sql);

			// 检查当前是否可用
			if (sqlBean != null && sqlBean.getCacheMapFile().isCacheAvailable()) {
				// 获取临近过期时间的配制
				long cacheTimeOut = (long) mycatSession.getAttrMap()
						.get(SessionKey.CACHE_TIMEOUT_CRITICAL);

				long currTime = System.currentTimeMillis();

				// 如果当前缓存已经过期,重新加载数据，返回前段
				if (currTime >= sqlBean.getTimeOut()) {
					// 标识当前缓存放入开始
					boolean upd = CacheManager.INSTANCE.begin(sql);

					if (upd) {
						// 先将数据进行清理，再进行将缓存更新
						CacheManager.INSTANCE.cleanCacheData(sql);
						// 再添加缓存据据,返回响应给前段
						timeOueryCache(mycatSession);
					}
				}
				// 检查是否临近过期时间,如果是先响应前段，然后响应完成后
				else if (currTime + cacheTimeOut >= sqlBean.getTimeOut()) {

					// 标识当前响应前段
					mycatSession.getAttrMap().put(SessionKey.CACHE_WRITE_FRONT_FLAG_KEY,
							true);

					// 标识当前需要从缓存中获取的标识
					mycatSession.getAttrMap().put(SessionKey.CACHE_GET_FLAG, true);

					// 打上标识，当响应前段完成后，进行缓存的清理
					mycatSession.getAttrMap()
							.put(SessionKey.CACHE_DELETE_QUERY_FLAG_KEY, true);

					// 进行数据的读取流程
					mycatSession.curSQLCommand = CacheMapFileCommand.INSTANCE;
					// 调用进行前段的数据请求操作
					// mycatSession.curSQLCommand.procssSQL(mycatSession);

				}
				// 未过期，直接从缓存中读取
				else {
					// 标识当前响应前段
					mycatSession.getAttrMap().put(SessionKey.CACHE_WRITE_FRONT_FLAG_KEY,
							true);

					// 标识当前需要从缓存中获取的标识
					mycatSession.getAttrMap().put(SessionKey.CACHE_GET_FLAG, true);

					// 进行数据的读取流程
					mycatSession.curSQLCommand = CacheMapFileCommand.INSTANCE;
					// 调用后端的数据处理
					// mycatSession.curSQLCommand.procssSQL(mycatSession);
				}
			}
		}

		return false;
	}

	/**
	 * 添加缓存操作
	 * 
	 * @param mycatSession
	 *            session信息
	 * @param sql
	 *            缓存的SQL
	 * @param rspFront
	 *            是否响应前段
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void addCache(MycatSession mycatSession, String sql, boolean rspFront, int timeOut)
			throws IOException, InterruptedException {
		// 过期时间为2分钟
		// int timeOut = 2 * 60;
		// 内存映射为16K
		int mapMemory = mycatSession.getProxyBuffer().getBuffer().capacity();

		// 创建一个SQL缓存,当这个缓存不存在时
		CacheManager.INSTANCE.createCache(sql, timeOut, mapMemory);

		// 标识当前添加缓存操作
		mycatSession.getAttrMap().put(SessionKey.CACHE_ADD_FLAG_KEY, true);

		// 标识当前缓存需要响应前端
		mycatSession.getAttrMap().put(SessionKey.CACHE_WRITE_FRONT_FLAG_KEY, rspFront);

		// // 将当前的SQLcommand切换到缓存数据响应的写入
		// mycatSession.curSQLCommand = CacheMapFileCommand.INSTANCE;
		// 进行数据的读取流程
		mycatSession.curSQLCommand = CacheMapFileCommand.INSTANCE;

		// 调用进行前段的数据请求操作
		// mycatSession.curSQLCommand.procssSQL(mycatSession);

		// 设置查询请求的SQL
		queryBufferPkg(mycatSession);

		// // 将查询的数据写入至mysql
		// ProxyBuffer curBuffer = mycatSession.proxyBuffer;
		// // 切换 buffer 读写状态
		// curBuffer.flip();
		// // 没有读取,直接透传时,需要指定 透传的数据 截止位置
		// curBuffer.readIndex = curBuffer.writeIndex;
		// // 改变 owner，对端Session获取，并且感兴趣写事件
		// mycatSession.giveupOwner(SelectionKey.OP_WRITE);
		// // 后数进行写入
		// mycatSession.curBackend.writeToChannel();

	}

	/**
	 * 超时之后进行的数据重新查询操作
	 * 
	 * @param mycatSession
	 */
	private void timeOueryCache(MycatSession mycatSession) {
		// 标识当前添加缓存操作
		mycatSession.getAttrMap().put(SessionKey.CACHE_ADD_FLAG_KEY, true);

		// 标识当前缓存需要响应前端
		mycatSession.getAttrMap().put(SessionKey.CACHE_WRITE_FRONT_FLAG_KEY, true);

		// 将当前的SQLcommand切换到缓存数据响应的写入
		// mycatSession.curSQLCommand = CacheMapFileCommand.INSTANCE;
		// 进行数据的读取流程
		mycatSession.curSQLCommand = CacheMapFileCommand.INSTANCE;
		// 调用进行前段的数据请求操作
		// mycatSession.curSQLCommand.procssSQL(mycatSession);
		// 设置查询请求的SQL
		queryBufferPkg(mycatSession);

		// // 将查询的数据写入至mysql
		// ProxyBuffer curBuffer = mycatSession.proxyBuffer;
		// // 切换 buffer 读写状态
		// curBuffer.flip();
		// // 没有读取,直接透传时,需要指定 透传的数据 截止位置
		// curBuffer.readIndex = curBuffer.writeIndex;
		// // 改变 owner，对端Session获取，并且感兴趣写事件
		// mycatSession.giveupOwner(SelectionKey.OP_WRITE);
		// // 后数进行写入
		// mycatSession.curBackend.writeToChannel();
	}

	private void queryBufferPkg(MycatSession mycatSession) {
		// 将当前的SQL信息放入到session中
		String selectSql = (String) mycatSession.getAttrMap()
				.get(SessionKey.CACHE_SQL_STR);

		// buffer查询
		ProxyBuffer proxyBuf = mycatSession.proxyBuffer;

		// 清理buffer，重新组装查询的报文
		proxyBuf.reset();

		QueryPacket queryPkg = new QueryPacket();

		queryPkg.packetId = 0;
		queryPkg.packetLength = selectSql.getBytes().length;
		queryPkg.sql = selectSql;

		// 查询数据的sql
		queryPkg.write(proxyBuf);
	}

}
