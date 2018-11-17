package io.mycat.mycat2.cmds.cache.directfrontchain.resulttomap.back;

import io.mycat.mycat2.MySQLSession;
import io.mycat.mycat2.beans.SqlCacheBean;
import io.mycat.mycat2.cmds.cache.mapcache.CacheManager;
import io.mycat.mycat2.common.ChainExecInf;
import io.mycat.mycat2.common.SeqContextList;
import io.mycat.mycat2.console.SessionKey;

/**
 * 进行后端事件的更新操作
 * 
 * @since 2017年9月18日 下午4:49:35
 * @version 0.0.1
 * @author liujun
 */
public class BackEventProc implements ChainExecInf {

	
	/**
	 * 实例对象
	 */
	public static final BackEventProc INSTANCE = new BackEventProc();
	
	@Override
	public boolean invoke(SeqContextList seqList) throws Exception {
		// 首先获取SQL
		MySQLSession mysqlSession = (MySQLSession) seqList.getSession();

		// 获取当前是否结束标识
		boolean check = mysqlSession.isIdle();
		// 检查到当前已经完成
		if (check) {
			// 首先清除持端的事件
			mysqlSession.clearReadWriteOpts();

			String cacheSql = (String) mysqlSession.getMycatSession().getAttrMap()
					.get(SessionKey.CACHE_SQL_STR);
			// 获取缓存的的信息
			SqlCacheBean sqlCache = CacheManager.INSTANCE.getCacheBean(cacheSql);
			//设置过期时间
			sqlCache.setTimeOut(System.currentTimeMillis() + sqlCache.getTimeOutCfg());
			// 设置可用为true
			sqlCache.getCacheMapFile().setCacheAvailable(true);

			// 移除从缓存中获取数据的标识
			mysqlSession.getAttrMap().remove(SessionKey.CACHE_GET_FLAG);
			// 移除SQL信息
			mysqlSession.getAttrMap().remove(SessionKey.CACHE_SQL_STR);

			// 同时修改缓存数据可用

		} else {
			// 未完成，则注册后端的读取事件
			mysqlSession.change2ReadOpts();
		}

		return seqList.nextExec();
	}

}
