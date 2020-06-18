/**
 * Copyright (C) <2019>  <chen junwen>
 * <p>
 * This program is free software: you can redistribute it and/or modify it under the terms of the
 * GNU General Public License as published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; without
 * even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 * <p>
 * You should have received a copy of the GNU General Public License along with this program.  If
 * not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.beans;

import io.mycat.beans.mycat.MycatDataSource;
import io.mycat.config.DatasourceRootConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * MySQL Seesion元信息 对外支持线程修改的属性是alive,其他属性只读
 *
 * @author jamie12221 date 2019-05-10 13:21
 **/
public abstract class MySQLDatasource implements MycatDataSource {


    private static final Logger LOGGER = LoggerFactory.getLogger(MySQLDatasource.class);
    protected final DatasourceRootConfig.DatasourceConfig datasourceConfig;
    protected final AtomicInteger connectionCounter = new AtomicInteger(0);
    protected final AtomicInteger usedCounter = new AtomicInteger(0);

    public MySQLDatasource(DatasourceRootConfig.DatasourceConfig datasourceConfig) {
        this.datasourceConfig = datasourceConfig;
    }

    public int getSessionLimitCount() {
        return datasourceConfig.getMaxCon();
    }

    public int getSessionMinCount() {
        return datasourceConfig.getMinCon();
    }

    @Override
    public String getName() {
        return this.datasourceConfig.getName();
    }

    public String getIp() {
        return this.datasourceConfig.getIp();
    }

    public int getPort() {
        return this.datasourceConfig.getPort();
    }

    public String getUsername() {
        return this.datasourceConfig.getUser();
    }

    public String getPassword() {
        return this.datasourceConfig.getPassword();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MySQLDatasource that = (MySQLDatasource) o;
        return getName().equals(that.getName());
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }

    public int decrementSessionCounter() {
        return connectionCounter.updateAndGet(operand -> {
            if (operand > 0) {
                return --operand;
            } else {
                return 0;
            }
        });
    }

    public boolean tryIncrementSessionCounter() {
        return connectionCounter.updateAndGet(operand -> {
            if (!(operand < this.datasourceConfig.getMaxCon())) {
                return operand;
            } else {
                return ++operand;
            }
        }) < this.datasourceConfig.getMaxCon();
    }

    public int decrementUsedCounter() {
        return usedCounter.decrementAndGet();
    }

    public int tryIncrementUsedCounter() {
        return usedCounter.incrementAndGet();
    }

    public String getInitSqlForProxy() {
        List<String> initSqls = datasourceConfig.getInitSqls();
        if (initSqls.isEmpty()) {
            return null;
        } else {
            return String.join(";", initSqls);
        }
    }


    public int gerMaxRetry() {
        return this.datasourceConfig.getMaxRetryCount();
    }

    public long getMaxConnectTimeout() {
        return this.datasourceConfig.getMaxConnectTimeout();
    }

    public long getIdleTimeout() {
        return this.datasourceConfig.getIdleTimeout();
    }


    public int getConnectionCounter() {
        return connectionCounter.get();
    }

    public int getUsedCounter() {
        return usedCounter.get();
    }
}
