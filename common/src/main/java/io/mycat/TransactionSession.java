/**
 * Copyright (C) <2021>  <chen junwen>
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
package io.mycat;

import io.mycat.beans.mycat.TransactionType;
import io.mycat.beans.mysql.MySQLIsolation;
import io.vertx.core.Future;
import lombok.SneakyThrows;

import java.util.concurrent.TimeUnit;

/**
 * @author Junwen Chen
 **/
public interface TransactionSession extends Dumpable {

    public final static String LOCAL = "local";
    public final static String XA = "xa";
    public final static String PROXY = "proxy";

    String name();

    Future<Void> begin();

    Future<Void> commit();

    Future<Void> rollback();

    boolean isInTransaction();

    boolean isAutocommit();

    void setAutocommit(boolean autocommit);

    MySQLIsolation getTransactionIsolation();

    void setTransactionIsolation(MySQLIsolation transactionIsolation);

    void setReadOnly(boolean value);

    Future<Void> closeStatementState();

    Future<Void> close();

    String resolveFinalTargetName(String targetName, boolean master,ReplicaBalanceType replicaBalanceType);

    /**
     * 模拟autocommit = 0 时候自动开启事务
     */
    public Future<Void> openStatementState();


    String getXid();

    @SneakyThrows
    default void deliverTo(TransactionSession newTransactionSession) {
        boolean inTransaction = isInTransaction();
        if (inTransaction) {
            throw new IllegalArgumentException("can not deliver transcation in transcation ");
        }
        closeStatementState().toCompletionStage().toCompletableFuture().get(1, TimeUnit.MINUTES);
        newTransactionSession.setTransactionIsolation(getTransactionIsolation());
        newTransactionSession.setAutocommit(isAutocommit());
    }

    TransactionType transactionType();

    Future<Void> kill();
}