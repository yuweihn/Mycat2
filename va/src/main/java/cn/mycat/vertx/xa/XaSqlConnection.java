/**
 * Copyright [2021] [chen junwen]
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.mycat.vertx.xa;

import io.mycat.beans.mysql.MySQLIsolation;
import io.mycat.newquery.NewMycatConnection;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.util.List;
import java.util.function.Function;

public interface XaSqlConnection {
    public static String XA_START = "XA START '%s';";
    public static String XA_END = "XA END '%s';";
    public static String XA_COMMIT = "XA COMMIT '%s';";
    public static String XA_PREPARE = "XA PREPARE '%s';";
    public static String XA_ROLLBACK = "XA ROLLBACK '%s';";
    public static String XA_COMMIT_ONE_PHASE = "XA COMMIT '%s' ONE PHASE;";
    public static String XA_RECOVER = "XA RECOVER;";

    public void setTransactionIsolation(MySQLIsolation level);

    public MySQLIsolation getTransactionIsolation();

    public Future<Void> begin();

    public Future<NewMycatConnection> getConnection(String targetName);

    public List<NewMycatConnection> getExistedTranscationConnections();

    public Future<Void> rollback();

    public Future<Void> commit();

    public Future<Void> commitXa(Function<ImmutableCoordinatorLog, Future<Void>> beforeCommit);

    public Future<Void> close();

    public Future<Void> kill();

    public Future<Void> openStatementState();

    public Future<Void> closeStatementState();

    public void setAutocommit(boolean b);

    public boolean isAutocommit();

    public boolean isInTransaction();

    String getXid();

    void addCloseFuture(Future<Void> future);

    public default Future<Void> createSavepoint(String name) {
        return Future.succeededFuture();
    }

    public default Future<Void> rollbackSavepoint(String name) {
        return Future.succeededFuture();
    }

    public default Future<Void> releaseSavepoint(String name) {
        return Future.succeededFuture();
    }

    public List<NewMycatConnection> getAllConnections();
}
