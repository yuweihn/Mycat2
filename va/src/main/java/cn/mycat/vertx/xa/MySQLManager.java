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

import io.mycat.newquery.NewMycatConnection;
import io.vertx.core.Future;

import java.util.Map;


public interface MySQLManager {

    Future<NewMycatConnection> getConnection(String targetName);
    int getAvailableNumber(String targetName);
    int getSessionCount(String targetName);

    Map<String, java.sql.Connection> getWriteableConnectionMap();

    java.sql.Connection getWriteableConnection(String name);

    Future<Void> close();

    Future<Map<String, Integer>> computeConnectionUsageSnapshot();

    void setTimer(long delay, Runnable handler);


}