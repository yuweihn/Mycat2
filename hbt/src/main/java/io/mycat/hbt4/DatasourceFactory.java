/**
 * Copyright (C) <2020>  <chen junwen>
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
package io.mycat.hbt4;

import com.google.common.collect.ImmutableList;
import lombok.Data;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

public interface DatasourceFactory extends AutoCloseable {

    public void open();

    public void createTableIfNotExisted(String targetName, String createTableSql);

    Map<String, Connection> getConnections(List<String> targets);

    void regist(ImmutableList<String> asList);

    Connection getConnection(String key);

    List<Connection> getTmpConnections(List<String> targets);

    void recycleTmpConnections(List<Connection> connections);
}