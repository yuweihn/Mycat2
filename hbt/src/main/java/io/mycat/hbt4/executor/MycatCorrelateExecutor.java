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
package io.mycat.hbt4.executor;

import io.mycat.hbt4.Executor;
import io.mycat.mpp.Row;
import org.apache.calcite.linq4j.Enumerable;

import java.util.Iterator;


/**
 * 未完成
 */
public class MycatCorrelateExecutor implements Executor {
    private final Iterator<Row> iterator;
    private Enumerable<Row> correlateJoin;

    public static MycatCorrelateExecutor create(Enumerable<Row> correlateJoin) {
        return new MycatCorrelateExecutor(correlateJoin);
    }

    protected MycatCorrelateExecutor(Enumerable<Row> correlateJoin) {
        this.correlateJoin = correlateJoin;
        this.iterator = this.correlateJoin.iterator();
    }

    @Override
    public void open() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Row next() {
        return null;
    }

    @Override
    public void close() {

    }

    @Override
    public boolean isRewindSupported() {
        return false;
    }
}