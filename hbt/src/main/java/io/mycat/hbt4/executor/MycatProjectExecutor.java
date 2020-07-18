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

public class MycatProjectExecutor implements Executor {
    private MycatScalar mycatScalar;
    private Executor executor;

    public MycatProjectExecutor(MycatScalar mycatScalar, Executor executor) {
        this.mycatScalar = mycatScalar;
        this.executor = executor;
    }

    @Override
    public void open() {
        executor.open();
    }

    @Override
    public Row next() {
        Row next = executor.next();
        if (next == null){
            return null;
        }
        Row res = Row.create(next.size());
        mycatScalar.execute(next,res);
        return res;
    }

    @Override
    public void close() {
        executor.close();
    }

    @Override
    public boolean isRewindSupported() {
        return executor.isRewindSupported();
    }
}