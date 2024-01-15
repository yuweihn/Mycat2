/*******************************************************************************
 *     ___                  _   ____  ____
 *    / _ \ _   _  ___  ___| |_|  _ \| __ )
 *   | | | | | | |/ _ \/ __| __| | | |  _ \
 *   | |_| | |_| |  __/\__ \ |_| |_| | |_) |
 *    \__\_\\__,_|\___||___/\__|____/|____/
 *
 *  Copyright (c) 2014-2019 Appsicle
 *  Copyright (c) 2019-2020 QuestDB
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 ******************************************************************************/
/*
 *     Copyright (C) <2021>  <Junwen Chen>
 *
 *     This program is free software: you can redistribute it and/or modify
 *     it under the terms of the GNU General Public License as published by
 *     the Free Software Foundation, either version 3 of the License, or
 *     (at your option) any later version.
 *
 *     This program is distributed in the hope that it will be useful,
 *     but WITHOUT ANY WARRANTY; without even the implied warranty of
 *     MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *     GNU General Public License for more details.
 *
 *     You should have received a copy of the GNU General Public License
 *     along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */

package io.ordinate.engine.function;

import io.ordinate.engine.schema.InnerType;
import io.ordinate.engine.record.Record;

public abstract class  BooleanFunction implements ScalarFunction {

    @Override
    public final BinarySequence getBinary(Record rec) {
        throw new UnsupportedOperationException();
    }

    @Override
    public final byte getByte(Record rec) {
        return (byte) getInt(rec);
    }

    @Override
    public final char getChar(Record rec) {
        return getInt(rec) ==1? 't' : 'f';
    }

    @Override
    public final long getDate(Record rec) {
        return getInt(rec);
    }

    @Override
    public final double getDouble(Record rec) {
        return getInt(rec);
    }

    @Override
    public final float getFloat(Record rec) {
        return getInt(rec);
    }


    @Override
    public final long getLong(Record rec) {
        return  getInt(rec);
    }


    @Override
    public final short getShort(Record rec) {
        return (short) getInt(rec);
    }

    @Override
    public final CharSequence getString(Record rec) {
        return getStr0(rec);
    }


    @Override
    public long getDatetime(Record rec) {
        return  getInt(rec)==1 ? 0 : 1;
    }

    @Override
    public final InnerType getType() {
        return InnerType.BOOLEAN_TYPE;
    }

    private String getStr0(Record rec) {
        return  getInt(rec) ==1 ? "true" : "false";
    }


}
