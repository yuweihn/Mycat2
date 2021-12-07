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

package io.ordinate.engine.function.bind;

import io.ordinate.engine.function.BinarySequence;
import io.ordinate.engine.record.Record;
import io.ordinate.engine.function.ScalarFunction;
import io.ordinate.engine.function.BinarySequenceFunction;
import io.ordinate.engine.function.Function;

import java.util.Collections;
import java.util.List;

public class BinaryBindVariable extends BinarySequenceFunction implements ScalarFunction, BindVariable {
    BinarySequence value;
    boolean isNull;

    @Override
    public List<Function> getArgs() {
        return Collections.emptyList();
    }

    @Override
    public BinarySequence getBinary(Record rec) {
        return value;
    }

    @Override
    public boolean isRuntimeConstant() {
        return true;
    }

    @Override
    public boolean isNull(Record rec) {
        return isNull;
    }

    @Override
    public void setObject(Object o) {
        if (o == null) {
            isNull = true;
            return;
        }else if (o instanceof byte[]) {
            value = BinarySequence.of((byte[]) o);
        }else {
            throw new UnsupportedOperationException();
        }
    }
}
