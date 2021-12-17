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

package io.ordinate.engine.function.math;


import io.ordinate.engine.builder.EngineConfiguration;
import io.ordinate.engine.function.DoubleFunction;
import io.ordinate.engine.function.Function;
import io.ordinate.engine.function.FunctionFactory;
import io.ordinate.engine.function.UnaryFunction;
import io.ordinate.engine.record.Record;

import java.util.List;

public class Atan2DoubleFunctionFactory implements FunctionFactory {
    @Override
    public String getSignature() {
        return "atan2(double,double)";
    }

    @Override
    public Function newInstance(List<Function> args, EngineConfiguration configuration) {
        return new Atan2DoubleFunction(args);
    }

    private static class Atan2DoubleFunction extends DoubleFunction {
        private final List<Function> args;
        boolean isNull;

        public Atan2DoubleFunction(List<Function> args) {
            super();
            this.args = args;
        }

        @Override
        public List<Function> getArgs() {
            return args;
        }

        @Override
        public double getDouble(Record rec) {
            Function one = args.get(0);
            Function two = args.get(1);

            isNull = one.isNull(rec) || two.isNull(rec);
            if (isNull) return 0;

            return Math.atan2(one.getDouble(rec), two.getDouble(rec));
        }

        @Override
        public boolean isNull(Record rec) {
            return isNull;
        }
    }
}
