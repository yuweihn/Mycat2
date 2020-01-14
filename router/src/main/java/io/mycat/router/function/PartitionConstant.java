/**
 * Copyright (C) <2020>  <mycat>
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
package io.mycat.router.function;

import io.mycat.router.RuleFunction;

import java.util.Map;

/**
 * @author jamie12221 date 2020-01-04
 **/
public class PartitionConstant extends RuleFunction {

    private int defaultNode;
    private int[] nodes;

    @Override
    public String name() {
        return "PartitionConstant";
    }

    @Override
    public void init(Map<String, String> properties, Map<String, String> ranges) {
        this.defaultNode = Integer.parseInt(properties.get("defaultNode"));
        this.nodes = new int[]{defaultNode};
    }

    @Override
    public int calculate(String columnValue) {
        return defaultNode;
    }

    @Override
    public int[] calculateRange(String beginValue, String endValue) {
        return nodes;
    }

    @Override
    public int getPartitionNum() {
        return 1;
    }
}
