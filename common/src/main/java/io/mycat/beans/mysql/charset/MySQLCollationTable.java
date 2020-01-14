/**
 * Copyright (C) <2019>  <chen junwen>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.mycat.beans.mysql.charset;

import java.util.HashMap;
import java.util.Map;

/**
 * @author jamie12221
 *  date 2019-05-05 16:22
 *
 * mysql 状态 字符集 LCollationTable
 **/
public class MySQLCollationTable {
    final Map<Integer, MySQLCollation> indexMap = new HashMap<>();
    final Map<String, MySQLCollation> collationNameMap = new HashMap<>();

    public MySQLCollation getCollationById(Integer id) {
        return indexMap.get(id);
    }

    public MySQLCollation getByCollationName(String collationName) {
        return collationNameMap.get(collationName);
    }

    public void put(MySQLCollation collation) {
        collationNameMap.put(collation.getCollatioNname(), collation);
        indexMap.put(collation.getId(), collation);
    }
}
