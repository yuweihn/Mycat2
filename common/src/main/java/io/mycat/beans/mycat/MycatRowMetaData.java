/**
 * Copyright (C) <2021>  <chen junwen>
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

package io.mycat.beans.mycat;

import java.io.Serializable;
import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.util.*;

/**
 * @author jamie12221
 * date 2020-01-09 23:18
 * column information,like a jdbc
 **/
public interface MycatRowMetaData extends Serializable {

    int getColumnCount();

    boolean isAutoIncrement(int column);

    boolean isCaseSensitive(int column);

    boolean isSigned(int column);

    int getColumnDisplaySize(int column);

    String getColumnName(int column);

    String getSchemaName(int column);

    int getPrecision(int column);

    int getScale(int column);

    String getTableName(int column);

    int getColumnType(int column);

    String getColumnLabel(int column);

    ResultSetMetaData metaData();

    public boolean isNullable(int column);

    public default boolean isPrimaryKey(int column) {
        return false;
    }

    default String toSimpleText() {
        int columnCount = getColumnCount();
        List list = new ArrayList();
        for (int i = 0; i < columnCount; i++) {
            Map<String, Object> info = new HashMap<>();


            String schemaName = getSchemaName(i);
            String tableName = getTableName(i);
            String columnName = getColumnName(i);
            boolean nullable = isNullable(i);
            int columnType = getColumnType(i);

//            info.put("schemaName", schemaName);
//            info.put("tableName", tableName);
            info.put("columnName", columnName);
            info.put("columnType", JDBCType.valueOf(columnType));
            info.put("nullable", nullable);
            list.add(info);

            String columnLabel = getColumnLabel(i);

            boolean autoIncrement = isAutoIncrement(i);
            boolean caseSensitive = isCaseSensitive(i);

            boolean signed = isSigned(i);
            int columnDisplaySize = getColumnDisplaySize(i);
            int precision = getPrecision(i);
            int scale = getScale(i);
        }
        return list.toString();
    }

    default boolean isUniqueKey(int column) {
        return isPrimaryKey(column);
    }

    default List<String> getColumnList() {
        int columnCount = getColumnCount();
        ArrayList<String> fields = new ArrayList<>();
        for (int i = 0; i < columnCount; i++) {
            fields.add(getColumnName(i));
        }
        return fields;
    }

   public default String toJson(){
        return null;
   }
}