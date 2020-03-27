/**
 * Copyright (C) <2020>  <chenjunwen>
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

package io.mycat.beans.mysql;

import lombok.Data;
import lombok.ToString;

import java.sql.JDBCType;

/**
 * @author: chenjunwen 294712221
 */
@ToString
@Data
public class MySQLVariableObject {
    private MySQLVariablesEnum enumObject;
    private String columnName;
    private JDBCType jdbcType;
    private Class<?> clazz;
    private Object value;

    public MySQLVariableObject(MySQLVariablesEnum enumObject, String columnName, JDBCType jdbcType, Class<?> clazz, Object value) {
        this.enumObject = enumObject;
        this.columnName = columnName;
        this.jdbcType = jdbcType;
        this.clazz = clazz;
        this.value = value;
    }
}