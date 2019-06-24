/**
 * Copyright (C) <2019>  <mycat>
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

/**
 * @author jamie12221
 *  date 2019-05-05 16:22
 *
 * 预处理语句 execute 标记
 **/
public enum MySQLPrepareStmtExecuteFlag {
  CURSOR_TYPE_NO_CURSOR((byte)0x00),
  CURSOR_TYPE_READ_ONLY((byte)0x01),
  CURSOR_TYPE_FOR_UPDATE((byte)0x02),
  CURSOR_TYPE_SCROLLABLE((byte)0x04);
  byte value;

  MySQLPrepareStmtExecuteFlag(byte value) {
    this.value = value;
  }

  public byte getValue() {
    return value;
  }
}
