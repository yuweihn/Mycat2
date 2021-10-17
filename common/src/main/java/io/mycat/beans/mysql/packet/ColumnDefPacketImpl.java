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
package io.mycat.beans.mysql.packet;

import com.mysql.cj.CharsetMapping;
import com.mysql.cj.result.Field;
import io.mycat.MycatException;
import io.mycat.beans.mycat.CopyMycatRowMetaData;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.beans.mysql.MySQLFieldInfo;
import io.mycat.beans.mysql.MySQLFieldsType;
import io.mycat.util.StringUtil;

import java.sql.JDBCType;
import java.sql.ResultSetMetaData;
import java.util.Arrays;

/**
 * @author jamie12221 date 2019-05-07 13:58
 * <p>
 * 字段包实现
 **/
public class ColumnDefPacketImpl implements ColumnDefPacket {

    final static byte[] EMPTY = new byte[]{};
    byte[] columnCatalog;
    byte[] columnSchema;
    byte[] columnTable;
    byte[] columnOrgTable;
    byte[] columnName;
    byte[] columnOrgName;
    int columnNextLength = 0xC;
    int columnCharsetSet;
    int columnLength = 256;
    int columnType;
    int columnFlags;
    byte columnDecimals;
    byte[] columnDefaultValues;

    public ColumnDefPacketImpl() {
    }

//    public ColumnDefPacketImpl(final ResultSetMetaData resultSetMetaData, int columnIndex) {
//        try {
//            this.columnSchema = resultSetMetaData.getSchemaName(columnIndex).getBytes();
//            this.columnName = resultSetMetaData.getColumnLabel(columnIndex).getBytes();
//            this.columnOrgName = resultSetMetaData.getColumnName(columnIndex).getBytes();
//            this.columnNextLength = 0xC;
//            this.columnLength = 256;
//            this.columnType = MySQLFieldsType.fromJdbcType(resultSetMetaData.getColumnType(columnIndex));
//            this.columnDecimals = (byte) resultSetMetaData.getScale(columnIndex);
//            this.columnCharsetSet = 0x21;
//        } catch (Exception e) {
//            throw new MycatException(e);
//        }
//    }

    public ColumnDefPacketImpl(final MycatRowMetaData resultSetMetaData, int columnIndex) {
        try {
            String schemaName = resultSetMetaData.getSchemaName(columnIndex);
            if (StringUtil.isEmpty(schemaName)) {
                schemaName = "";//mysql workbench 该字段不能为长度0
            }
            int jdbcColumnType = resultSetMetaData.getColumnType(columnIndex);
            this.columnSchema = new byte[]{};
            String columnName = resultSetMetaData.getColumnName(columnIndex);
            this.columnName = getBytes(columnName);
            this.columnOrgName = new byte[]{};
            this.columnOrgTable = new byte[]{};
            this.columnTable = new byte[]{};
            this.columnNextLength = 0xC;
            this.columnType = MySQLFieldsType.fromJdbcType(jdbcColumnType);
            this.columnLength = resultSetMetaData.getColumnType(columnIndex) == JDBCType.BIT.getVendorTypeNumber() ? 1 : this.columnName.length;
            this.columnDecimals = (byte) resultSetMetaData.getScale(columnIndex);
            if (!resultSetMetaData.isNullable(columnIndex)) {
                this.columnFlags |= MySQLFieldsType.NOT_NULL_FLAG;
            }
            this.columnCharsetSet = 0x21;
            switch (JDBCType.valueOf(jdbcColumnType)) {
                case BINARY:
                case VARBINARY:
                case LONGVARBINARY:
                case BLOB:
                case CLOB: {
                    this.columnFlags |= MySQLFieldsType.BLOB_FLAG;
                    this.columnFlags |= MySQLFieldsType.BINARY_FLAG;
                    this.columnCharsetSet |= CharsetMapping.MYSQL_COLLATION_INDEX_binary;
                    break;
                }
                case TIMESTAMP: {
                    this.columnFlags |= MySQLFieldsType.TIMESTAMP_FLAG;
                    break;
                }
            }

            if (resultSetMetaData instanceof CopyMycatRowMetaData) {
                CopyMycatRowMetaData rowMetaData = (CopyMycatRowMetaData) resultSetMetaData;
                ResultSetMetaData rsmd = rowMetaData.metaData();
                if (null != rsmd && rsmd instanceof com.mysql.cj.jdbc.result.ResultSetMetaData) {
                    com.mysql.cj.jdbc.result.ResultSetMetaData metaData = (com.mysql.cj.jdbc.result.ResultSetMetaData) rsmd;
                    Field field = metaData.getFields()[columnIndex];
                    this.columnSchema = getBytes(field.getDatabaseName());
                    this.columnTable = getBytes(field.getTableName());
                    this.columnOrgTable = getBytes(field.getOriginalTableName());
                    this.columnName = getBytes(field.getName());
                    this.columnOrgName = getBytes(field.getOriginalName());

                    this.columnCharsetSet = field.getCollationIndex();
                    this.columnLength = (int) field.getLength();
                    this.columnType = field.getMysqlTypeId();
                    this.columnFlags = field.getFlags();
                    this.columnDecimals = (byte) field.getDecimals();
                }
            }

        } catch (Exception e) {
            throw new MycatException(e);
        }
    }

    byte[] getBytes(String text) {
        if (text == null || "".equals(text)) {
            return EMPTY;
        }
        return text.getBytes();
    }

    public ColumnDefPacket toColumnDefPacket(MySQLFieldInfo def, String alias) {
        ColumnDefPacket columnDefPacket = new ColumnDefPacketImpl();
        columnDefPacket.setColumnCatalog(columnDefPacket.DEFAULT_CATALOG);
        columnDefPacket.setColumnSchema(def.getSchemaName().getBytes());
        columnDefPacket.setColumnTable(def.getTableName().getBytes());
        if (alias == null) {
            alias = def.getName();
        }
        columnDefPacket.setColumnName(alias.getBytes());
        columnDefPacket.setColumnOrgName(def.getName().getBytes());
        columnDefPacket.setColumnNextLength(0xC);
        columnDefPacket.setColumnCharsetSet(def.getCollationId());
        columnDefPacket.setColumnLength(256);
        columnDefPacket.setColumnType(def.getFieldType());
        columnDefPacket.setColumnFlags(def.getFieldDetailFlag());
        columnDefPacket.setColumnDecimals(def.getDecimals());
        columnDefPacket.setColumnDefaultValues(def.getDefaultValues());
        return columnDefPacket;
    }

    public void writePayload(MySQLPayloadWriteView buffer) {
        buffer.writeLenencBytesWithNullable(ColumnDefPacket.DEFAULT_CATALOG);
        buffer.writeLenencBytesWithNullable(columnSchema);
        buffer.writeLenencBytesWithNullable(columnTable);
        buffer.writeLenencBytesWithNullable(columnOrgTable);
        buffer.writeLenencBytesWithNullable(columnName);
        buffer.writeLenencBytesWithNullable(columnOrgName);
        buffer.writeLenencInt(0x0c);
        buffer.writeFixInt(2, columnCharsetSet);
        buffer.writeFixInt(4, columnLength);
        buffer.writeByte(columnType);
        buffer.writeFixInt(2, columnFlags);
        buffer.writeByte(columnDecimals);
        buffer.writeByte((byte) 0x00);//filler
        buffer.writeByte((byte) 0x00);//filler
        if (columnDefaultValues != null) {
            buffer.writeLenencString(columnDefaultValues);
        }
    }

    @Override
    public String toString() {
        return "ColumnDefPacketImpl{" +
                "columnCatalog=" + new String(ColumnDefPacket.DEFAULT_CATALOG) +
                ", columnSchema=" + new String(columnSchema) +
                ", columnTable=" + new String(columnTable) +
                ", columnOrgTable=" + new String(columnOrgTable) +
                ", columnName=" + new String(columnName) +
                ", columnOrgName=" + new String(columnOrgName) +
                ", columnNextLength=" + columnNextLength +
                ", columnCharsetSet=" + columnCharsetSet +
                ", columnLength=" + columnLength +
                ", columnType=" + columnType +
                ", columnFlags=" + columnFlags +
                ", columnDecimals=" + columnDecimals +
                ", columnDefaultValues=" + Arrays.toString(columnDefaultValues) +
                '}';
    }


    @Override
    public byte[] getColumnCatalog() {
        return ColumnDefPacket.DEFAULT_CATALOG;
    }

    @Override
    public void setColumnCatalog(byte[] catalog) {
        this.columnCatalog = catalog;
    }

    @Override
    public byte[] getColumnSchema() {
        return columnSchema;
    }

    @Override
    public void setColumnSchema(byte[] schema) {
        this.columnSchema = schema;
    }

    @Override
    public byte[] getColumnTable() {
        return columnTable;
    }

    @Override
    public void setColumnTable(byte[] table) {
        this.columnTable = table;
    }

    @Override
    public byte[] getColumnOrgTable() {
        return columnOrgTable;
    }

    @Override
    public void setColumnOrgTable(byte[] orgTable) {
        this.columnOrgTable = orgTable;
    }

    @Override
    public byte[] getColumnName() {
        return columnName;
    }

    @Override
    public void setColumnName(byte[] name) {
        this.columnName = name;
    }

    @Override
    public byte[] getColumnOrgName() {
        return columnOrgName;
    }

    @Override
    public void setColumnOrgName(byte[] orgName) {
        this.columnOrgName = orgName;
    }

    @Override
    public int getColumnNextLength() {
        return columnNextLength;
    }

    @Override
    public void setColumnNextLength(int nextLength) {
        this.columnLength = nextLength;
    }

    @Override
    public int getColumnCharsetSet() {
        return columnCharsetSet;
    }

    @Override
    public void setColumnCharsetSet(int charsetSet) {
        this.columnCharsetSet = charsetSet;
    }

    @Override
    public int getColumnLength() {
        return columnLength;
    }

    @Override
    public void setColumnLength(int columnLength) {
        this.columnLength = columnLength;
    }

    @Override
    public int getColumnType() {
        return columnType;
    }

    @Override
    public void setColumnType(int type) {
        this.columnType = type;
    }

    @Override
    public int getColumnFlags() {
        return columnFlags;
    }

    @Override
    public void setColumnFlags(int flags) {
        this.columnFlags = flags;
    }

    @Override
    public byte getColumnDecimals() {
        return this.columnDecimals;
    }

    @Override
    public void setColumnDecimals(byte decimals) {
        this.columnDecimals = decimals;
    }

    @Override
    public byte[] getColumnDefaultValues() {
        return columnDefaultValues;
    }

    @Override
    public void setColumnDefaultValues(byte[] defaultValues) {
        this.columnDefaultValues = defaultValues;
    }

    public void read(MySQLPacket buffer, int startPos, int endPos) {
        buffer.packetReadStartIndex(startPos);
        this.columnCatalog = buffer.readLenencStringBytes();
        this.columnSchema = buffer.readLenencStringBytes();
        this.columnTable = buffer.readLenencStringBytes();
        this.columnOrgTable = buffer.readLenencStringBytes();
        this.columnName = buffer.readLenencStringBytes();
        this.columnOrgName = buffer.readLenencStringBytes();
        byte c = buffer.readByte();
        assert (0xc == c);
        this.columnCharsetSet = (int) buffer.readFixInt(2);
        this.columnLength = (int) buffer.readFixInt(4);
        this.columnType = (buffer.readByte() & 0xff);
        this.columnFlags = (int) buffer.readFixInt(2);
        this.columnDecimals = buffer.readByte();
        buffer.skipInReading(2);
        if (!buffer.readFinished()) {
            Long len = buffer.readLenencInt();
            if (len != null && len > 0) {
                this.columnDefaultValues = buffer.readFixStringBytes(len.intValue());
            }
        }
    }

}
