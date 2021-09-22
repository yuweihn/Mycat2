package io.mycat.resultset;

import io.mycat.api.collector.RowBaseIterator;
import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.MySQLPacketUtil;
import io.mycat.beans.mysql.MySQLPayloadWriter;
import io.mycat.util.ByteUtil;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.Types;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Date;
import java.util.Iterator;

import static java.sql.Types.*;

public class BinaryResultSetResponse extends AbstractMycatResultSetResponse {
    private static final Logger LOGGER = LoggerFactory.getLogger(BinaryResultSetResponse.class);
    final int[] jdbcTypes;

    public BinaryResultSetResponse(RowBaseIterator iterator) {
        super(iterator);
        final MycatRowMetaData mycatRowMetaData = iterator.getMetaData();
        int columnCount = mycatRowMetaData.getColumnCount();
        jdbcTypes = new int[columnCount];
        for (int i = 0; i < columnCount; i++) {
            int type = mycatRowMetaData.getColumnType(i);
            jdbcTypes[i] = type;
        }
    }

    @Override
    public Iterator rowIterator() {
        final RowBaseIterator rowBaseIterator = iterator;
        return new Iterator<byte[]>() {

            @Override
            public boolean hasNext() {
                return rowBaseIterator.next();
            }

            @Override
            public byte[] next() {
                byte[][] rows = getBytes(rowBaseIterator);
                byte[] bytes = MySQLPacketUtil.generateBinaryRow(rows);
                return bytes;
            }
        };
    }

    @NotNull
    public byte[][] getBytes(RowBaseIterator rowBaseIterator) {
        byte[][] rows = new byte[jdbcTypes.length][];
        for (int i = 0; i < jdbcTypes.length; i++) {
            int columnType = jdbcTypes[i];
            Object object = rowBaseIterator.getObject(i);
            boolean wasNull = rowBaseIterator.wasNull();
            if (wasNull) {
                rows[i] = null;
                continue;
            }
            byte[] value = getBytes(columnType, object);
            rows[i] = value;
        }
        return rows;
    }

    @Nullable
    public static byte[] getBytes(int columnType, Object object) {
        byte[] value;
        switch (columnType) {
            case BIT://MysqlDefs.FIELD_TYPE_BIT n
                value = converBit(object);
                break;
            case TINYINT://MysqlDefs.FIELD_TYPE_TINY 1
                value = convertToTiny(object);
                break;
            case SMALLINT://MysqlDefs.FIELD_TYPE_SHORT 2
                value = convertToInt16((Number) object);
                break;
            case INTEGER://MysqlDefs.FIELD_TYPE_LONG  4
                value = convertToInt32((Number) object);
                break;
            case BIGINT://MysqlDefs.FIELD_TYPE_LONGLONG 8
                value = convertToInt64((Number) object);
                break;
            case BOOLEAN://MysqlDefs.FIELD_TYPE_TINY 1
                value = convertToTiny(object);
                break;
            case Types.NUMERIC://MysqlDefs.FIELD_TYPE_DECIMAL n
                value = convertToInt16((Number) object);
                break;

            case Types.REAL://MysqlDefs.FIELD_TYPE_FLOAT 4
                value = convertToFloat32((Number) object);
                break;
            case Types.DOUBLE://MysqlDefs.FIELD_TYPE_DOUBLE 8
                value = convertToFloat64((Number) object);
                break;
            case Types.NULL://MysqlDefs.FIELD_TYPE_NULL null
                value = null;
                break;
            case Types.TIMESTAMP://MysqlDefs.FIELD_TYPE_TIMESTAMP t
            case Types.TIMESTAMP_WITH_TIMEZONE:
                try {
                    if (object instanceof Date) {
                        Date dateVar = (Date) object;
                        value = (ByteUtil.getBytes(dateVar, false));
                    } else if (object instanceof LocalDateTime) {
                        LocalDateTime localDateTime = (LocalDateTime) object;
                        value = (ByteUtil.getBytesFromTimestamp(localDateTime));
                    } else {
                        throw new UnsupportedOperationException("unsupported class:" + object.getClass());
                    }
                } catch (org.joda.time.IllegalFieldValueException e1) {
                    // 当时间为 0000-00-00 00:00:00 的时候, 默认返回 1970-01-01 08:00:00.0
                    value = (ByteUtil.getBytes(new Date(0L), true));
                }
                break;
            case Types.TIME_WITH_TIMEZONE:
            case Types.TIME://MysqlDefs.FIELD_TYPE_TIME t
                try {
                    if (object instanceof Date) {
                        Date dateVar = (Date) object;
                        value = (ByteUtil.getBytes(dateVar, true));
                    } else if (object instanceof String) {
                        String dateText = (String) object;
                        value = (ByteUtil.getBytesFromTimeString(dateText));
                    } else if (object instanceof LocalTime) {
                        LocalTime time = (LocalTime) object;
                        value = (ByteUtil.getBytesFromTime(time));
                    } else if (object instanceof Duration) {
                        Duration time = (Duration) object;
                        value = (ByteUtil.getBytesFromDuration(time));
                    } else {
                        throw new UnsupportedOperationException("unsupported class:" + object.getClass());
                    }
                } catch (org.joda.time.IllegalFieldValueException e1) {
                    // 当时间为 0000-00-00 00:00:00 的时候, 默认返回 1970-01-01 08:00:00.0
                    value = (ByteUtil.getBytes(new Date(0L), true));
                }
                break;
            case Types.DATE://MysqlDefs.FIELD_TYPE_DATE t
                try {
                    if (object instanceof LocalDate) {
                        LocalDate date = (LocalDate) object;
                        value = (ByteUtil.getBytesFromDate(date));
                    } else {
                        Date dateVar = (Date) object;
                        value = (ByteUtil.getBytes(dateVar, false));
                    }
                } catch (org.joda.time.IllegalFieldValueException e1) {
                    // 当时间为 0000-00-00 00:00:00 的时候, 默认返回 1970-01-01 08:00:00.0
                    value = (ByteUtil.getBytes(new Date(0L), false));
                }
                break;
            case Types.DECIMAL://MysqlDefs.FIELD_TYPE_NEW_DECIMAL n
            case Types.VARBINARY:// MysqlDefs.FIELD_TYPE_TINY_BLOB n
            case Types.LONGVARBINARY://MysqlDefs.FIELD_TYPE_BLOB n
            case 27://sqlserver.image MysqlDefs.FIELD_TYPE_BLOB n
            case Types.VARCHAR://MysqlDefs.FIELD_TYPE_VAR_STRING
            case Types.CHAR://MysqlDefs.FIELD_TYPE_STRING
            case Types.BINARY://MysqlDefs.FIELD_TYPE_GEOMETRY
            case Types.CLOB://MysqlDefs.FIELD_TYPE_VAR_STRING
            case Types.BLOB://MysqlDefs.FIELD_TYPE_BLOB
            case Types.NVARCHAR://MysqlDefs.FIELD_TYPE_VAR_STRING
            case Types.NCHAR://MysqlDefs.FIELD_TYPE_STRING
            case Types.NCLOB://MysqlDefs.FIELD_TYPE_VAR_STRING
            case Types.LONGNVARCHAR://MysqlDefs.FIELD_TYPE_VAR_STRING
            default:
                // MysqlDefs.FIELD_TYPE_VAR_STRING
                value = convertString(object);
        }
        return value;
    }

    public static byte[] converBit(Object object) {
        byte[] bytes;
        if (object instanceof byte[]) {
            bytes = (byte[]) object;
        } else if (object instanceof Boolean) {
            long i = (Boolean) object ? 1 : 0;
            bytes= new byte[]{(byte) i};
        } else if (object instanceof Number) {
            long i = ((Number) object).longValue();
            bytes = ByteUtil.getBytes((long)i);
        } else {
            bytes = object.toString().getBytes();
        }
        MySQLPayloadWriter mySQLPayloadWriter = new MySQLPayloadWriter(bytes.length);
        mySQLPayloadWriter.writeLenencBytes(bytes);
        return mySQLPayloadWriter.toByteArray();
    }

    public static byte[] convertToFloat32(Number object) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putFloat(object.floatValue()).array();
    }

    public static byte[] convertToInt64(Number object) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(object.longValue()).array();
    }

    public static byte[] convertString(Object object) {
        byte[] bytes;
        if (object instanceof byte[]) {
            bytes = (byte[]) object;
        } else {
            bytes = object.toString().getBytes();
        }
        MySQLPayloadWriter mySQLPayloadWriter = new MySQLPayloadWriter(bytes.length);
        mySQLPayloadWriter.writeLenencBytes(bytes);
        return mySQLPayloadWriter.toByteArray();
    }

    public static byte[] convertToFloat64(Number object) {
        return ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putDouble(object.doubleValue()).array();
    }

    public static byte[] convertToInt32(Number object) {
        return ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(object.intValue()).array();
    }

    public static byte[] convertToInt16(Number object) {
        return ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN).putShort(object.shortValue()).array();
    }

    public static byte[] convertLong(Number object) {
        return ByteUtil.getBytes(object.longValue());
    }

    public static byte[] convertInt(Number object) {
        return ByteUtil.getBytes(object.intValue());
    }

    public static byte[] convertToByte(Object object) {
        if (object == Boolean.TRUE) {
            return ByteUtil.getBytes(1);
        } else if (object == Boolean.FALSE) {
            return ByteUtil.getBytes(0);
        } else if (object instanceof Number) {
            byte b = ((Number) object).byteValue();
            return new byte[]{0};
        } else {
            throw new UnsupportedOperationException();
        }
    }

    public static byte[] convertToTiny(Object object) {
        if (object == Boolean.TRUE) {
            return new byte[]{1};
        } else if (object == Boolean.FALSE) {
            return new byte[]{0};
        } else if (object instanceof Number) {
            byte b = ((Number) object).byteValue();
            return new byte[]{b};
        } else if (object instanceof String) {
            return new byte[]{(byte) Integer.parseInt((String) object)};
        } else {
            throw new UnsupportedOperationException();
        }
    }
}