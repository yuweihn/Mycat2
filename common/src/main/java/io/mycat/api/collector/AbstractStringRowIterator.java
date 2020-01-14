package io.mycat.api.collector;

import io.mycat.beans.mycat.MycatRowMetaData;
import io.mycat.logTip.MycatLogger;
import io.mycat.logTip.MycatLoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Iterator;

/**
 *
 * chen junwen
 *
 * a iterator,transform text to object
 */
public abstract class AbstractStringRowIterator implements RowBaseIterator {
    final static MycatLogger LOGGER = MycatLoggerFactory
            .getLogger(AbstractStringRowIterator.class);

    protected final MycatRowMetaData mycatRowMetaData;
    protected final Iterator<String[]> iterator;
    private String[] currentRow;
    private boolean wasNull;

    public AbstractStringRowIterator(MycatRowMetaData mycatRowMetaData, Iterator<String[]> iterator) {
        this.mycatRowMetaData = mycatRowMetaData;
        this.iterator = iterator;
    }

    @Override
    public MycatRowMetaData metaData() {
        return mycatRowMetaData;
    }

    @Override
    public boolean next() {
        if (this.iterator.hasNext()) {
            this.currentRow = this.iterator.next();
            return true;
        } else {
            return false;
        }
    }

    @Override
    public boolean wasNull() {
        return wasNull;
    }

    @Override
    public String getString(int columnIndex) {
        String o = getString(currentRow[columnIndex - 1]);
        return o;
    }

    @Override
    public boolean getBoolean(int columnIndex) {
        String o = getString(currentRow[columnIndex - 1]);
        if (wasNull) return false;
        return Boolean.parseBoolean(o);
    }

    @Override
    public byte getByte(int columnIndex) {
        String o = getString(currentRow[columnIndex - 1]);
        if (wasNull) return 0;
        return Byte.parseByte(o);
    }

    @Override
    public short getShort(int columnIndex) {
        String o = getString(currentRow[columnIndex - 1]);
        if (wasNull) return 0;
        return Short.parseShort(o);
    }

    @Override
    public int getInt(int columnIndex) {
        String o = getString(currentRow[columnIndex - 1]);
        if (wasNull) return 0;
        return Integer.parseInt(o);
    }

    @Override
    public long getLong(int columnIndex) {
        String o = getString(currentRow[columnIndex - 1]);
        if (wasNull) return 0;
        return Long.parseLong(o);
    }

    @Override
    public float getFloat(int columnIndex) {
        String o = getString(currentRow[columnIndex - 1]);
        if (wasNull) return 0;
        return Float.parseFloat(o);
    }

    @Override
    public double getDouble(int columnIndex) {
        String o = getString(currentRow[columnIndex - 1]);
        if (wasNull) return 0;
        return Double.parseDouble(o);
    }

    @Override
    public byte[] getBytes(int columnIndex) {
        String o = getString(currentRow[columnIndex - 1]);
        if (wasNull) return null;
        return o.getBytes();
    }

    @Override
    public Date getDate(int columnIndex) {
        String o = getString(currentRow[columnIndex - 1]);
        if (wasNull) return null;
        return java.sql.Date.valueOf(o);
    }

    @Override
    public Time getTime(int columnIndex) {
        String o = getString(currentRow[columnIndex - 1]);
        if (wasNull) return null;
        return java.sql.Time.valueOf(o);
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) {
        String o = getString(currentRow[columnIndex - 1]);
        if (wasNull) return null;
        return java.sql.Timestamp.valueOf(o);
    }

    @Override
    public InputStream getAsciiStream(int columnIndex) {
        return getBinaryStream(columnIndex);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) {
        String o = getString(currentRow[columnIndex - 1]);
        if (wasNull) return null;
        return new ByteArrayInputStream(o.getBytes());
    }

    private String getString(String o1) {
        String o = o1;
        wasNull = o == null;
        return o;
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) {
        String o = getString(currentRow[columnIndex - 1]);
        if (wasNull) return null;
        return new BigDecimal(o);
    }
    @Override
    public Object getObject(int columnIndex) {
        int columnType = mycatRowMetaData.getColumnType(columnIndex);
        switch (columnType) {
            case Types.NUMERIC: {

            }
            case Types.DECIMAL: {
                return this.getBigDecimal(columnIndex);
            }
            case Types.BIT: {
                return this.getBoolean(columnIndex);
            }
            case Types.TINYINT: {
                return this.getByte(columnIndex);
            }
            case Types.SMALLINT: {
                return this.getShort(columnIndex);
            }
            case Types.INTEGER: {
                return this.getInt(columnIndex);
            }
            case Types.BIGINT: {
                return this.getLong(columnIndex);
            }
            case Types.REAL: {
                return this.getFloat(columnIndex);
            }
            case Types.FLOAT: {

            }
            case Types.DOUBLE: {
                return this.getDouble(columnIndex);
            }
            case Types.BINARY: {

            }
            case Types.VARBINARY: {

            }
            case Types.LONGVARBINARY: {
                return this.getBytes(columnIndex);
            }
            case Types.DATE: {
                return this.getDate(columnIndex);
            }
            case Types.TIME: {
                return this.getTime(columnIndex);
            }
            case Types.TIMESTAMP: {
                return this.getTimestamp(columnIndex);
            }
            case Types.CHAR: {

            }
            case Types.VARCHAR: {

            }
            case Types.LONGVARCHAR: {
                return this.getString(columnIndex);
            }
            case Types.BLOB: {

            }
            case Types.CLOB: {
                return this.getBytes(columnIndex);
            }
            case Types.NULL: {
                return null;
            }
            default:
                throw new RuntimeException("unsupport!");
        }
    }

}