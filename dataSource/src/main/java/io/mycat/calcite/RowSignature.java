package io.mycat.calcite;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import javafx.util.Pair;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.type.SqlTypeName;

import java.nio.charset.Charset;
import java.sql.JDBCType;
import java.util.List;
import java.util.Map;

public class RowSignature {
    private final Map<String, JDBCType> columnTypes;
    private final List<String> columnNames;

    private RowSignature(final List<Pair<String, JDBCType>> columnTypeList) {
        final Map<String, JDBCType> columnTypes0 = Maps.newHashMap();
        final ImmutableList.Builder<String> columnNamesBuilder =
                ImmutableList.builder();

        int i = 0;
        for (Pair<String, JDBCType> pair : columnTypeList) {
            final JDBCType existingType = columnTypes0.get(pair.getKey());
            if (existingType != null && existingType != pair.getValue()) {
                // throw new exception
                throw new UnsupportedOperationException();
            }
            columnTypes0.put(pair.getKey(), pair.getValue());
            columnNamesBuilder.add(pair.getKey());
        }
        this.columnTypes = ImmutableMap.copyOf(columnTypes0);
        this.columnNames = columnNamesBuilder.build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public JDBCType getColumnType(final String name) {
        return columnTypes.get(name);
    }

    public List<String> getRowOrder() {
        return columnNames;
    }

    public RelDataType getRelDataType(final RelDataTypeFactory factory) throws Exception {
        final RelDataTypeFactory.Builder builder = new RelDataTypeFactory.Builder(factory);
        for (final String columnName : columnNames) {
            final JDBCType columnType = getColumnType(columnName);
            final RelDataType type;
            if (columnType == JDBCType.VARCHAR) {
                type = factory.createTypeWithCharsetAndCollation(
                        factory.createSqlType(SqlTypeName.VARCHAR),
                        Charset.defaultCharset(),
                        SqlCollation.IMPLICIT);
            } else {
                SqlTypeName sqlTypeName = SqlTypeName.getNameForJdbcType(columnType.getVendorTypeNumber());
                if (sqlTypeName == null) {
                    throw new UnsupportedOperationException();
                }
                type = factory.createSqlType(sqlTypeName);
            }
            builder.add(columnName, type);
        }
        return builder.build();
    }

    public static class Builder {
        private final List<Pair<String, JDBCType>> columnTypeList;

        private Builder() {
            this.columnTypeList = Lists.newArrayList();
        }

        public Builder add(String columnName, JDBCType columnType) {
            columnTypeList.add(new Pair<>(columnName, columnType));
            return this;
        }

        public RowSignature build() {
            return new RowSignature(columnTypeList);
        }
    }
    public int getColumnCount(){
        return this.columnNames.size();
    }
}
