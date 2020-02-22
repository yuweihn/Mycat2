/**
 * Copyright (C) <2019>  <chen junwen>
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
package io.mycat.calcite;

import com.google.common.collect.ImmutableList;
import io.mycat.calcite.logic.MycatConvention;
import io.mycat.calcite.logic.MycatTransientSQLTable;
import io.mycat.calcite.logic.MycatTransientSQLTableScan;
import org.apache.calcite.avatica.util.ByteString;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.dialect.MysqlSqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;

/**
 * @author Junwen Chen
 **/
public class MycatRelBuilder extends RelBuilder {
    public MycatRelBuilder(Context context, RelOptCluster cluster, RelOptSchema relOptSchema) {
        super(context, cluster, relOptSchema);
    }
    public static RelNode makeTransientSQLScan(RelBuilder relBuilder, String targetName, RelNode input) {
        RelDataType rowType = input.getRowType();
        MycatConvention convention = MycatConvention.of(targetName, MysqlSqlDialect.DEFAULT);
        MycatTransientSQLTable transientTable = new MycatTransientSQLTable(convention, input);
        RelOptTable relOptTable = RelOptTableImpl.create(
                relBuilder.getRelOptSchema(),
                rowType,
                transientTable,
                ImmutableList.of(targetName,String.valueOf(input.getId())));
        return new MycatTransientSQLTableScan(input.getCluster(), convention, relOptTable, input);
    }
    public  RelNode makeTransientSQLScan(String targetName, RelNode input) {
        return makeTransientSQLScan(this,targetName,input);
    }
    /**
     * Creates a literal (constant expression).
     */
    public static RexNode literal(RelDataType type, Object value, boolean allowCast) {
        final RexBuilder rexBuilder = MycatCalciteContext.INSTANCE.RexBuilder;
        JavaTypeFactoryImpl typeFactory = MycatCalciteContext.INSTANCE.TypeFactory;
        RexNode literal;
        if (value == null) {
            literal = rexBuilder.makeNullLiteral(typeFactory.createSqlType(SqlTypeName.NULL));
        } else if (value instanceof Boolean) {
            literal = rexBuilder.makeLiteral((Boolean) value);
        } else if (value instanceof BigDecimal) {
            literal = rexBuilder.makeExactLiteral((BigDecimal) value);
        } else if (value instanceof Float || value instanceof Double) {
            literal = rexBuilder.makeApproxLiteral(BigDecimal.valueOf(((Number) value).doubleValue()));
        } else if (value instanceof Number) {
            literal = rexBuilder.makeExactLiteral(BigDecimal.valueOf(((Number) value).longValue()));
        } else if (value instanceof String) {
            literal = rexBuilder.makeLiteral((String) value);
        } else if (value instanceof Enum) {
            literal = rexBuilder.makeLiteral(value, typeFactory.createSqlType(SqlTypeName.SYMBOL), false);
        } else if (value instanceof byte[]) {
            literal = rexBuilder.makeBinaryLiteral(new ByteString((byte[]) value));
        } else if (value instanceof LocalDate) {
            LocalDate value1 = (LocalDate) value;
            DateString dateString = new DateString(value1.getYear(), value1.getMonthValue(), value1.getDayOfMonth());
            literal = rexBuilder.makeDateLiteral(dateString);
        } else if (value instanceof LocalTime) {
            LocalTime value1 = (LocalTime) value;
            TimeString timeString = new TimeString(value1.getHour(), value1.getMinute(), value1.getSecond());
            literal = rexBuilder.makeTimeLiteral(timeString, -1);
        } else if (value instanceof LocalDateTime) {
            LocalDateTime value1 = (LocalDateTime) value;
            TimestampString timeString = new TimestampString(value1.getYear(), value1.getMonthValue(), value1.getDayOfMonth(), value1.getHour(), value1.getMinute(), value1.getSecond());
            timeString = timeString.withNanos(value1.getNano());
            literal = rexBuilder.makeTimestampLiteral(timeString, -1);
        } else {
            throw new IllegalArgumentException("cannot convert " + value
                    + " (" + value.getClass() + ") to a constant");
        }
        if (allowCast) {
            return rexBuilder.makeCast(type, literal);
        } else {
            return literal;
        }
    }

}