package io.mycat.calcite.sqlfunction;

import io.mycat.calcite.UnsolvedMysqlFunctionUtil;
import org.apache.calcite.linq4j.function.Parameter;

import java.time.LocalDate;
import java.time.format.TextStyle;
import java.util.Locale;
import java.util.Objects;

import static java.time.format.DateTimeFormatter.BASIC_ISO_DATE;

public  class DateFormatFunction {
        public static String eval(@Parameter(name = "date") String dateText, @Parameter(name = "format") String format) {
            LocalDate date = LocalDate.parse(dateText, BASIC_ISO_DATE);

            Locale locale = Locale.getDefault();
            format = format.replaceAll("%a", date.getDayOfWeek().getDisplayName(TextStyle.SHORT, locale));
            format = format.replaceAll("%b", date.getMonth().getDisplayName(TextStyle.SHORT, locale));
            format = format.replaceAll("%c", date.getMonthValue() + "");
            format = format.replaceAll("%Y", String.format("%04d", date.getYear()));
            format = format.replaceAll("%y", String.format("%02d", date.getYear()));
            format = format.replaceAll("%m", String.format("%02d", date.getMonthValue()));
            format = format.replaceAll("%M", date.getMonth().getDisplayName(TextStyle.FULL, locale));
            format = format.replaceAll("%d", String.format("%02d", date.getDayOfMonth()));
            format = format.replaceAll("%e", String.format("%01d", date.getDayOfMonth()));
            format = format.replaceAll("%c", String.format("%01d", date.getMonthValue()));

            if (!format.contains("%")) {
                return format;
            }
            return Objects.toString(UnsolvedMysqlFunctionUtil.eval("data_format", dateText, format));
        }
    }