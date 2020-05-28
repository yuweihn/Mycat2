package io.mycat.upondb;

import io.mycat.plug.sequence.SequenceGenerator;
import io.mycat.util.MySQLFunction;
import io.mycat.util.SQLContext;

import java.util.function.Supplier;

public class MysqlFunctions {
    public static final MySQLFunction next_value_for = new MySQLFunction() {
        @Override
        public String getFunctionName() {
            return "next_value_for";
        }

        @Override
        public int getArgumentSize() {
            return 1;
        }

        @Override
        public Object eval(SQLContext context, Object[] args) {
            String name = args[0].toString().replaceAll("MYCATSEQ_", "");
            Supplier<String> sequence = SequenceGenerator.INSTANCE.getSequence(name);
            String s = sequence.get();
            return s;
        }
    };

    public static final MySQLFunction last_insert_id = new MySQLFunction() {
        @Override
        public String getFunctionName() {
            return "last_insert_id";
        }

        @Override
        public int getArgumentSize() {
            return 0;
        }

        @Override
        public Object eval(SQLContext context, Object[] args) {
            return context.lastInsertId();
        }
    };

    //SELECT current_user() mysql workbench
    public static final MySQLFunction current_user = new MySQLFunction() {
        @Override
        public String getFunctionName() {
            return "current_user";
        }

        @Override
        public int getArgumentSize() {
            return 0;
        }

        @Override
        public Object eval(SQLContext context, Object[] args) {
            return context.getSQLVariantRef("current_user");
        }
    };
}