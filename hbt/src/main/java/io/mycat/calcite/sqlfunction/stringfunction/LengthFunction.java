package io.mycat.calcite.sqlfunction.stringfunction;


import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;

public class LengthFunction  extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(LengthFunction.class,
            "length");

    public static final LengthFunction INSTANCE = new LengthFunction();

    public LengthFunction() {
        super("length", scalarFunction);
    }

    public static Integer length(String str) {
        if (str == null) {
            return null;
        }
        return str.length();
    }
}