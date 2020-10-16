package io.mycat.calcite.sqlfunction.stringfunction;


import org.apache.calcite.schema.ScalarFunction;
import org.apache.calcite.schema.impl.ScalarFunctionImpl;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.*;

import java.nio.ByteBuffer;


public class OrdFunction extends MycatStringFunction {
    public static ScalarFunction scalarFunction = ScalarFunctionImpl.create(OrdFunction.class,
            "ord");

    public static final OrdFunction INSTANCE = new OrdFunction();

    public OrdFunction() {
        super("ord", scalarFunction);
    }

    public static Integer ord(String str) {
        if (str == null) {
            return null;
        }
        if (str.isEmpty()) {
            return 0;
        }
        char c = str.charAt(0);
        ByteBuffer allocate = ByteBuffer.allocate(4);
        allocate.putChar(c);
        allocate.position(0);
        return allocate.getInt();
    }
}