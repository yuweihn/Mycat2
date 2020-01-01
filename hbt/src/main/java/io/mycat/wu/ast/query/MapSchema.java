package io.mycat.wu.ast.query;

import io.mycat.wu.Op;
import io.mycat.wu.ast.base.Expr;
import io.mycat.wu.ast.base.NodeVisitor;
import io.mycat.wu.ast.base.Schema;
import lombok.Data;

import java.util.Collections;
import java.util.List;

@Data
public class MapSchema extends Schema {
    private final Schema schema;
    private final List<Expr> expr;

    public MapSchema(Schema schema, List<Expr> expr) {
        super(Op.MAP);
        this.schema = schema;
        this.expr = expr;
    }

    @Override
    public List<FieldType> fields() {
        return Collections.unmodifiableList(schema.fields());
    }

    public List<Expr> getExpr() {
        return expr;
    }

    public Schema getSchema() {
        return schema;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}