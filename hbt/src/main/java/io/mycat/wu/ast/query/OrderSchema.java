package io.mycat.wu.ast.query;

import io.mycat.wu.Op;
import io.mycat.wu.ast.base.NodeVisitor;
import io.mycat.wu.ast.base.OrderItem;
import io.mycat.wu.ast.base.Schema;
import lombok.Data;

import java.util.Collections;
import java.util.List;

@Data
public class OrderSchema extends Schema {
    private final Schema schema;
    private final List<OrderItem> orders;

    public OrderSchema(Schema schema, List<OrderItem> fields) {
        super(Op.ORDER);
        this.schema = schema;
        this.orders = fields;
    }

    @Override
    public List<FieldType> fields() {
        return Collections.unmodifiableList(schema.fields());
    }

    public Schema getSchema() {
        return schema;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }
}
