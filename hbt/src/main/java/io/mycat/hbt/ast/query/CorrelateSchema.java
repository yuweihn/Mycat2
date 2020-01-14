/**
 * Copyright (C) <2020>  <chen junwen>
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
package io.mycat.hbt.ast.query;

import io.mycat.hbt.Op;
import io.mycat.hbt.ast.base.Identifier;
import io.mycat.hbt.ast.base.NodeVisitor;
import io.mycat.hbt.ast.base.Schema;
import lombok.Data;

import java.util.List;

/**
 * @author jamie12221
 **/
@Data
public class CorrelateSchema extends Schema {
    private final List<Identifier> columnName;
    private final Schema left;
    private final Schema right;
    private Op op;
    private Identifier refName;

    public CorrelateSchema(Op op, Identifier refName, List<Identifier> columnName, Schema left, Schema right) {
        super(op);
        this.op = op;
        this.columnName = columnName;
        this.left = left;
        this.right = right;
        this.refName = refName;
    }

    @Override
    public List<FieldType> fields() {
        return null;
    }

    @Override
    public void accept(NodeVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String getAlias() {
        return null;
    }
}