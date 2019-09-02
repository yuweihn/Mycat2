package cn.lightfish.sqlEngine.persistent;

import cn.lightfish.sqlEngine.schema.MycatTable;
import cn.lightfish.sqlEngine.schema.TableColumnDefinition;

import java.util.Iterator;

public class QueryPersistentImpl implements QueryPersistent {

  private final MycatTable table;
  private final Iterator<Object[]> rows;


  public QueryPersistentImpl(MycatTable table, Iterator<Object[]> rows) {
    this.table = table;
    this.rows = rows;
  }

  @Override
  public TableColumnDefinition[] columnDefList() {
    return table.getColumnDefinitions();
  }

  @Override
  public boolean hasNext() {
    return rows.hasNext();
  }

  @Override
  public Object[] next() {
    return rows.next();
  }
}