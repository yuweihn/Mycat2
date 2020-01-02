package io.mycat.sqlEngine.ast.expr.booleanExpr.compareExpr;

import io.mycat.sqlEngine.ast.expr.ValueExpr;
import io.mycat.sqlEngine.ast.expr.booleanExpr.BooleanExpr;
import io.mycat.sqlEngine.context.RootSessionContext;


public class BooleanIsExpr implements BooleanExpr {

  private final RootSessionContext context;
  private final ValueExpr expr;
  private final ValueExpr target;

  public BooleanIsExpr(RootSessionContext context, ValueExpr expr,
      ValueExpr target) {
    this.context = context;
    this.expr = expr;
    this.target = target;
    if (this.target.getType() != Integer.class) {
      throw new UnsupportedOperationException();
    }
  }

  @Override
  public Boolean test() {
    Comparable test = this.expr.getValue();
    Comparable target = this.target.getValue();
    if (test == null && target == null) {
      return true;
    }
    if (test == null && target != null) {
      return false;
    }
    if (test != null && target == null) {
      return false;
    }
    if (test != null && target != null) {
      Number targetValue = (Number) target;
      if ((test instanceof Number) && (targetValue instanceof Number)) {
        int testValue = ((Number) test).intValue();
        return (testValue == 0 && targetValue.intValue() == 0);
      } else {
        return targetValue.equals(test);
      }
    }
    return false;
  }
}