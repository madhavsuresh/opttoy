package org.apache.calcite.adapter.opttoy;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.apache.calcite.rel.logical.LogicalJoin;

public class OptToyDummyRule extends ConverterRule {

  // TODO(madhavsuresh): this isn't normally public.
  public OptToyDummyRule() {
    super(LogicalJoin.class, Convention.NONE, OptToyConvention.INSTANCE, "OptToyDummyRule");
    System.out.println("In Opt Toy Dummy Rule");
  }

  @Override
  public RelNode convert(RelNode rel) {
    LogicalJoin join = (LogicalJoin) rel;
    RelNode newRel;
    RelTraitSet traitSet = join.getTraitSet().replace(OptToyConvention.INSTANCE);
    //
    return null;
  }
}
