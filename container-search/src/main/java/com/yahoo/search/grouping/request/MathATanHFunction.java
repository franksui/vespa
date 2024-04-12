// Copyright Vespa.ai. Licensed under the terms of the Apache 2.0 license. See LICENSE in the project root.
package com.yahoo.search.grouping.request;

import java.util.List;

/**
 * @author baldersheim
 * @author bratseth
 */
public class MathATanHFunction extends FunctionNode {

    /**
     * Constructs a new instance of this class.
     *
     * @param exp The expression to evaluate, double value will be requested.
     */
    public MathATanHFunction(GroupingExpression exp) {
        this(null, null, exp);
    }

    private MathATanHFunction(String label, Integer level, GroupingExpression exp) {
        super("math.atanh", label, level, List.of(exp));
    }

    @Override
    public MathATanHFunction copy() {
        return new MathATanHFunction(getLabel(), getLevelOrNull(), getArg(0).copy());
    }

}
