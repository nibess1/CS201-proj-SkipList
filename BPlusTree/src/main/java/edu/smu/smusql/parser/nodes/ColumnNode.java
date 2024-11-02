package edu.smu.smusql.parser.nodes;

// Node for column references
public class ColumnNode extends ExpressionNode {
    String name;

    public ColumnNode(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }
}