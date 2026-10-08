package flink.compile;

/**
 * Compiled condition tree node. {@link #cost()} is a static estimate used to order children so that
 * cheap raw-field checks run before aggregate reads (cost-based short-circuiting).
 */
public abstract class Node {

    protected String path = "";

    public abstract boolean eval(EvalContext ctx);

    public abstract int cost();

    public String path() {
        return path;
    }

    void assignPath(String p) {
        this.path = p;
    }

    public static final class Const extends Node {
        public static final Const TRUE = new Const(true);
        public static final Const FALSE = new Const(false);

        private final boolean value;

        private Const(boolean value) {
            this.value = value;
        }

        public boolean value() {
            return value;
        }

        @Override
        public boolean eval(EvalContext ctx) {
            return value;
        }

        @Override
        public int cost() {
            return 0;
        }
    }

    public static final class And extends Node {
        private final Node[] children;
        private final int cost;

        public And(Node[] children) {
            this.children = sortByCost(children);
            this.cost = sumCost(children);
        }

        public Node[] children() {
            return children;
        }

        @Override
        public boolean eval(EvalContext ctx) {
            for (Node c : children) {
                if (!c.eval(ctx)) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int cost() {
            return cost;
        }
    }

    public static final class Or extends Node {
        private final Node[] children;
        private final int cost;

        public Or(Node[] children) {
            this.children = sortByCost(children);
            this.cost = sumCost(children);
        }

        public Node[] children() {
            return children;
        }

        @Override
        public boolean eval(EvalContext ctx) {
            for (Node c : children) {
                if (c.eval(ctx)) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public int cost() {
            return cost;
        }
    }

    public static final class Not extends Node {
        private final Node child;

        public Not(Node child) {
            this.child = child;
        }

        public Node child() {
            return child;
        }

        @Override
        public boolean eval(EvalContext ctx) {
            return !child.eval(ctx);
        }

        @Override
        public int cost() {
            return child.cost();
        }
    }

    static Node[] sortByCost(Node[] nodes) {
        Node[] copy = nodes.clone();
        java.util.Arrays.sort(copy, java.util.Comparator.comparingInt(Node::cost));
        return copy;
    }

    static int sumCost(Node[] nodes) {
        long s = 0;
        for (Node n : nodes) {
            s += n.cost();
        }
        return (int) Math.min(Integer.MAX_VALUE, s);
    }
}
