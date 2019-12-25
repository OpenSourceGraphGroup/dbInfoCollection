package loadingInfoCollector;

/**
 * @Author: Xin Jin, Ran Wang
 * @Description: Query Tee Structure
 */
enum NodeType {
    JOIN_NODE, SELECT_NODE, LEAF_NODE
}

interface QueryNodeProcessor {
    void process(QueryNode queryNode);
}

class QueryNode {
    int count;
    NodeType nodeType;
    QueryNode parent;
    QueryNode leftChild;
    QueryNode rightChild;
    String condition;
    String sql;

    public String toString() {
        return this.nodeType + " " + this.condition + "\r\n";
    }

    /**
     * Constructor
     * @param nodeType
     * @param leftChild
     * @param rightChild
     * @param condition
     */
    QueryNode(NodeType nodeType, QueryNode leftChild, QueryNode rightChild, String condition) {
        this.nodeType = nodeType;
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.condition = condition;
    }

    /**
     * Process node using post order
     * @param processor
     */
    void postOrder(QueryNodeProcessor processor) {
        if (leftChild != null)
            leftChild.postOrder(processor);
        if (rightChild != null)
            rightChild.postOrder(processor);
        processor.process(this);
    }
}
