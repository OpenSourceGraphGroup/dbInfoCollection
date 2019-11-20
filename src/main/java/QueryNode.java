
/**
 * @Author:
 * @Description:
 * @Date: 2019/11/14
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

    QueryNode(NodeType nodeType, QueryNode leftChild, QueryNode rightChild, String condition) {
        this.nodeType = nodeType;
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.condition = condition;
    }

    void postOrder(QueryNodeProcessor processor) {
        if (leftChild != null)
            leftChild.postOrder(processor);
        if (rightChild != null)
            rightChild.postOrder(processor);
        processor.process(this);
    }
}
