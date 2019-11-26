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

    public QueryNode(NodeType type, String condition) {
        this.count = 0;
        this.nodeType = type;
        this.parent = null;
        this.leftChild = null;
        this.rightChild = null;
        this.condition = condition;
    }

    QueryNode(NodeType nodeType, QueryNode leftChild, QueryNode rightChild, String condition) {
        this.nodeType = nodeType;
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.condition = condition;
    }

    public int getCount() {
        return count;
    }

    public void setCount(int count) {
        this.count = count;
    }

    public NodeType getNodeType() {
        return nodeType;
    }

    public void setNodeType(NodeType nodeType) {
        this.nodeType = nodeType;
    }

    void postOrder(QueryNodeProcessor processor) {
        if (leftChild != null)
            leftChild.postOrder(processor);
        if (rightChild != null)
            rightChild.postOrder(processor);
        processor.process(this);
    }

    public QueryNode getParent() {
        return parent;
    }

    public void setParent(QueryNode parent) {
        this.parent = parent;
    }

    public QueryNode getLeftChild() {
        return leftChild;
    }

    public void setLeftChild(QueryNode leftChild) {
        this.leftChild = leftChild;
    }

    public QueryNode getRightChild() {
        return rightChild;
    }

    public void setRightChild(QueryNode rightChild) {
        this.rightChild = rightChild;
    }

    public String getCondition() {
        return condition;
    }

    public void setCondition(String condition) {
        this.condition = condition;
    }
}
