
/**
 * @Author:
 * @Description:
 * @Date: 2019/11/14
 */
enum NodeType {
    JOIN_NODE, SELECT_NODE, LEAF_NODE
}

class QueryNode {
    int count;
    NodeType nodeType;
    QueryNode parent;
    QueryNode leftChild;
    QueryNode rightChild;
    String condition;

    QueryNode(NodeType nodeType) {
        this.nodeType = nodeType;
    }
}
