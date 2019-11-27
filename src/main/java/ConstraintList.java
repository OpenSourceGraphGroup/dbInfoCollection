import org.junit.Test;

import java.util.*;

public class ConstraintList {
    public List<String> getConstraintList(QueryNode root){
        if(root == null){
            return new ArrayList<>();
        }

        List<QueryNode> queryNodeList = new ArrayList<>();
        root.postOrderNodes(queryNodeList);

        Map<String, Integer> tableMap = new HashMap<>();
        List<String> tableConstraints = new LinkedList<>();
        int index = 0;
        for(QueryNode node: queryNodeList){
            double filterRate;
            String tableName;
            if(node.nodeType == NodeType.LEAF_NODE){
                //String condition = node.condition;
                if(node.parent != null) {
                    filterRate = (double) node.count / (double) node.parent.count;
                    if (node.parent.nodeType == NodeType.SELECT_NODE) {
                        // I need to know the table name, and what filter operations it uses(such as >, etc)
                        tableName = node.parent.getCondition();

                        String whereOps = "c_mktsegment@=";
                        int idx = tableMap.getOrDefault(tableName, index);
                        if (idx == index) {
                            tableMap.put(tableName, idx);
                            tableConstraints.add("[" + tableName + "]");
                            index++;
                        }
                        StringBuilder tableConstraintStr = new StringBuilder(tableConstraints.get(idx));
                        tableConstraintStr.append("; [0, ").append(whereOps)
                                .append(", ").append(filterRate).append("]");
                        tableConstraints.set(idx, tableConstraintStr.toString());
                    } else {
                        // parent is a join node, I need to know the table name, and if there exists any attributes
                        // participating in this join as primary key or foreign key
                    }
                }
            }else if(node.nodeType == NodeType.SELECT_NODE){
                if(node.parent != null){
                    if(node.parent.nodeType == NodeType.JOIN_NODE){
                        // I need to know the table name, and if there exists any attributes participating in this
                        // join as primary key or foreigin key
                    }else{
                        System.out.println("I think there must be sth wrong with the parsed tree, where there exists " +
                                "a SELECT NODE whose parent node is also a SELECT NODE");
                    }
                }
            }else if(node.nodeType == NodeType.JOIN_NODE){
                if(node.parent != null){
                    if(node.parent.nodeType == NodeType.JOIN_NODE){

                    }else if(node.parent.nodeType == NodeType.SELECT_NODE){

                    }else{
                        System.out.println("I think there must be sth wrong with the parsed tree, where there exists " +
                                "a JOIN NODE whose parent node is a LEAF NODE");
                    }
                }
            }
        }
        return tableConstraints;
    }

    @Test
    public void testPostOrderNodes(){
        QueryNode root = new QueryNode(NodeType.JOIN_NODE, "Condition 1");
        root.leftChild = new QueryNode(NodeType.SELECT_NODE, "Condition 2");
        root.leftChild.leftChild = new QueryNode(NodeType.LEAF_NODE, "Condition 3");
        root.rightChild = new QueryNode(NodeType.LEAF_NODE, "Condition 4");

        List<QueryNode> nodes = new ArrayList<>();
        root.postOrderNodes(nodes);

        for(QueryNode node : nodes){
            System.out.println("Node type: " + node.nodeType + ", condition: " + node.condition);
        }
    }
}
