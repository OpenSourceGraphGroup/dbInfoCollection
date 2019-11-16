import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class ConstraintList {
    public List<String> getConstraintList(List<QueryNode> queryNodeList){
        Map<String, Integer> tableMap = new HashMap<>();
        List<String> tableConstraints = new LinkedList<>();
        int index = 0;
        for(QueryNode node: queryNodeList){
            if(node.nodeType == NodeType.LEAF_NODE){
                //String condition = node.condition;
                if(node.parent != null) {
                    double filterRate = (double) node.count / (double) node.parent.count;
                    String tableName = "customer";
                    if (node.parent.nodeType == NodeType.SELECT_NODE) {
                        // I need to know the table name, and what filter operations it uses(such as >, etc)
                        String whereOps = "c_mktsegment@=";
                        int idx = tableMap.getOrDefault(tableName, index);
                        if (idx == index) {
                            tableMap.put(tableName, idx);
                            tableConstraints.add("[" + tableName + "]");
                            index++;
                        }
                        String tableConstraintStr = tableConstraints.get(idx);
                        tableConstraintStr += "; [0, " + whereOps + ", " + filterRate + "]";
                        tableConstraints.set(idx, tableConstraintStr);
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
                                "a SELECT NODE whose parent node is a SELECT NODE");
                    }
                }
            }
        }
        return tableConstraints;
    }
}
