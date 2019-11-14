import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * @Author:
 * @Description:
 * @Date: 2019/11/14
 */
public class LoadingInfoCollect {
    public static void main(String[] args) {

    }

    /**
     * Using 'explain' statement in mysql to generate query plan
     *
     * @param connection
     * @param _sql
     * @return
     */
    private static List<HashMap<String, String>> queryPlanGenerate(Connection connection, String _sql) {
        List<HashMap<String, String>> queryPlan = new ArrayList<>();
        String sql = String.format("explain %s", _sql);
        ResultSet resultSet = Common.query(connection, sql);
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                HashMap rowData = new HashMap();
                for (int i = 1; i <= columnCount; i++) {
                    rowData.put(metaData.getColumnName(i), resultSet.getObject(i));
                }
                queryPlan.add(rowData);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return queryPlan;
    }

    /**
     * Generate query tree according to query plan
     *
     * @param queryPlan
     * @return
     */
    private static QueryNode queryTreeGenerate(List<HashMap<String, String>> queryPlan) {
        QueryNode start = new QueryNode(NodeType.LEAF_NODE);
        QueryNode currentNode = start;
        HashMap<String, String> plan = queryPlan.get(0);
        start.condition = plan.get("table");
        if (plan.get("Extra").contains("Using where")) {
            currentNode.parent = new QueryNode(NodeType.SELECT_NODE);
            currentNode.parent.leftChild = currentNode;
            currentNode = currentNode.parent;
        }
        for (int index = 1; index < queryPlan.size(); index++) {
            plan = queryPlan.get(index);
            currentNode.parent = new QueryNode(NodeType.JOIN_NODE);
            currentNode.parent.condition = plan.get("ref");
            currentNode.parent.leftChild = currentNode;
            currentNode.parent.rightChild = new QueryNode(NodeType.LEAF_NODE);
            currentNode.parent.rightChild.condition = plan.get("table");
            currentNode = currentNode.parent;
            if (plan.get("Extra") != null && plan.get("Extra").contains("Using where")) {
                currentNode.parent = new QueryNode(NodeType.SELECT_NODE);
                currentNode.parent.leftChild = currentNode;
                currentNode = currentNode.parent;
            }
        }
        return start;
    }

    @Test
    public void testQueryTreeGenerate() throws SQLException {
        Connection connection = Common.connect("59.78.194.63", "tpch", "root", "OpenSource");
        List<HashMap<String, String>> queryPlan = queryPlanGenerate(connection, Common.getSql("sql/3.sql"));
        QueryNode start = queryTreeGenerate(queryPlan);
        while (start != null) {
            System.out.println(start.nodeType);
            start = start.parent;
        }
        connection.close();
    }
}
