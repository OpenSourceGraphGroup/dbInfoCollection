import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.json.JSONObject;
import org.junit.Test;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @Author: XinJin
 * @Description: 查询树生成器
 * @Date: 2019/11/15
 */
public class QueryTreeGenerator {
    @Test
    public void testQueryTreeGenerate() throws SQLException, JSQLParserException {
            Connection connection = Common.connect("59.78.194.63", "tpch", "root", "OpenSource");
            QueryNode queryNode = generate(connection, Common.getSql("sql/" + 1 + ".sql"));
            queryNode.postOrder(queryNode1 -> System.out.println(queryNode1.nodeType + " " + queryNode1.condition));
            connection.close();
            System.out.println();
    }

    /**
     * Generate query tree according to sql
     *
     * @param connection
     * @param sql
     * @return
     * @throws JSQLParserException
     */
    private static QueryNode generate(Connection connection, String sql) throws JSQLParserException {
        List<QueryPlan> queryPlan = queryPlanGenerate(connection, sql);
        Map<String, String> tableAlias = getTableAlias(sql);
        if (queryPlan.isEmpty()) return null;
        QueryPlan plan = queryPlan.get(0);
        String tableName = tableAlias.getOrDefault(plan.tableName, plan.tableName);
        QueryNode queryNode = new QueryNode(NodeType.LEAF_NODE, null, null, tableName);
        // Generate SELECT_NODE
        if (!plan.attachedCondition.equals("")) {
            queryNode.parent = new QueryNode(NodeType.SELECT_NODE, queryNode, null, plan.attachedCondition);
            queryNode = queryNode.parent;
        }
        if (!plan.subQueries.isEmpty()) {
            queryNode = generate(connection, plan.subQueries, 0, tableAlias, queryNode);
        }
        return generate(connection, queryPlan, 1, tableAlias, queryNode);
    }

    private static QueryNode generate(Connection connection, List<QueryPlan> queryPlan, int start, Map<String, String> tableAlias, QueryNode queryNode) {
        for (int index = start; index < queryPlan.size(); index++) {
            QueryPlan plan = queryPlan.get(index);
            String tableName = tableAlias.getOrDefault(plan.tableName, plan.tableName);
            QueryNode newNode = new QueryNode(NodeType.LEAF_NODE, null, null, tableName);


            // Generate JOIN_NODE
            String joinNodeCondition = plan.ref.isEmpty() ? "" : plan.ref.get(0) + " = " + getJoinKey(connection, plan.key, tableName);
            newNode.parent = new QueryNode(NodeType.JOIN_NODE, queryNode, newNode, joinNodeCondition);
            queryNode = newNode.parent;

            // Generate SELECT_NODE
            if (!plan.attachedCondition.equals("")) {
                queryNode.parent = new QueryNode(NodeType.SELECT_NODE, queryNode, null, plan.attachedCondition);
                queryNode = queryNode.parent;
            }


            if (!plan.subQueries.isEmpty()) {
                queryNode = generate(connection, plan.subQueries, 0, tableAlias, queryNode);
            }
        }
        return queryNode;
    }


    /**
     * Using 'explain' statement in mysql to generate query plan
     *
     * @param connection
     * @param sql
     * @return
     */
    private static List<QueryPlan> queryPlanGenerate(Connection connection, String sql) {
        sql = String.format("explain format = json %s", sql);
        ResultSet resultSet = Common.query(connection, sql);
        try {
            if (resultSet.next()) {
                String planJson = resultSet.getObject(1).toString();
                Map<String, Object> json = new JSONObject(planJson).toMap();
                List<QueryPlan> results = new ArrayList<>();
                getPlan(json, results);
                return results;
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return new ArrayList<>();
    }


    /**
     * Get LEAF_NODE'S condition
     *
     * @param sql
     * @return
     * @throws JSQLParserException
     */
    private static Map<String, String> getTableAlias(String sql) throws JSQLParserException {
        Map<String, String> condition = new HashMap<>();
        PlainSelect selectBody = (PlainSelect) ((Select) CCJSqlParserUtil.parse(sql)).getSelectBody();
        while (selectBody != null) {
            List<Join> tables = selectBody.getJoins();
            if (tables != null) {
                for (Join join : tables) {
                    Table table = (Table) join.getRightItem();
                    if (table.getAlias() != null) {
                        condition.put(table.getAlias().getName(), table.getName());
                    }
                }
            }
            if (selectBody.getFromItem().getClass().equals(Table.class)) {
                Table table = (Table) selectBody.getFromItem();
                if (table.getAlias() != null) {
                    condition.put(table.getAlias().getName(), table.getName());
                }
                break;
            } else {
                selectBody = (PlainSelect) ((SubSelect) selectBody.getFromItem()).getSelectBody();
            }
        }
        return condition;
    }

    /**
     * Get join key in 'tableName' whose index column is 'indexName'
     *
     * @param connection
     * @param indexName
     * @param tableName
     * @return
     */
    private static String getJoinKey(Connection connection, String indexName, String tableName) {
        String sql = String.format("select COLUMN_NAME from INFORMATION_SCHEMA.STATISTICS WHERE INDEX_NAME = '%s' and TABLE_NAME = '%s'", indexName, tableName);
        try {
            ResultSet resultSet = Common.query(connection, sql);
            if (resultSet.next())
                return resultSet.getString("COLUMN_NAME");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return "";
    }

    private static void getPlan(Object object, List<QueryPlan> plans) {
        if (Map.class.isAssignableFrom(object.getClass())) {
            Map map = (Map) object;
            if (map.containsKey("table")) {
                plans.add(new QueryPlan((Map) (map.get("table"))));
            } else {
                for (Object subObject : map.values()) {
                    getPlan(subObject, plans);
                }
            }
        } else if (List.class.isAssignableFrom(object.getClass())) {
            List list = (List) object;
            for (Object subObject : list) {
                getPlan(subObject, plans);
            }
        }
    }

    private static class QueryPlan implements Serializable {
        String tableName;
        String key;
        List<String> ref;
        String attachedCondition;
        List<String> usedColumns;
        List<QueryPlan> subQueries = new ArrayList<>();

        QueryPlan(Map<String, Object> object) {
            ref = (ArrayList<String>) object.getOrDefault("ref", new ArrayList<>());
            key = object.getOrDefault("key", "").toString();
            tableName = object.getOrDefault("table_name", "").toString().replace("`", "");
            attachedCondition = object.getOrDefault("attached_condition", "").toString().replace("`", "").replace("<cache>", "");
            usedColumns = (ArrayList<String>) object.getOrDefault("used_columns", new ArrayList<>());
            List subQueryJson = (List) object.getOrDefault("attached_subqueries", new ArrayList<>());
            getPlan(subQueryJson, subQueries);
        }
    }
}
