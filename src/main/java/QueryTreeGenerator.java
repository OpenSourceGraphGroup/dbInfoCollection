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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author: XinJin
 * @Description: 查询树生成器
 * @Date: 2019/11/15
 */
public class QueryTreeGenerator {
    @Test
    public void testQueryTreeGenerate() throws SQLException, JSQLParserException {
        for (int i = 1; i < 17; i++) {
            Connection connection = Common.connect("59.78.194.63", "tpch", "root", "OpenSource");
            QueryNode queryNode = generate(connection, Common.getSql("sql/" + i + ".sql"), "tpch");
            if (queryNode != null) {
                queryNode.postOrder(queryNode1 -> System.out.println(queryNode1.nodeType + " " + queryNode1.condition));
            }
            connection.close();
            System.out.println("SQL " + i + " Complete.");
            System.out.println();
        }
    }

    /**
     * Generate query tree according to sql
     *
     * @param connection
     * @param sql
     * @return
     * @throws JSQLParserException
     */
    static QueryNode generate(Connection connection, String sql, String dbName) throws JSQLParserException {
        List<QueryPlan> queryPlan = queryPlanGenerate(connection, sql);
        Map<String, String> tableAlias = getTableAlias(sql);
        if (queryPlan.isEmpty()) return null;
        QueryPlan plan = queryPlan.get(0);
        QueryNode queryNode = null;
        if (!plan.tableName.equals("derived")) {
            String leafCondition = tableAlias.containsKey(plan.tableName) ? tableAlias.get(plan.tableName) + " " + plan.tableName : plan.tableName;
            queryNode = new QueryNode(NodeType.LEAF_NODE, null, null, leafCondition);
            // Generate SELECT_NODE
            String selectCondition = attachedConditionProcess(plan.attachedCondition);
            if (!selectCondition.equals("")) {
                queryNode.parent = new QueryNode(NodeType.SELECT_NODE, queryNode, null, selectCondition);
                queryNode = queryNode.parent;
            }
        }
        if (!plan.subQueries.isEmpty()) {
            queryNode = generate(plan.subQueries, 0, tableAlias, queryNode, dbName);
        }
        return generate(queryPlan, 1, tableAlias, queryNode, dbName);
    }

    private static QueryNode generate(List<QueryPlan> queryPlan, int start, Map<String, String> tableAlias, QueryNode queryNode, String dbName) {
        for (int index = start; index < queryPlan.size(); index++) {
            QueryPlan plan = queryPlan.get(index);
            if (!plan.tableName.equals("derived")) {
                String leafCondition = tableAlias.containsKey(plan.tableName) ? tableAlias.get(plan.tableName) + " " + plan.tableName : plan.tableName;
                QueryNode newNode = new QueryNode(NodeType.LEAF_NODE, null, null, leafCondition);
                if (plan.ref.size() > 0) {
                    if (queryNode != null) {
                        // Generate JOIN_NODE
                        StringBuilder joinNodeCondition = new StringBuilder();
                        for (int i = 0; i < plan.ref.size(); i++) {
                            joinNodeCondition.append(plan.ref.get(i)).append(" = ")
                                    .append(dbName).append(".")
                                    .append(plan.tableName).append(".")
                                    .append(plan.usedKey.get(i));
                            if (i != plan.ref.size() - 1) joinNodeCondition.append(" and ");
                        }
                        newNode.parent = new QueryNode(NodeType.JOIN_NODE, queryNode, newNode, joinNodeCondition.toString());
                        queryNode = newNode.parent;
                    } else {
                        queryNode = newNode;
                    }

                    // Generate SELECT_NODE
                    String selectCondition = attachedConditionProcess(plan.attachedCondition);
                    if (!selectCondition.equals("")) {
                        queryNode.parent = new QueryNode(NodeType.SELECT_NODE, queryNode, null, selectCondition);
                        queryNode = queryNode.parent;
                    }
                } else {
                    // Generate SELECT_NODE
                    String selectCondition = attachedConditionProcess(plan.attachedCondition);
                    if (!selectCondition.equals("")) {
                        newNode.parent = new QueryNode(NodeType.SELECT_NODE, newNode, null, selectCondition);
                        newNode = newNode.parent;
                    }
                    if (queryNode != null) {
                        queryNode.parent = new QueryNode(NodeType.JOIN_NODE, queryNode, newNode, "");
                        queryNode = queryNode.parent;
                    } else {
                        queryNode = newNode;
                    }
                }
            }

            if (!plan.subQueries.isEmpty()) {
                queryNode = generate(plan.subQueries, 0, tableAlias, queryNode, dbName);
                if (queryNode.rightChild != null && !queryNode.rightChild.nodeType.equals(NodeType.LEAF_NODE)) {
                    queryNode.condition = attachedJoinConditionProcess(plan.attachedCondition);
                }
            }
        }
        return queryNode;
    }

    private static String attachedJoinConditionProcess(String attachedCondition) {
        if (attachedCondition.equals("")) return attachedCondition;
//        attachedCondition = attachedCondition.replaceAll(" <.+?> ", "").replaceAll("/\\*.*\\*/ ", "");
        Pattern pattern = Pattern.compile("(.*) <.+?> \\((.*)\\)(.*)");
        Matcher matcher = pattern.matcher(attachedCondition);
        if (matcher.find()) {
            attachedCondition = matcher.group(1) + matcher.group(2) + matcher.group(3);
        }
        pattern = Pattern.compile("(not\\().*,(.*),.*(\\))");
        matcher = pattern.matcher(attachedCondition);
        if (matcher.find()) {
            attachedCondition = matcher.group(1) + matcher.group(2) + matcher.group(3);
        }
        attachedCondition = attachedCondition.replaceAll("/\\*.*\\*/ ", "");
        return attachedCondition;
    }

    private static String attachedConditionProcess(String attachedCondition) {
        int select2Position = attachedCondition.indexOf("select#2");
        if (select2Position != -1) {
            int position = attachedCondition.lastIndexOf(" and ", select2Position);
            if (position == -1) position = attachedCondition.lastIndexOf(" or ", select2Position);
            if (position == -1) return "";
            int pCount = 0;
            StringBuilder result = new StringBuilder(attachedCondition.substring(0, position));
            for (int i = position; i < attachedCondition.length(); i++) {
                if (attachedCondition.charAt(i) == '(') {
                    pCount++;
                } else if (attachedCondition.charAt(i) == ')') {
                    if (pCount == 1) {
                        result.append(attachedCondition.substring(i + 1));
                    } else {
                        pCount--;
                    }
                }
            }
            attachedCondition = result.toString();
        }
        int ifPosition = attachedCondition.indexOf("<if>");
        if (ifPosition != -1) {
            StringBuilder result = new StringBuilder(attachedCondition.substring(0, ifPosition));
            int leftCommasPos = attachedCondition.indexOf(",", ifPosition) + 2;
            int endPos = -1;
            int pCount = 0;
            for (int i = ifPosition; i < attachedCondition.length(); i++) {
                if (attachedCondition.charAt(i) == '(') {
                    pCount++;
                } else if (attachedCondition.charAt(i) == ')') {
                    if (pCount == 1) {
                        endPos = i;
                        break;
                    } else {
                        pCount--;
                    }
                }
            }
            int rightCommasPos = attachedCondition.lastIndexOf(",", endPos);
            result.append(attachedCondition, leftCommasPos, rightCommasPos).append(attachedCondition.substring(endPos + 1));
            attachedCondition = result.toString();
        }
        return attachedCondition;
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
//        String planJson = Common.getSql("executePlan/" + sqlIndex + ".json").replace("\r\n", "");
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

    private static void getPlan(Object object, List<QueryPlan> plans) {
        if (Map.class.isAssignableFrom(object.getClass())) {
            Map map = (Map) object;
            if (map.containsKey("table")) {
                plans.add(new QueryPlan(Common.cast(map.get("table"))));
            } else {
                for (Object key : map.keySet()) {
                    if (!key.equals("optimized_away_subqueries"))
                        getPlan(map.get(key), plans);
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
        List<String> usedKey;
        String attachedCondition;
        List<String> usedColumns;
        List<QueryPlan> subQueries = new ArrayList<>();

        QueryPlan(Map<String, Object> object) {
            ref = Common.cast(object.getOrDefault("ref", new ArrayList<>()));
            usedKey = Common.cast(object.getOrDefault("used_key_parts", new ArrayList<>()));
            key = object.getOrDefault("key", "").toString();
            tableName = object.containsKey("materialized_from_subquery") ? "derived" : object.getOrDefault("table_name", "").toString().replace("`", "");
            attachedCondition = object.getOrDefault("attached_condition", "").toString().replace("`", "").replace("<cache>", "");
            usedColumns = Common.cast(object.getOrDefault("used_columns", new ArrayList<>()));
            getPlan(object, subQueries);
        }
    }
}
