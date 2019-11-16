import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SubSelect;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;

/**
 * @Author: XinJin
 * @Description: 查询树生成器
 * @Date: 2019/11/15
 */
public class QueryTreeGenerator {
    private static class QueryPlan extends ArrayList<HashMap<String, String>> {
        public String toString() {
            StringBuilder result = new StringBuilder();
            result.append(this.get(0).keySet()).append("\r\n");
            for (HashMap ele : this) {
                result.append(ele.values()).append("\r\n");
            }
            return result.toString();
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
    static QueryNode generate(Connection connection, String sql) throws JSQLParserException {
        QueryPlan queryPlan = queryPlanGenerate(connection, sql);
        System.out.println(queryPlan);
        Map<String, String> selectCondition = getSelectCondition(sql);
        Map<String, String> leafCondition = getLeafCondition(sql);

        Map<String, String> plan = queryPlan.get(0);

        QueryNode queryNode = new QueryNode(NodeType.LEAF_NODE);
        String tableName = leafCondition.getOrDefault(plan.get("table"), plan.get("table"));
        queryNode.condition = tableName;
        if (plan.get("Extra").contains("Using where")) {
            queryNode.parent = new QueryNode(NodeType.SELECT_NODE);
            queryNode.parent.leftChild = queryNode;
            queryNode.parent.condition = selectCondition.get(tableName);
            queryNode = queryNode.parent;
        }

        for (int index = 1; index < queryPlan.size(); index++) {
            plan = queryPlan.get(index);
            tableName = leafCondition.getOrDefault(plan.get("table"), plan.get("table"));
            String[] joinKey = plan.get("ref").split("\\.");
            String leftJoinKey = joinKey[joinKey.length - 1];
            queryNode.parent = new QueryNode(NodeType.JOIN_NODE);
            queryNode.parent.condition = leftJoinKey + " = "
                    + getJoinKey(connection, plan.get("key"), tableName);
            queryNode.parent.leftChild = queryNode;
            queryNode.parent.rightChild = new QueryNode(NodeType.LEAF_NODE);
            queryNode.parent.rightChild.condition = tableName;
            queryNode = queryNode.parent;
            if (plan.get("Extra") != null && plan.get("Extra").contains("Using where")) {
                queryNode.parent = new QueryNode(NodeType.SELECT_NODE);
                queryNode.parent.leftChild = queryNode;
                queryNode.parent.condition = selectCondition.get(tableName);
                queryNode = queryNode.parent;
            }
        }
        return queryNode;
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

    /**
     * Using 'explain' statement in mysql to generate query plan
     *
     * @param connection
     * @param sql
     * @return
     */
    private static QueryPlan queryPlanGenerate(Connection connection, String sql) {
        QueryPlan queryPlan = new QueryPlan();
        sql = String.format("explain %s", sql);
        ResultSet resultSet = Common.query(connection, sql);
        try {
            ResultSetMetaData metaData = resultSet.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (resultSet.next()) {
                HashMap<String, String> rowData = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    Object val = resultSet.getObject(i);
                    rowData.put(metaData.getColumnName(i), val == null ? "null" : val.toString());
                }
                queryPlan.add(rowData);
            }
            queryPlan.sort((o1, o2) -> Integer.valueOf(o2.get("id")).compareTo(Integer.valueOf(o1.get("id"))));
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return queryPlan;
    }

    /**
     * Get LEAF_NODE'S condition
     *
     * @param sql
     * @return
     * @throws JSQLParserException
     */
    static Map<String, String> getLeafCondition(String sql) throws JSQLParserException {
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
            if (selectBody.getFromItem().getClass().equals(Table.class)) break;
            else {
                selectBody = (PlainSelect) ((SubSelect) selectBody.getFromItem()).getSelectBody();
            }
        }
        return condition;
    }

    /**
     * Get SELECT_NODE's condition
     *
     * @param sql
     * @return
     */
    private static Map<String, String> getSelectCondition(String sql) throws JSQLParserException {
        Map<String, String> results = new HashMap<>();
        PlainSelect selectBody = (PlainSelect) ((Select) CCJSqlParserUtil.parse(sql)).getSelectBody();
        while (selectBody != null) {
            Expression where = selectBody.getWhere();
            parseExpression(where, results);
            if (selectBody.getFromItem().getClass().equals(Table.class)) break;
            else {
                selectBody = (PlainSelect) ((SubSelect) selectBody.getFromItem()).getSelectBody();
            }
        }
        return results;
    }

    /**
     * Parse expression into conditions
     *
     * @param expression
     * @param results
     */
    private static void parseExpression(Expression expression, Map<String, String> results) {
        if (expression == null) return;
        if (expression.getClass().equals(AndExpression.class)
                || expression.getClass().equals(OrExpression.class)) {
            parseExpression(((BinaryExpression) expression).getLeftExpression(), results);
            parseExpression(((BinaryExpression) expression).getRightExpression(), results);
        } else if (expression.getClass().equals(Parenthesis.class)) {

        } else {
            String columnName = null;
            if (BinaryExpression.class.isAssignableFrom(expression.getClass())) {
                if (!((BinaryExpression) expression).getRightExpression().getClass().equals(Column.class))
                    columnName = ((Column) ((BinaryExpression) expression).getLeftExpression()).getColumnName();
            } else if (expression.getClass().equals(Between.class)) {
                columnName = ((Column) ((Between) expression).getLeftExpression()).getColumnName();
            } else {
                columnName = ((Column) ((InExpression) expression).getLeftExpression()).getColumnName();
            }
            if (columnName != null) {
                String tableName = getTableNameByColumn(columnName);
                results.put(tableName, expression.toString());
            }
        }
    }

    /**
     * Will be deprecated after ZhongXin implements corresponding API
     *
     * @param columnName
     * @return
     */
    @Deprecated
    private static String getTableNameByColumn(String columnName) {
        try {
            Connection connection = Common.connect("59.78.194.63", "information_schema", "root", "OpenSource");
            ResultSet resultSet = Common.query(connection, String.format("SELECT TABLE_NAME FROM COLUMNS WHERE COLUMN_NAME = '%s'", columnName));
            if (resultSet.next()) {
                return resultSet.getString("TABLE_NAME");
            }
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return "";
    }

    @Test
    public void testQueryTreeGenerate() throws SQLException, JSQLParserException {
        Connection connection = Common.connect("59.78.194.63", "tpch", "root", "OpenSource");
        QueryNode queryNode = generate(connection, Common.getSql("sql/3.sql"));
        queryNode.postOrder(queryNode1 -> System.out.println(queryNode1.nodeType + " " + queryNode1.condition));
        connection.close();
    }
}
