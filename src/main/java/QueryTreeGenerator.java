import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
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

    enum Operator {
        EQU(" = "), NEQ(" <> "), LSS(" < "), LEQ(" <= "), GTR(" > "), GEQ(" >= "), BET(" between "), LIKE(" like "), NOTLIKE(" not like "), IN(" in "), NOTIN(" not in ");
        String value;

        Operator(String value) {
            this.value = value;
        }

        public String toString() {
            return value;
        }
    }

    private static Map<Class, Operator> operatorMap = new HashMap<Class, Operator>() {
        {
            put(EqualsTo.class, Operator.EQU);
            put(NotEqualsTo.class, Operator.NEQ);
            put(GreaterThan.class, Operator.GTR);
            put(GreaterThanEquals.class, Operator.GEQ);
            put(MinorThan.class, Operator.LSS);
            put(MinorThanEquals.class, Operator.LEQ);
        }
    };

    private static class Where {
        Operator operator;
        String left; // left is always a column, right may be a column or value
        String right;
        String table;
        boolean and;
        String id;

        Where(Operator operator, String left, String right, String table, String id, boolean and) {
            this.operator = operator;
            this.left = left;
            this.right = right;
            this.table = table;
            this.id = id;
            this.and = and;
        }

        static String getCondition(List<Where> whereStatements, String id, String table) {
            StringBuilder result = new StringBuilder();
            for (Where statement : whereStatements) {
                if (statement.id.equals(id) && statement.table.equals(table)) {
                    if (result.length() != 0) {
                        result.append(statement.and ? " and " : " or ");
                    }
                    result.append(statement.left).append(statement.operator).append(statement.right);
                }
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
    public static QueryNode generate(Connection connection, String sql) throws JSQLParserException {
        QueryPlan queryPlan = queryPlanGenerate(connection, sql);
        System.out.println(queryPlan);
        Map<String, String> tableAlias = getTableAlias(sql);
        List<Where> wheres = getWheres(sql, Integer.valueOf(queryPlan.get(0).get("id")));
        return generate(connection, queryPlan, tableAlias, wheres);
    }

    private static QueryNode generate(Connection connection, QueryPlan queryPlan, Map<String, String> tableAlias, List<Where> wheres) {
        QueryNode queryNode = null;
        for (int index = 0; index < queryPlan.size(); index++) {
            Map<String, String> plan = queryPlan.get(index);
            String tableName = tableAlias.getOrDefault(plan.get("table"), plan.get("table"));
            QueryNode leafNode = new QueryNode(NodeType.LEAF_NODE, null, null, tableName);
            if (index != 0) {
                // Generate JOIN_NODE
                String[] joinKey = plan.get("ref").split("\\.");
                String leftJoinKey = joinKey[joinKey.length - 1];
                queryNode.parent = new QueryNode(NodeType.JOIN_NODE, queryNode, leafNode, leftJoinKey + " = " + getJoinKey(connection, plan.get("key"), tableName));
                queryNode = queryNode.parent;
            } else {
                //Generate LEAF_NODE
                queryNode = leafNode;
            }
            // Generate SELECT_NODE
            if (plan.get("Extra").contains("Using where")) {
                queryNode.parent = new QueryNode(NodeType.SELECT_NODE, queryNode, null, Where.getCondition(wheres, plan.get("id"), tableName));
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
     * Get SELECT_NODE's condition
     *
     * @param sql
     * @return
     */
    private static List<Where> getWheres(String sql, Integer SubQueryDepth) throws JSQLParserException {
        List<Where> results = new ArrayList<>();
        PlainSelect selectBody = (PlainSelect) ((Select) CCJSqlParserUtil.parse(sql)).getSelectBody();
        int id = 0;
        while (selectBody != null) {
            Expression where = selectBody.getWhere();
            parseExpression(where, results, true, SubQueryDepth - id);
            if (selectBody.getFromItem().getClass().equals(Table.class)) break;
            else {
                selectBody = (PlainSelect) ((SubSelect) selectBody.getFromItem()).getSelectBody();
            }
            id++;
        }
        return results;
    }

    /**
     * Parse expression into conditions
     *
     * @param expression
     * @param results
     */
    private static void parseExpression(Expression expression, List<Where> results, boolean and, int id) {
        if (expression == null) return;
        Class expressionClass = expression.getClass();
        if (expressionClass.equals(AndExpression.class)) {
            parseExpression(((BinaryExpression) expression).getLeftExpression(), results, true, id);
            parseExpression(((BinaryExpression) expression).getRightExpression(), results, true, id);
        } else if (expressionClass.equals(OrExpression.class)) {
            parseExpression(((BinaryExpression) expression).getLeftExpression(), results, true, id);
            parseExpression(((BinaryExpression) expression).getRightExpression(), results, false, id);
        } else if (expressionClass.equals(Parenthesis.class)) {

        } else {
            String left = null;
            String right = null;
            Operator operator = null;
            if (ComparisonOperator.class.isAssignableFrom(expressionClass)) {
                Expression leftExpression = ((BinaryExpression) expression).getLeftExpression();
                Expression rightExpression = ((BinaryExpression) expression).getRightExpression();
                if (!leftExpression.getClass().equals(Column.class)) {
                    Expression tmp = leftExpression;
                    leftExpression = rightExpression;
                    rightExpression = tmp;
                }
                if (!rightExpression.getClass().equals(Column.class)) {
                    left = leftExpression.toString();
                    right = rightExpression.toString();
                    operator = operatorMap.get(expressionClass);
                }
            } else if (expressionClass.equals(Between.class)) {
                left = ((Between) expression).getLeftExpression().toString();
                right = ((Between) expression).getBetweenExpressionStart() + " and " +
                        ((Between) expression).getBetweenExpressionEnd();
                operator = Operator.BET;
            } else if (expressionClass.equals(InExpression.class)) {
                left = ((InExpression) expression).getLeftExpression().toString();
                right = ((InExpression) expression).getRightItemsList().toString();
                operator = ((InExpression) expression).isNot() ? Operator.NOTIN : Operator.IN;

            } else if (expressionClass.equals(LikeExpression.class)) {
                left = ((BinaryExpression) expression).getLeftExpression().toString();
                right = ((BinaryExpression) expression).getRightExpression().toString();
                operator = ((LikeExpression) expression).isNot() ? Operator.NOTLIKE : Operator.LIKE;
            }
            if (left != null) {
                String tableName = getTableNameByColumn(left);
                results.add(new Where(operator, left, right, tableName, String.valueOf(id), and));
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
        QueryNode queryNode = generate(connection, Common.getSql("sql/17.sql"));
        queryNode.postOrder(queryNode1 -> System.out.println(queryNode1.nodeType + " " + queryNode1.condition));
        connection.close();
    }
}
