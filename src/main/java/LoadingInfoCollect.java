import org.junit.Test;

import java.sql.Connection;


/**
 * @Author:
 * @Description: 负载信息采集工具
 * @Date: 2019/11/14
 */
public class LoadingInfoCollect {

    @Test
    public void test() throws Exception {
        Connection connection = Common.connect("59.78.194.63", "tpch", "root", "OpenSource");

        String sql = Common.getSql("sql/" + 4 + ".sql");
        String dbName = "tpch";
        loadingInfoCollect(connection, sql, dbName);
//        for (int i = 1; i <= 16; i++) {
//            String sql = Common.getSql("sql/" + i + ".sql");
//            String dbName = "tpch";
//            loadingInfoCollect(connection, sql, dbName);
//        }
    }

    static void loadingInfoCollect(Connection connection, String sql, String dbName) throws Exception {
        QueryNode root = QueryTreeGenerator.generate(connection, sql, dbName);
        root.postOrder(queryNode1 -> Common.writeTo(queryNode1.nodeType + " " + queryNode1.condition, sql + ".log"));
        ComputingTree.computingSqlUpdateCount(connection, root);

        StringBuilder sb = new StringBuilder();
        sb.append("\nConstraint List:\n");

        ConstraintList constraintList = new ConstraintList(connection);
        constraintList.generateConstraintList(root);
        for (String output : constraintList.getTableConstraints()) {
            sb.append(output).append("\n");
        }

        sb.append("\nRefined Constraint List:\n");
        constraintList.refineConstraintList();
        for (String output : constraintList.getTableConstraints()) {
            sb.append(output).append("\n");
        }

        Common.writeTo(sb.toString(), sql+".log");
    }
}
