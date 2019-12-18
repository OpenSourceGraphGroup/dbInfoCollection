import net.sf.jsqlparser.JSQLParserException;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Scanner;

/**
 * @Author:
 * @Description: 负载信息采集工具
 * @Date: 2019/11/14
 */
public class LoadingInfoCollect {
    @Test
    public void test() throws Exception {
        Connection connection = Common.connect("59.78.194.63", "tpch", "root", "OpenSource");

        for (int i = 1; i <= 16; i++) {
            String sql = Common.getSql("sql/" + 1 + ".sql");
            String dbName = "tpch";
            loadingInfoCollect(connection, sql, dbName);
        }
    }

    static void loadingInfoCollect(Connection connection, String sql, String dbName) throws Exception {
        QueryNode root = QueryTreeGenerator.generate(connection, sql, dbName);
        root.postOrder(queryNode1 -> System.out.println(queryNode1.nodeType + " " + queryNode1.condition));
        ComputingTree.computingSqlUpdateCount(connection, root);

        System.out.println("\nConstraint List:");
        ConstraintList constraintList = new ConstraintList(connection);
        constraintList.generateConstraintList(root);
        for (String output : constraintList.getTableConstraints()) {
            System.out.println(output);
        }

        System.out.println("\nRefined Constraint List:");
        constraintList.refineConstraintList();
        for (String output : constraintList.getTableConstraints()) {
            System.out.println(output);
        }
    }
}
