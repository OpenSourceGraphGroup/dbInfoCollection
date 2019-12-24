import org.junit.Test;

import java.sql.Connection;


/**
 * @Author:
 * @Description: 负载信息采集工具
 * @Date: 2019/11/14
 */
public class LoadingInfoCollect {
    @Test
    public void test() {
        String dbName = "tpch";
        Connection connection = Common.connect("127.0.0.1", dbName, "OpenSource", "OpenSource");
//        String sqlPath = "sql/" + 16 + ".sql";
//        loadingInfoCollect(connection, sqlPath, dbName);

        for (int i = 1; i <= 16; i++) {
            String sqlPath = "sql/" + i + ".sql";
//            String sql = Common.getSql(sqlPath);
//            Common.query(connection,sql);
            loadingInfoCollect(connection, sqlPath, dbName);
        }
    }

    static void loadingInfoCollect(Connection connection, String sqlPath, String dbName) {
        /* Get Sql Name */
        String[] sqlNames = sqlPath.split("/");
        String sqlName = sqlNames[sqlNames.length - 1];

        String sql = Common.getSql(sqlPath);
        try {
            QueryNode root = QueryTreeGenerator.generate(connection, sql, dbName);
            ComputingTree.computingSqlUpdateCount(connection, root);
            ConstraintList.computeConstraintList(connection, root, sqlName);
        } catch (Exception ex) {
            Common.error(ex.toString());
        }
    }
}
