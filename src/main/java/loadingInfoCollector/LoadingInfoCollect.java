package loadingInfoCollector;

import common.Common;

import java.sql.Connection;


/**
 * @Author:
 * @Description: 负载信息采集工具
 * @Date: 2019/11/14
 */
public class LoadingInfoCollect {
    public static void loadingInfoCollect(Connection connection, String sqlPath, String dbName) {
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
