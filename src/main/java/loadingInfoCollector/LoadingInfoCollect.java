package loadingInfoCollector;

import common.Common;
import java.sql.Connection;


/**
 * @Author: Xin Jin
 * @Description: collect loading info
 */
public class LoadingInfoCollect {
    public static void main(String args[]) {
        int arg = 0;
        if (args.length < 6) {
            System.out.println("please input correct arguments");
            return;
        }
        String ip = args[arg++];
        String port = args[arg++];
        String dbName = args[arg++];
        String user = args[arg++];
        String password = args[arg++];
        String sqlPath = args[arg];

        Connection connection = Common.connect(ip, port, dbName, user, password);
        LoadingInfoCollect.loadingInfoCollect(connection, sqlPath, dbName);
    }

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
