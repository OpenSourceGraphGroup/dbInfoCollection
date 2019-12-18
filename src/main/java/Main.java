import java.sql.Connection;

/**
 * @Author:
 * @Description:
 * @Date: 2019/12/18
 */
public class Main {
    public static void main(String[] args) throws Exception {
        String ip = args[0];
        String dbName = args[1];
        String user = args[2];
        String password = args[3];
        String sqlPath = args[4];

        Connection connection = Common.connect(ip, dbName, user, password);
        /* Loading Information Collect */
        String sql = Common.getSql(sqlPath);
        LoadingInfoCollect.loadingInfoCollect(connection, sql, dbName);
    }
}
