import java.sql.Connection;

/**
 * @Author:
 * @Description:
 * @Date: 2019/12/18
 */
public class Main {
    public static void main(String[] args) throws Exception {
        Connection connection = Common.connect("59.78.194.63", "tpch", "root", "OpenSource");
        String sql = Common.getSql("sql/" + 1 + ".sql");
        String dbName = "tpch";
        LoadingInfoCollect.loadingInfoCollect(connection, sql, dbName);
    }
}
