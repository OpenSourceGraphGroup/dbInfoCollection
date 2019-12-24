import java.sql.Connection;

/**
 * @Author:
 * @Description:
 * @Date: 2019/12/18
 */
public class Main {
    public static void main(String[] args) {
        int arg = 0;
        String ip = args[arg++];
        String port = args[arg++];
        String dbName = args[arg++];
        String user = args[arg++];
        String password = args[arg++];
        String sqlPath = args[arg++];

        Connection connection = Common.connect(ip, port, dbName, user, password);
//System.out.println("connected");
        DatabaseInfoCollector.DatabaseInfoCollect(connection, dbName);
        /* Loading Information Collect */
        LoadingInfoCollect.loadingInfoCollect(connection, sqlPath, dbName);
    }
}
