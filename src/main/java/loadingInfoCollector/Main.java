package loadingInfoCollector;

import common.Common;
import org.junit.Test;

import java.sql.Connection;

/**
 * @Author:
 * @Description:
 * @Date: 2019/12/25
 */
public class Main {
    @Test
    public void testAll() {
        Connection connection = Common.connect("59.78.194.63", "3306", "tpch", "root", "OpenSource");
        for (int i = 1; i <= 16; i++)
            LoadingInfoCollect.loadingInfoCollect(connection, "sql/" + i + ".sql", "tpch");
    }

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
}
