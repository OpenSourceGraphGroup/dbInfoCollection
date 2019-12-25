import common.Common;
import databaseInfoCollector.DatabaseInfoCollector;
import loadingInfoCollector.LoadingInfoCollect;

import java.sql.Connection;

/**
 * @Author:
 * @Description:
 * @Date: 2019/12/18
 */
public class Main {
    public static void main(String[] args) {
        int arg = 0;
        if (args.length < 6) {
            System.out.println("please input correct arguments");
            return;
        }
        String task = args[arg++];
        String ip = args[arg++];
        String port = args[arg++];
        String dbName = args[arg++];
        String user = args[arg++];
        String password = args[arg++];
        String sqlPath = "";
        if (args.length > arg) sqlPath = args[arg];
        else if (!task.equals("d")) {
            System.out.println("please input sql path");
            return;
        }

        Connection connection = Common.connect(ip, port, dbName, user, password);
//System.out.println("connected");
        if (task.equals("d"))
            DatabaseInfoCollector.DatabaseInfoCollect(connection, dbName);
            /* Loading Information Collect */
        else LoadingInfoCollect.loadingInfoCollect(connection, sqlPath, dbName);
    }
}
