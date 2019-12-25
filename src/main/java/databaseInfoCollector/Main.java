package databaseInfoCollector;

import common.Common;

import java.sql.Connection;

/**
 * @Author:
 * @Description:
 * @Date: 2019/12/25
 */
public class Main {
    public static void main(String[] args) {
        int arg = 0;
        if (args.length < 6) {
            System.out.println("please input correct arguments");
            return;
        }
        String ip = args[arg++];
        String port = args[arg++];
        String dbName = args[arg++];
        String user = args[arg++];
        String password = args[arg];

        Connection connection = Common.connect(ip, port, dbName, user, password);
        DatabaseInfoCollector.DatabaseInfoCollect(connection, dbName);
    }
}
