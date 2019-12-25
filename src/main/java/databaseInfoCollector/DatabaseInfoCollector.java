package databaseInfoCollector;

import common.Common;

import java.sql.Connection;
import java.util.List;

/**
 * @Author: Jiaye Liu
 * @Description: database and schema information collection
 **/

public class DatabaseInfoCollector {
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

    public static void DatabaseInfoCollect(Connection connection, String dbName) {
        SchemaCollector sc = new SchemaCollector(connection);
        DataInfoCollector dic = new DataInfoCollector(connection);

        List<Object> tableNameList = sc.getTableList(dbName);
        String outPath = dbName + "databaseInfo";
        for (Object table : tableNameList) {
            Common.writeTo(sc.getTableInfo(dbName, (String) table) + "\n", outPath, Common.WriteType.Append);
            long tableSize = Long.parseLong(sc.getTableSize(dbName, (String) table));
            Common.writeTo(dic.getDataStatistics(tableSize, sc.getTableColumns(dbName, (String) table)) + "\n", outPath, Common.WriteType.Append);
        }
    }
}
