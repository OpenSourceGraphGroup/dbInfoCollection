package databaseInfoCollector;

import common.Common;
import loadingInfoCollector.LoadingInfoCollect;

import java.sql.Connection;
import java.util.List;

/**
 * @program: dbInfoCollection
 * @description: database and schema information collection
 * @author: Jiaye Liu
 * @create: 2019-12-18 22:54
 **/

public class DatabaseInfoCollector {
    public static void DatabaseInfoCollect(Connection connection, String dbName) {
        SchemaCollector sc = new SchemaCollector(connection);
        DataInfoCollector dic = new DataInfoCollector(connection);

        List<Object> tableNameList = sc.getTableList(dbName);
        String outPath = "out/" + System.currentTimeMillis() + ".dbInfo";
        for (Object table : tableNameList) {
            Common.writeTo(sc.getTableInfo(dbName, (String) table) + "\n", outPath, Common.WriteType.Append);
            long tableSize = Long.parseLong(sc.getTableSize(dbName, (String) table));
            Common.writeTo(dic.getDataStatistics(dbName, (String) table, tableSize, sc.getTableColumns(dbName, (String) table)) + "\n", outPath, Common.WriteType.Append);
        }
    }
}
