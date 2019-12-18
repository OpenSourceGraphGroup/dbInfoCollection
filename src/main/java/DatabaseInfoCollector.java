import java.sql.Connection;
import java.util.List;

/**
 * @program: dbInfoCollection
 * @description: database and schema information collection
 * @author: Jiaye Liu
 * @create: 2019-12-18 22:54
 **/

public class DatabaseInfoCollector {
    static void DatabaseInfoCollect(Connection connection,String dbName){
        SchemaCollector sc = new SchemaCollector(connection);
        DataInfoCollector dic = new DataInfoCollector(connection);

        List<Object> tableNameList=sc.getTableList();
        for(Object table:tableNameList){
            Common.info(sc.getTableInfo(dbName,(String)table)+"\n");
            long tableSize = Long.parseLong(sc.getTableSize(dbName, (String)table));
            Common.info(dic.getDataStatistics(dbName, (String)table, tableSize, sc.getTableColumns(dbName, (String)table))+"\n");
        }
    }
}
