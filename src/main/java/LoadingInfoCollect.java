import net.sf.jsqlparser.JSQLParserException;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * @Author:
 * @Description: 负载信息采集工具
 * @Date: 2019/11/14
 */
public class LoadingInfoCollect {
    public static void main(String[] args) throws JSQLParserException, SQLException {
        Connection connection = Common.connect("59.78.194.63", "tpch", "root", "OpenSource");
        QueryNode root = QueryTreeGenerator.generate(connection, Common.getSql("sql/15.sql"), "tpch");
        if(root!=null) {
            root.postOrder(queryNode1 -> System.out.println(queryNode1.nodeType + " " + queryNode1.condition));
            ComputingTree.computingSqlUpadteCount(connection, root);
            ComputingTree.printInfo(root);
        }
    }
}
