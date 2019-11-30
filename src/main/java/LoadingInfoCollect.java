import java.sql.Connection;
import java.util.Scanner;

/**
 * @Author:
 * @Description: 负载信息采集工具
 * @Date: 2019/11/14
 */
public class LoadingInfoCollect {
    public static void main(String[] args) throws Exception {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("Please input the query name(type E to exit): ");
            String q = scanner.next();
            if(q.trim().equals("E")){
                return;
            }
            Connection connection = Common.connect("59.78.194.63", "tpch", "root", "OpenSource");
            QueryNode root = QueryTreeGenerator.generate(connection, Common.getSql("sql/" + q + ".sql"), "tpch");
            root.postOrder(queryNode1 -> System.out.println(queryNode1.nodeType + " " + queryNode1.condition));
            ComputingTree.computingSqlUpadteCount(connection, root);

            ConstraintList constraintList = new ConstraintList(connection);
            constraintList.getConstraintList(root);
            for (String output : constraintList.getTableConstraints()) {
                System.out.println(output);
            }
        }
    }
}
