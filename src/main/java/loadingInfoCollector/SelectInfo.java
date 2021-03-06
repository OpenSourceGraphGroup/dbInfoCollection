package loadingInfoCollector;

/**
 *  @Author: Zhengmin Lai
 *  @Description: Parse Constraint List of Select Node
 */
class SelectInfo {
    private String tableName;
    private String parsedWhereOps;

    String getTableName() {
        return tableName;
    }

    String getParsedWhereOps() {
        return parsedWhereOps;
    }

    void parseSelectNodeWhereOps(String condition) throws Exception {
        if (condition == null || condition.length() < 1) {
            return;
        }
        if (condition.charAt(0) == '(') {
            condition = condition.substring(1, condition.length() - 1);
        }
        StringBuilder sb = new StringBuilder();
        String[] andOps = condition.split(" and ");
        if (andOps.length == 1) {
            String[] orOps = condition.split(" or ");

            for (String orOp : orOps) {
                String rst = parseOp(orOp);
                if(!rst.isEmpty()){
                    sb.append(rst).append("#");
                }
            }
            // only one op
            if (orOps.length == 1) {
                sb.deleteCharAt(sb.length() - 1);
            } else {
                sb.append("or");
            }
        } else {
            for (String andOp : andOps) {
                String rst = parseOp(andOp);
                if(!rst.isEmpty()){
                    sb.append(rst).append("#");
                }
            }
            sb.append("and");
        }
        this.parsedWhereOps = sb.toString();
    }

    private String parseOp(String whereOp) throws Exception {
        if (whereOp.charAt(0) == '(') {
            whereOp = whereOp.substring(1, whereOp.length() - 1);
        }
        String op;
        if (whereOp.contains(" >= ")) {
            whereOp = whereOp.split(" >= ")[0];
            op = "@>=";
        } else if (whereOp.contains(" <= ")) {
            whereOp = whereOp.split(" <= ")[0];
            op = "@<=";
        } else if (whereOp.contains(" <> ")) {
            whereOp = whereOp.split(" <> ")[0];
            op = "@<>";
        } else if (whereOp.contains(" = ")) {
            whereOp = whereOp.split(" = ")[0];
            op = "@=";
        } else if (whereOp.contains(" > ")) {
            whereOp = whereOp.split(" > ")[0];
            op = "@>";
        } else if (whereOp.contains(" < ")) {
            whereOp = whereOp.split(" < ")[0];
            op = "@<";
        } else if (whereOp.contains(" between ")) {
            whereOp = whereOp.split(" between ")[0];
            op = "@bet";
        } else if (whereOp.contains(" like ") && whereOp.contains("not")) {
            whereOp = whereOp.split(" like ")[0];
            whereOp = whereOp.substring(whereOp.lastIndexOf("(") + 1);
            op = "@notlike";
        } else if (whereOp.contains(" like ")) {
            whereOp = whereOp.split(" like ")[0];
            op = "@like";
        } else if (whereOp.contains(" in ") && whereOp.contains("not")) {
            whereOp = whereOp.split(" notin ")[0];
            op = "@notin";
        }else if (whereOp.contains(" in ")) {
            whereOp = whereOp.split(" in ")[0];
            op = "@in";
        } else{
            return "";
        }
        // the attribute
        String attribute = "";
        if (whereOp.contains(".")) {
            int firstIdx = whereOp.indexOf(".");
            int lastIdx = whereOp.lastIndexOf(".");
            if (lastIdx == -1) {
                throw new Exception("There must be sth wrong with the format where it should be in 'DATABASE.TABLE.ATTRIBUTE'");
            }
            this.tableName = whereOp.substring(firstIdx + 1, lastIdx);
            attribute = whereOp.substring(lastIdx + 1);
        }
        return attribute + op;
    }
}
