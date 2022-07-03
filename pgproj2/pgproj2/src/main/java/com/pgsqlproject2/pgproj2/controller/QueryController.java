package com.pgsqlproject2.pgproj2.controller;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.multipart.MultipartFile;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Controller
@RequestMapping("thymeleaf")
public class QueryController {
    @Autowired
    JdbcTemplate jdbcTemplate;

    @RequestMapping(value = "query", method = RequestMethod.GET)
    public String query() {
        return "query";
    }

    @RequestMapping(value = "queryBlog", method = RequestMethod.POST)
    @ResponseBody
    public String queryBlog(String query1) {
        if (query1.length() <= 2) {
            int id = Integer.parseInt(query1);
            if (id < 6 || id > 11) return """
                    <body style="font-family: fira code; text-align: center; padding:20px; background-color: cyan">
                    Enter valid id
                    </Body>""";

            StringBuilder ret = new StringBuilder();
            ret.append("<body style=\"font-family: fira code; padding:20px; background-color: cyan\">\n");

            if (id == 6) {
                String sql = "select count(*) as cnt from staff where type = 'Director'";
                List<Map<String, Object>> datas = jdbcTemplate.queryForList(sql);

                for (Map<String, Object> dat : datas) {
                    ret.append("Director").append("&nbsp;&nbsp;&nbsp;").append(getMapToString(dat)).append("<br>");
                }

                datas.clear();
                sql = "select count(*) as cnt from staff where type = 'Contracts Manager'";
                datas = jdbcTemplate.queryForList(sql);

                for (Map<String, Object> dat : datas) {
                    ret.append("Contracts Manager ").append("&nbsp;&nbsp;&nbsp;").append(getMapToString(dat)).append("<br>");
                }

                datas.clear();
                sql = "select count(*) as cnt from staff where type = 'Salesman'";
                datas = jdbcTemplate.queryForList(sql);

                for (Map<String, Object> dat : datas) {
                    ret.append("Salesman").append("&nbsp;&nbsp;&nbsp;").append(getMapToString(dat)).append("<br>");
                }

                datas.clear();
                sql = "select count(*) as cnt from staff where type = 'Supply Staff'";
                datas = jdbcTemplate.queryForList(sql);

                for (Map<String, Object> dat : datas) {
                    ret.append("Supply Staff ").append("&nbsp;&nbsp;&nbsp;").append(getMapToString(dat)).append("<br>");
                }

            } else if (id == 7) {
                String sql = "select count(*) from (select distinct contract_num from \"placeOrder\") temp;";
                List<Map<String, Object>> datas = jdbcTemplate.queryForList(sql);

                for (Map<String, Object> dat : datas) {
                    ret.append("ContractCount").append("&nbsp;&nbsp;&nbsp;").append(getMapToString(dat)).append("<br>");
                }
            } else if (id == 8) {
                String sql = "select count(*) from \"placeOrder\"";
                List<Map<String, Object>> datas = jdbcTemplate.queryForList(sql);

                for (Map<String, Object> dat : datas) {
                    ret.append("OrderCount").append("&nbsp;&nbsp;&nbsp;").append(getMapToString(dat)).append("<br>");
                }
            } else if (id == 9) {
                String sql = """
                        select (max(cnt) - min(cnt)) res
                        from (select *
                              from (select count(*) cnt
                                    from (select distinct stk.product_model, count(*) from "stockIn" stk group by stk.product_model) m) k
                              union all
                              (select count(*) cnt
                               from (select distinct stk.product_model, count(*) n
                                     from "stockIn" stk
                                              join "placeOrder" o on stk.product_model = o.product_model
                                     group by stk.product_model) n)) pl;""";
                List<Map<String, Object>> datas = jdbcTemplate.queryForList(sql);

                for (Map<String, Object> dat : datas) {
                    ret.append("NeverSoldProductCount").append("&nbsp;&nbsp;&nbsp;").append(getMapToString(dat)).append("<br>");
                }
            } else if (id == 10) {
                String sql = """
                        select product_model, sum(quantity)
                        from "placeOrder"
                        group by product_model
                        having sum(quantity) =
                               (
                                   select max(temp.sum) as max_val
                                   from (select sum(quantity) as sum
                                         from "placeOrder"
                                         group by product_model) temp
                               );""";
                List<Map<String, Object>> datas = jdbcTemplate.queryForList(sql);

                for (Map<String, Object> dat : datas) {
                    ret.append("FavoriteProductModel").append("&nbsp;&nbsp;&nbsp;").append(getMapToString(dat)).append("<br>");
                }
            } else if (id == 11) {
                String sql = """
                        select supply_center, avg(quantity) as avg
                        from "stockIn"
                        group by supply_center
                        order by supply_center;""";
                List<Map<String, Object>> datas = jdbcTemplate.queryForList(sql);

                for (Map<String, Object> dat : datas) {
                    ret.append("AvgStockByCenter").append("&nbsp;&nbsp;&nbsp;").append(getMapToString(dat)).append("<br>");
                }
            }

            ret.append("\n</body>");

            return ret.toString();
        } else if (query1.length() <= 10) {
            StringBuilder ret = new StringBuilder();
            if (query1.length() == 7) {
                ret.append("<body style=\"font-family: fira code; padding:20px; background-color: cyan\">\n");
                String sql = "select supply_center, temp.number as product_number, product_model, purchase_price, quantity\n" +
                        "from \"stockIn\"\n" +
                        "     join\n" +
                        "(select number, model\n" +
                        "from model as mo where number = '" + query1 + "') temp on temp.model = \"stockIn\".product_model;\n";

                List<Map<String, Object>> datas = jdbcTemplate.queryForList(sql);

                for (Map<String, Object> dat : datas) {
                    ret.append("ProductByNumber:").append("&nbsp;&nbsp;&nbsp;").append(getMapToString(dat)).append("<br>");
                }
                ret.append("\n</body>");
            } else if (query1.length() == 10) {
                ret.append("<body style=\"font-family: fira code; padding:20px; background-color: cyan\">\n");
                String sql = "select distinct contract_num, sub1.name, enterprise, enterprise.supply_center from \"placeOrder\"\n" +
                        "join (\n" +
                        "    select * from staff where type = 'Contracts Manager'\n" +
                        "    )sub1\n" +
                        "on contract_manager = number\n" +
                        "join enterprise\n" +
                        "on \"placeOrder\".enterprise = enterprise.name\n" +
                        "where contract_num = '" + query1 + "';";

                List<Map<String, Object>> datas = jdbcTemplate.queryForList(sql);

                for (Map<String, Object> dat : datas) {
                    ret.append("ProductByNumber:").append("&nbsp;&nbsp;&nbsp;").append(getMapToString(dat)).append("<br>");
                }

                datas.clear();
                sql = "select distinct product_model, salesman_num, quantity, unit_price, estimated_delivery_date, lodgement_date from \"placeOrder\"\n" +
                        "join model\n" +
                        "on product_model = model\n" +
                        "where contract_num = '" + query1 + "';";

                datas = jdbcTemplate.queryForList(sql);

                for (Map<String, Object> dat : datas) {
                    ret.append("ProductByNumber:").append("&nbsp;&nbsp;&nbsp;").append(getMapToString(dat)).append("<br>");
                }

                ret.append("\n</body>");
            } else
                return "<body style=\"font-family: fira code; padding:20px; background-color: cyan\">\ninvalid input\n</body>";
            return ret.toString();
        } else {
            try {
                List<Map<String, Object>> datas = jdbcTemplate.queryForList(query1);

                StringBuilder ret = new StringBuilder();
                ret.append("<body style=\"font-family: fira code; padding:20px; background-color: cyan\">\n");
                for (Map<String, Object> dat : datas) {
                    ret.append(getMapToString(dat)).append("<br>");
                }

                ret.append("\n</body>");
                return ret.toString();
            } catch (DataAccessException dataAccessException) {
                return "<body style=\"font-family: fira code; padding:20px; background-color: cyan\">\ninvalid input\n</body>";
            }
        }
    }

    /**
     * Map转String
     *
     * @param map
     * @return
     */
    public static String getMapToString(Map<String, Object> map) {
        Set<String> keySet = map.keySet();
        //将set集合转换为数组
        String[] keyArray = keySet.toArray(new String[keySet.size()]);
        //给数组排序(升序)
        Arrays.sort(keyArray);
        //因为String拼接效率会很低的，所以转用StringBuilder
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < keyArray.length; i++) {
            // 参数值为空，则不参与签名 这个方法trim()是去空格
            if ((String.valueOf(map.get(keyArray[i]))).trim().length() > 0) {
//                sb.append(keyArray[i]).append(":").append(String.valueOf(map.get(keyArray[i])).trim());
                sb.append(String.valueOf(map.get(keyArray[i])).trim());
            }
            if (i != keyArray.length - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }
}
