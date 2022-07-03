import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Properties;

public class PlaceOrder {
    private static final int BATCH_SIZE = 500;
    private static URL propertyURL = GoodLoader.class
            .getResource("/loader.cnf");

    private static Connection con = null;
    private static PreparedStatement stmt = null;
    private static boolean verbose = false;

    private static final String host = "localhost";
    private static final String dbname = "project2";
    private static final String user = "checker";
    private static final String pwd = "123456";

    public static HashMap<String, String> EnterpriseOfContract = new HashMap<String, String>();

    private static void openDB(String host, String dbname,
                               String user, String pwd) {
        try {
            //
            Class.forName("org.postgresql.Driver");
        } catch (Exception e) {
            System.err.println("Cannot find the Postgres driver. Check CLASSPATH.");
            System.exit(1);
        }
        String url = "jdbc:postgresql://" + host + "/" + dbname;
        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", pwd);
        try {
            con = DriverManager.getConnection(url, props);
            if (verbose) {
                System.out.println("Successfully connected to the database "
                        + dbname + " as " + user);
            }
            con.setAutoCommit(false);
        } catch (SQLException e) {
            System.err.println("Database connection failed");
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    private static void closeDB() {
        if (con != null) {
            try {
                if (stmt != null) {
                    stmt.close();
                }
                con.close();
                con = null;
            } catch (Exception e) {
                // Forget about it
            }
        }
    }

    private void getConnection() {
        try {
            Class.forName("org.postgresql.Driver");

        } catch (Exception e) {
            System.err.println("Cannot find the PostgreSQL driver. Check CLASSPATH.");
            System.exit(1);
        }

        try {
            String url = "jdbc:postgresql://" + host + "/" + dbname;
            con = DriverManager.getConnection(url, user, pwd);
            con.setAutoCommit(false);
        } catch (SQLException e) {
            System.err.println("Database connection failed");
            System.err.println(e.getMessage());
            System.exit(1);
        }
    }

    private void closeConnection() {
        if (con != null) {
            try {
                con.close();
                con = null;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private void init() {
        try {
            getConnection();
            PreparedStatement preparedStatement = con.prepareStatement("truncate table \"placeOrder\"");
            preparedStatement.execute();
            con.commit();
            preparedStatement.close();
        } catch (SQLException se) {
            System.out.println("Failed to init data");
        } finally {
            closeConnection();
        }
    }

    private static void Import() {
        try {
            stmt = con.prepareStatement("insert into \"placeOrder\" (contract_num, enterprise, product_model, quantity, contract_manager, contract_date, estimated_delivery_date, lodgement_date, salesman_num, contract_type)" + "values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        } catch (SQLException e) {
            System.err.println("Insert statement failed");
            System.err.println(e.getMessage());
            closeDB();
            System.exit(1);
        }
    }

    private static void loadData(String a, String b, String c, int d, String e, String f, String g, String h, String i, String j) throws SQLException {
        if (con != null) {
            stmt.setString(1, a);
            stmt.setString(2, b);
            stmt.setString(3, c);
            stmt.setInt(4, d);
            stmt.setString(5, e);
            stmt.setString(6, f);
            stmt.setString(7, g);
            stmt.setString(8, h);
            stmt.setString(9, i);
            stmt.setString(10, j);
            stmt.addBatch();
        }
    }

    public void insert(String a, String b, String c, int d, String e, String f, String g, String h, String i, String j) {
        try {
            getConnection();
            String sql = "insert into \"placeOrder\" (contract_num, enterprise, product_model, quantity, contract_manager, contract_date, estimated_delivery_date, lodgement_date, salesman_num, contract_type)  values ('" + a + "','" + b + "','" + c + "'," + d + ",'" + e + "','" + f + "','" + g + "','" + h + "','" + i + "','" + j + "');";
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.execute();
            con.commit();
            preparedStatement.close();
        } catch (SQLException se) {
            System.out.println("Failed to insert data");
        } finally {
            closeConnection();
        }
    }

    public void deleteOrder() {
        String fileName1 = "delete_final.csv";
        getConnection();
        try (BufferedReader infile1 = new BufferedReader(new FileReader(fileName1))){
//            String sql = "delete from \"placeOrder\" where id = " + id;
//            PreparedStatement preparedStatement = con.prepareStatement(sql);
//            preparedStatement.execute();
//            con.commit();
//            preparedStatement.close();
            String line;
            String[] parts;

            int cnt = 0;
            // Empty target table

            while ((line = infile1.readLine()) != null) {
                parts = line.split(",");
                if (parts[0].equals("contract")) continue;
                if (parts.length > 1) {
                    String sql = "delete from \"placeOrder\"\n" +
                            "where contract_num = (select contract_num from \"placeOrder\"\n" +
                            "where contract_num = '" + parts[0] + "'\n" +
                            "and salesman_num = '" + parts[1] + "'\n" +
                            "order by (estimated_delivery_date, product_model) limit 1 offset " + (Integer.parseInt(parts[2]) - 1) + ")\n" +
                            "and salesman_num = (select salesman_num from \"placeOrder\"\n" +
                            "where contract_num = '" + parts[0] + "'\n" +
                            "and salesman_num = '" + parts[1] + "'\n" +
                            "order by (estimated_delivery_date, product_model) limit 1 offset " + (Integer.parseInt(parts[2]) - 1) + ")\n" +
                            "and product_model = (select product_model from \"placeOrder\"\n" +
                            "where contract_num = '" + parts[0] + "'\n" +
                            "and salesman_num = '" + parts[1] + "'\n" +
                            "order by (estimated_delivery_date, product_model) limit 1 offset " + (Integer.parseInt(parts[2]) - 1) + ");";
                    Statement statement = con.createStatement();
                    statement.execute(sql);
                    con.commit();
                    cnt++;
                }
            }
            System.out.println(cnt + " records successfully deleted");
        } catch (SQLException se) {
            System.out.println("delete failed");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeConnection();
        }
    }

    public void updateOrder() {
        String fileName1 = "update_final_test.csv";
        getConnection();
        try (BufferedReader infile1 = new BufferedReader(new FileReader(fileName1))){
            String line;
            String[] parts;

            int cnt = 0;
            // Empty target table

            while ((line = infile1.readLine()) != null) {
                parts = line.split(",");
                if (parts[0].equals("contract")) continue;
                if (parts.length > 1) {
                    String sql = "update \"placeOrder\" \n" +
                            "set(quantity, estimated_delivery_date, lodgement_date) = (" + Integer.parseInt(parts[3]) + ",'" + parts[4] + "','" + parts[5] +"')\n" +
                            "where contract_num = '" + parts[0] + "'\n" +
                            "  and product_model = '" + parts[1] + "'\n" +
                            "  and salesman_num = '" + parts[2] + "';";
                    Statement statement = con.createStatement();
                    statement.execute(sql);
                    con.commit();
                    cnt++;
                }
            }
            System.out.println(cnt + " records successfully updated");
        } catch (SQLException | FileNotFoundException se) {
            System.out.println("modify failed");
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            closeConnection();
        }
    }

    public void select(ArrayList<String> strs) {
        getConnection();
        try {
            StringBuilder sql = new StringBuilder("select ");
            for (int i = 0; i < strs.size(); i++) {
                String str = strs.get(i);
                if (i == strs.size() - 1) sql.append(str).append(" ");
                else sql.append(str).append(", ");
            }
            sql.append("from \"placeOrder\"");

            PreparedStatement preparedStatement = con.prepareStatement(sql.toString());
            ResultSet rs = preparedStatement.executeQuery();
            ResultSetMetaData data = rs.getMetaData();
            while (rs.next()) {
                StringBuilder res = new StringBuilder();

                for (int i = 1; i <= data.getColumnCount(); i++) {
                    String columnName = data.getColumnName(i);
                    for (String str : strs)
                        if (str.equals(columnName)) {
                            String type = data.getColumnTypeName(i);
                            if (type.equals("varchar")) res.append(rs.getString(i)).append(" ");
                            else if (type.startsWith("int")) res.append(rs.getInt(i)).append(" ");
                        }
                }

                System.out.println(res);
            }
        } catch (SQLException se) {
            System.out.println("select failed");
        }
        finally {
            closeConnection();
        }
    }

    public void getContractCount() {
        getConnection();
        try {
            PreparedStatement preparedStatement = con.prepareStatement("select count(*) from (select distinct contract_num\n" +
                    "from \"placeOrder\") temp;");
            ResultSet rs = preparedStatement.executeQuery();
            while(rs.next()) System.out.println(rs.getInt("count"));
        } catch (SQLException se) {
            System.out.println("getContractCount failed");
        } finally {
            closeConnection();
        }
    }

    public PlaceOrder() {
        String fileName = "task2_test_data_final_public.csv";

        Properties defprop = new Properties();
        defprop.put("host", "localhost");
        defprop.put("user", "checker");
        defprop.put("password", "123456");
        defprop.put("database", "project2");
        Properties prop = new Properties(defprop);

        openDB(prop.getProperty("host"), prop.getProperty("database"),
                prop.getProperty("user"), prop.getProperty("password"));

        init();
        getConnection();
        Import();

        try (BufferedReader infile = new BufferedReader(new FileReader(fileName))) {
            String line;
            String[] parts;

            int cnt = 0;
            // Empty target table

            while ((line = infile.readLine()) != null) {
                parts = line.split(",");
                if (parts[0].equals("contract_num")) continue;
                if (parts.length > 1) {
                    if (!Staff.isSalesman.contains(parts[8])) continue;
                    EnterpriseOfContract.put(parts[0],parts[1]);
                    String splc = Enterprise.CenterOfEnterprise.get(parts[1]);
//                    System.out.println(splc);
                    String sqlt = "select quantity from \"stockIn\" where supply_center = '" + splc + "' and product_model = '" + parts[2] +  "';";
//                    System.out.println(sqlt);
                    Statement statement = con.createStatement();
                    ResultSet resultSet = statement.executeQuery(sqlt);
                    if(!resultSet.next()) continue;
                    int num = resultSet.getInt("quantity");
                    if (Integer.parseInt(parts[3]) > num) continue;
                    else {
                        String sqlu = "update \"stockIn\" set quantity = " + (num - Integer.parseInt(parts[3])) + " where supply_center = '" + splc + "' and product_model = '" + parts[2] +  "';";
//                        System.out.println(sqlu);
                        Statement statement1 = con.createStatement();
                        statement1.execute(sqlu);
                    }
                    loadData(parts[0], parts[1], parts[2], Integer.parseInt(parts[3]), parts[4], parts[5], parts[6], parts[7], parts[8], parts[9]);
                    cnt++;
                    if (cnt % BATCH_SIZE == 0) {
                        stmt.executeBatch();
                        stmt.clearBatch();
                    }
                }
            }
            if (cnt % BATCH_SIZE != 0) {
                stmt.executeBatch();
            }
            con.commit();
            stmt.close();
            closeDB();
            System.out.println(cnt + " records successfully loaded");
        } catch (SQLException se) {
//            se.printStackTrace();
            System.err.println("SQL error: " + se.getMessage());
            try {
                con.rollback();
                stmt.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        } catch (IOException e) {
            System.err.println("Fatal error: " + e.getMessage());
            try {
                con.rollback();
                stmt.close();
            } catch (Exception e2) {
            }
            closeDB();
            System.exit(1);
        }
        closeDB();
    }

    public void getOrderCount() {
        getConnection();
        try {
            PreparedStatement preparedStatement = con.prepareStatement("select count(*) from \"placeOrder\"");
            ResultSet rs = preparedStatement.executeQuery();
            while(rs.next()) System.out.println(rs.getInt("count"));
        } catch (SQLException se) {
            System.out.println("getOrderCount failed");
        } finally {
            closeConnection();
        }
    }

    public void getFavoriteProductModel() {
        getConnection();
        try {
            PreparedStatement preparedStatement = con.prepareStatement("""
                    select product_model, sum(quantity)
                    from "placeOrder"
                    group by product_model
                    having sum(quantity) =
                           (
                               select max(temp.sum) as max_val
                               from (select sum(quantity) as sum
                                     from "placeOrder"
                                     group by product_model) temp
                           );""");
            ResultSet rs = preparedStatement.executeQuery();
            while(rs.next()) System.out.println(rs.getString("product_model") + " " + rs.getInt("sum"));
        } catch (SQLException se) {
            System.out.println("getFavoriteProductModel failed");
        } finally {
            closeConnection();
        }
    }

    public void getNeverSoldProductCount() {
        getConnection();
        try {
            PreparedStatement preparedStatement = con.prepareStatement("""
                    select (max(cnt) - min(cnt)) res
                    from (select *
                          from (select count(*) cnt
                                from (select distinct stk.product_model, count(*) from "stockIn" stk group by stk.product_model) m) k
                          union all
                          (select count(*) cnt
                           from (select distinct stk.product_model, count(*) n
                                 from "stockIn" stk
                                          join "placeOrder" o on stk.product_model = o.product_model
                                 group by stk.product_model) n)) pl;""");
            ResultSet rs = preparedStatement.executeQuery();
            while(rs.next()) System.out.println(rs.getInt("res"));
        } catch (SQLException se) {
            System.out.println("getNeverSoldProductCount failed");
        } finally {
            closeConnection();
        }
    }

    public void getContractInfo(String cts){
        getConnection();
        try {
            PreparedStatement preparedStatement1 = con.prepareStatement(
                    "select distinct contract_num, sub1.name, enterprise, enterprise.supply_center from \"placeOrder\"\n" +
                            "join (\n" +
                            "    select * from staff where type = 'Contracts Manager'\n" +
                            "    )sub1\n" +
                            "on contract_manager = number\n" +
                            "join enterprise\n" +
                            "on \"placeOrder\".enterprise = enterprise.name\n" +
                            "where contract_num = '" + cts + "';"
            );
            PreparedStatement preparedStatement2 = con.prepareStatement(
                    "select distinct product_model, salesman_num, quantity, unit_price, estimated_delivery_date, lodgement_date from \"placeOrder\"\n" +
                            "join model\n" +
                            "on product_model = model\n" +
                            "where contract_num = '" + cts + "';"
            );
            ResultSet rs = preparedStatement1.executeQuery();
            ResultSet rs2 = preparedStatement2.executeQuery();
            while(rs.next()) {
                System.out.println(rs.getString("contract_num") + " " + rs.getString("name") + " " + rs.getString("enterprise") + " " + rs.getString("supply_center"));
            }
            while(rs2.next()) {
                System.out.println(rs2.getString("product_model") + " " + rs2.getString("salesman_num") + " " + rs2.getInt("quantity") + " " + rs2.getInt("unit_price") + " " + rs2.getString("estimated_delivery_date") + " " + rs2.getString("lodgement_date"));
            }
        } catch (SQLException se) {
            System.out.println("getContractInfo failed");
        } finally {
            closeConnection();
        }
    }

}

