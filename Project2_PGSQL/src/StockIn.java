import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Objects;
import java.util.Properties;

public class StockIn {
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

    public static HashMap<Msg, Integer> buf = new HashMap<Msg, Integer>();

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
            PreparedStatement preparedStatement = con.prepareStatement("truncate table \"stockIn\"");
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
            stmt = con.prepareStatement("insert into \"stockIn\" (id, supply_center, product_model, supply_staff, date, purchase_price, quantity)" + "values (?, ?, ?, ?, ?, ?, ?)");
        } catch (SQLException e) {
            System.err.println("Insert statement failed");
            System.err.println(e.getMessage());
            closeDB();
            System.exit(1);
        }
    }

    private void loadData(int a, String b, String c, String d, String e, int f, int g) throws SQLException {
        if (buf.containsKey(new Msg(b, c))) {
            modify(b, c, g + buf.get(new Msg(b, c)));
        } else {
            buf.put(new Msg(b, c), g);
            insert(a, b, c, d, e, f, g);
        }
    }

    public void insert(int a, String b, String c, String d, String e, int f, int g) {
        try {
            getConnection();
            String sql = "insert into \"stockIn\" (id, supply_center, product_model, supply_staff, date, purchase_price, quantity) values (" + a + ",'" + b + "','" + c + "','" + d + "','" + e + "'," + f + "," + g + ");";
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

    public void delete(int id, String name) {
        getConnection();
        try {
            String sql = "delete from \"stockIn\" where id = " + id;
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.execute();
            con.commit();
            preparedStatement.close();
        } catch (SQLException se) {
            System.out.println("delete failed");
        } finally {
            closeConnection();
        }
    }

    public void modify(String q, String ts, int newQ) {
        getConnection();
        try {
            String sql = "update \"stockIn\"\n" +
                    "set quantity = " + newQ + "\n" +
                    "where supply_center = '" + q + "' and product_model = '" + ts + "';";
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            preparedStatement.execute();
            con.commit();
            preparedStatement.close();
        } catch (SQLException se) {
            System.out.println("modify failed");
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
            sql.append("from \"stockIn\"");

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
        } finally {
            closeConnection();
        }
    }

    public void getAvgStockByCenter() {
        getConnection();
        try {
            PreparedStatement preparedStatement = con.prepareStatement("""
                    select supply_center, avg(quantity) as avg
                    from "stockIn"
                    group by supply_center
                    order by supply_center;""");
            ResultSet rs = preparedStatement.executeQuery();
            while(rs.next()){
                System.out.print(rs.getString("supply_center") + " ");
                System.out.printf("%.1f\n",  rs.getDouble("avg"));
            }
        } catch (SQLException se) {
            System.out.println("getAvgStockByCenter failed");
        } finally {
            closeConnection();
        }
    }

    public StockIn() {
        String fileName = "in_stoke_test.csv";

        Properties defprop = new Properties();
        defprop.put("host", "localhost");
        defprop.put("user", "checker");
        defprop.put("password", "123456");
        defprop.put("database", "project2");
        Properties prop = new Properties(defprop);

        openDB(prop.getProperty("host"), prop.getProperty("database"),
                prop.getProperty("user"), prop.getProperty("password"));

        init();

        try (BufferedReader infile = new BufferedReader(new FileReader(fileName))) {
            String line;
            String[] parts;

            // Empty target table

            while ((line = infile.readLine()) != null) {
                parts = line.split(",");
                if (parts[0].equals("id")) continue;
                if (parts.length != 0) {
                    if (parts[1].contains("\"")) {
                        if (!Staff.isSupplyStaff.contains(parts[4])) continue;
                        String temp = parts[1].substring(1) + "," + parts[2].substring(0, parts[2].length() - 1); // the HK center
                        if (!Center.setOfCenter.contains(temp)) continue;
                        if (!Model.setOfModel.contains(parts[3])) continue;
                        if (!Staff.CenterOfStaff.get(parts[4]).equals(temp)) continue;
                        loadData(Integer.parseInt(parts[0]), temp, parts[3], parts[4], parts[5], Integer.parseInt(parts[6]), Integer.parseInt(parts[7]));
                    } else {
                        if (!Staff.isSupplyStaff.contains(parts[3])) continue;
                        if (!Center.setOfCenter.contains(parts[1])) continue;
                        if (!Model.setOfModel.contains(parts[2])) continue;
                        if (!Staff.CenterOfStaff.get(parts[3]).equals(parts[1])) continue;
                        loadData(Integer.parseInt(parts[0]), parts[1], parts[2], parts[3], parts[4], Integer.parseInt(parts[5]), Integer.parseInt(parts[6]));
                    }

                }
            }

            closeDB();
        } catch (SQLException se) {
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

    public void getProductByNumber(String target) {
        getConnection();
        try {
            String sql = "select supply_center, temp.number as product_number, product_model, purchase_price, quantity\n" +
                    "from \"stockIn\"\n" +
                    "     join\n" +
                    "(select number, model\n" +
                    "from model as mo where number = '" + target + "') temp on temp.model = \"stockIn\".product_model;\n";
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            ResultSet rs = preparedStatement.executeQuery();
            while(rs.next()){
                System.out.println(rs.getString("supply_center") + " " + rs.getString("product_number") + " " + rs.getString("product_model") + " " + rs.getInt("purchase_price") + " " + rs.getInt("quantity"));
            }
        } catch (SQLException se) {
            System.out.println("getProductByNumber failed");
        } finally {
            closeConnection();
        }
    }

    private static class Msg {
        String x, y;

        public Msg() {
        }

        public Msg(String x, String y) {
            this.x = x;
            this.y = y;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Msg pss = (Msg) o;
            return this.x.equals(pss.x) && this.y.equals(pss.y);
        }

        @Override
        public int hashCode() {
            return Objects.hash(x, y);
        }
    }
}