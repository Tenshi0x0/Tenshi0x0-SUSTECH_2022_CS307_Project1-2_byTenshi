import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.sql.*;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;

public class Center {
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

    public static HashSet<String> setOfCenter = new HashSet<String>();

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
            PreparedStatement preparedStatement = con.prepareStatement("truncate table center");
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
            stmt = con.prepareStatement("insert into center (id, name)" + "values (?, ?)");
        } catch (SQLException e) {
            System.err.println("Insert statement failed");
            System.err.println(e.getMessage());
            closeDB();
            System.exit(1);
        }
    }

    private static void loadData(int id, String name) throws SQLException {
        if (con != null) {
            stmt.setInt(1, id);
            stmt.setString(2, name);
            setOfCenter.add(name);
            stmt.addBatch();
        }
    }

    public void insert(int id, String name) {
        try {
            getConnection();
            String sql = "insert into center (id, name) values (" + id + ",'" + name + "');";
            PreparedStatement preparedStatement = con.prepareStatement(sql);
            setOfCenter.add(name);
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
            String sql = "delete from center where id = " + id +
                    "and name = '" + name + "';";
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

    public void modify(int id, String name, int newId, String newName) {
        getConnection();
        try {
            String sql = "update center\n" +
                    "set id = " + newId + ", name = '" + newName + "'\n" +
                    "where id = " + id + " and name = '" + name + "'";
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
            sql.append("from center");

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

    public Center() {
        String fileName = "center.csv";

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
            long start;
            long end;
            String line;
            String[] parts;

            int cnt = 0;
            // Empty target table

            start = System.currentTimeMillis();

            while ((line = infile.readLine()) != null) {
                parts = line.split(",");
                if (parts.length > 1) {

                    if (parts[1].equals("\"Hong Kong")) {
                        loadData(Integer.parseInt(parts[0]), "Hong Kong, Macao and Taiwan regions of China");
                    } else loadData(Integer.parseInt(parts[0]), parts[1]);

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
            end = System.currentTimeMillis();
            System.out.println(cnt + " records successfully loaded");
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
}

