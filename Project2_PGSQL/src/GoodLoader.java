import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URL;
import java.sql.*;
import java.util.Properties;

public class GoodLoader {
    private static final int BATCH_SIZE = 500;
    private static URL propertyURL = GoodLoader.class
            .getResource("/loader.cnf");

    private static Connection con = null;
    private static PreparedStatement stmt = null;
    private static boolean verbose = false;

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
        try {
            stmt = con.prepareStatement("insert into \"order\"(id,quantity,estimated_delivery_date,lodgement_date,contract_id,salesman_number,product_code,product_model,unit_price)"
                    + " values(?,?,?,?,?,?,?,?,?)");
        } catch (SQLException e) {
            System.err.println("Insert statement failed");
            System.err.println(e.getMessage());
            closeDB();
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

    private static void loadData(int contract_number, int quantity, Date e_d_date, Date l_date, String contract_id, int salesman, String product_code, String model, int price)
            throws SQLException {
        if (con != null) {
            stmt.setInt(1, contract_number);
            stmt.setInt(2, quantity);
            stmt.addBatch();
        }
    }

    public static void main(String[] args) {
        String fileName = null;
        boolean verbose = false;

        switch (args.length) {
            case 1:
                fileName = args[0];
                break;
            case 2:
                switch (args[0]) {
                    case "-v":
                        verbose = true;
                        break;
                    default:
                        System.err.println("Usage: java [-v] GoodLoader filename");
                        System.exit(1);
                }
                fileName = args[1];
                break;
            default:
                System.err.println("Usage: java [-v] GoodLoader filename");
                System.exit(1);
        }

        Properties defprop = new Properties();
        defprop.put("host", "localhost");
        defprop.put("user", "checker");
        defprop.put("password", "123456");
        defprop.put("database", "project2");
        Properties prop = new Properties(defprop);

        try (BufferedReader infile = new BufferedReader(new FileReader(fileName))) {
            long start;
            long end;
            String line;
            String[] parts;
            String contract_id;
            String e_d_date;
            String l_date;
            int cnt = 0;
            // Empty target table
            openDB(prop.getProperty("host"), prop.getProperty("database"),
                    prop.getProperty("user"), prop.getProperty("password"));
            Statement stmt0;
            closeDB();
            //
            start = System.currentTimeMillis();
            openDB(prop.getProperty("host"), prop.getProperty("database"),
                    prop.getProperty("user"), prop.getProperty("password"));
            while ((line = infile.readLine()) != null) {
                parts = line.split(",");
                if (parts.length > 1) {
                    contract_id = parts[0];
                    e_d_date = parts[12];
                    String[] a = e_d_date.split("-");
                    int[] b = new int[3];
                    for (int i = 0; i < 3; i++) {
                        b[i] = Integer.parseInt(a[i]);
                    }
                    l_date = parts[13];
                    if (!l_date.equals("")) {
                        String[] c = l_date.split("-");
                        int[] d = new int[3];
                        for (int i = 0; i < 3; i++) {
                            d[i] = Integer.parseInt(c[i]);
                        }

                        loadData(cnt + 1, Integer.parseInt(parts[10]), new Date(b[0] - 1900, b[1] - 1, b[2]), new Date(d[0] - 1900, d[1] - 1, d[2]), contract_id, Integer.parseInt(parts[16]), parts[6], parts[8], Integer.parseInt(parts[9]));
                    } else {
                        loadData(cnt + 1, Integer.parseInt(parts[10]), new Date(b[0] - 1900, b[1] - 1, b[2]), null, contract_id, Integer.parseInt(parts[16]), parts[6], parts[8], Integer.parseInt(parts[9]));
                    }
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
            System.out.println("Loading speed : "
                    + (cnt * 1000) / (end - start)
                    + " records/s");
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

