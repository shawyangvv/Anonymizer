package databasewrapper;

import java.io.File;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class SqLiteWrapper implements DatabaseWrapper{
    private final String libName="sqlite_jni";
    private final String driver = "SQLite.JDBCDriver";
    private final String jdbc = "jdbc:sqlite:";
    private final String dbName = "Anonymizer.db";
    private String dbPath = null;
    private Connection conn = null;

    private static final SqLiteWrapper sqLiteInstance = new SqLiteWrapper();

    private SqLiteWrapper() {
    }

    public static SqLiteWrapper getInstance() {
        try{
            if(sqLiteInstance.conn == null || sqLiteInstance.conn.isClosed())
                sqLiteInstance.conn = sqLiteInstance.getConnection();
        }catch(Exception ex){
            ex.printStackTrace();
            return null;
        }
        return sqLiteInstance;
    }

    public boolean execute(String sql) {
        Statement stmt= null;
        try {
            stmt=sqLiteInstance.conn.createStatement();
            boolean result=stmt.execute(sql);
            stmt.close();
            return result;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
    }

    public QueryResult executeQuery(String sql) {
        QueryResult result = null;
        try {
            Statement stmt=sqLiteInstance.conn.createStatement();
            result=new QueryResult(stmt, sql);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public void commit() {
        try {
            conn.commit();
        } catch(SQLException e) {
            e.printStackTrace();
        }
    }

    public boolean flush() {
        try {
            conn.commit();
            conn.close();

            String dbPath = sqLiteInstance.dbPath +"/" +sqLiteInstance.dbName;
            File temFile = new File(dbPath);
            boolean result = temFile.delete();

            if(result){
                File dir = new File(sqLiteInstance.dbPath);
                dir.delete();
            }
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        return true;
    }

    private Connection getConnection() throws SQLException {
        loadDriver();

        sqLiteInstance.dbPath= System.getProperty("user.home") + "/Anonymizer" ;
        if (!(new File(sqLiteInstance.dbPath)).exists()){
            boolean result = (new File(sqLiteInstance.dbPath)).mkdir();
            if(!result)
                return null;
        }

        conn = DriverManager.getConnection( sqLiteInstance.jdbc + "/" + sqLiteInstance.dbPath +"/" +sqLiteInstance.dbName , "", "" );
        conn.setAutoCommit(false);
        return conn;
    }

    private void loadDriver() throws SQLException {
        try {
            System.loadLibrary(libName);
            sqLiteInstance.getClass();
            Class.forName(driver);
        } catch (ClassNotFoundException exception) {
            System.err.println("Unable to load the JDBC driver ");
            System.err.println("Please check your CLASSPATH.");
            exception.printStackTrace(System.err);
        }
    }
}
