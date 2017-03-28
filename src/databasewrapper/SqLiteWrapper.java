package databasewrapper;

import java.io.File;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import anonymizer.Configuration;

public class SqLiteWrapper implements DatabaseWrapper{
    private final String jdbc = "jdbc:sqlite:";
    private String dbName = null;
    private String dbPath = null;
    private Connection conn = null;

    private static SqLiteWrapper sqLiteInstance = null;

    private SqLiteWrapper() {
    }
    
    public static void initialize(Configuration conf) {
    	if (sqLiteInstance != null) {
    		throw new IllegalArgumentException("sqLiteInstance has been initialized");
    	}
    	
    	if (conf.sqlitefilePath == null || conf.sqlitefilePath.trim().isEmpty()) {
    		throw new IllegalArgumentException("sqlitefile is either not specified or is empty");
    	}
    	
    	SqLiteWrapper.sqLiteInstance = new SqLiteWrapper();
    	
    	File sqliteFileDirectory = new File(conf.sqlitefilePath.trim()).getParentFile();
    	if (sqliteFileDirectory == null) {
    		SqLiteWrapper.sqLiteInstance.dbPath = new File("dummyfile").getAbsoluteFile().getParent();
    	} else {
    		SqLiteWrapper.sqLiteInstance.dbPath = sqliteFileDirectory.getAbsolutePath();
    	}
    	
    	SqLiteWrapper.sqLiteInstance.dbName = new File(conf.sqlitefilePath.trim()).getName();
    	
    	
    	
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
        if (!(new File(sqLiteInstance.dbPath)).exists()){
            boolean result = (new File(sqLiteInstance.dbPath)).mkdir();
            if(!result)
                return null;
        }

        conn = DriverManager.getConnection( sqLiteInstance.jdbc + "/" + sqLiteInstance.dbPath +"/" +sqLiteInstance.dbName , "", "" );
        conn.setAutoCommit(false);
        return conn;
    }
}
