/**
 * Source is Provided as is.
 * You are free to use it for whatever you want.
 * If you want to modify it, please contact me.
 * */

package de.md;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.persistence.EntityManager;
import javax.persistence.EntityManagerFactory;
import javax.persistence.Persistence;
import javax.sql.DataSource;

import junit.framework.TestCase;

import org.hsqldb.jdbc.jdbcDataSource;

/**
 * Simple Unit Test, which enabled setting up a Database by sql-scripts.
 * Testcases allows for Smoke-Testing of JPA-Applications.
 * You need to implement the following Methods in order to set up 
 * the Testcase correctly:
 * {@link #getDatabaseProperties()} returns the Path to a Properties File, which contains
 * all needed Properties to set up an EntityManager, e.g.
 * 
 * hibernate.connection.username = sa
 * hibernate.connection.password =
 * hibernate.connection.url = jdbc:hsqldb:mem:mydatabase"
 * hibernate.connection.driver_class=org.hsqldb.jdbcDriver
 * hibernate.dialect=org.hibernate.dialect.HSQLDialect
 * 
 * {@link #getSqlSetUpScript()} - Path to an sql script which contains the sql-statements to 
 * set up the db schema (e.g. create table demo...)
 * {@link #getSqlTearDownScript()} - Path to an sql script which contains the sql-statements to tear down 
 * a schema (e.g. drop table...)
 * 
 * Currently the Testcase works just with HSQL and I am fine with it.
 * If you want to improve, feel free to contact me about it at
 * <a href="mailto:martin.dilger@googlemail.com"/>
 * @author md
 * */
public abstract class DatabaseTestCase extends TestCase {

    protected String[] tableNames;
    
    protected Map<String, String> databaseProperties;
    
    /**
     * initializes the schema
     * */
    public void setUp() throws Exception{
	databaseProperties = initDataBaseProperties();
	initDatabase();
    }

    public void tearDown() {
	dropDatabase();
    }

    private void dropDatabase() {
	try {
	    String[] sql = extractSql(getSqlTearDownScript());
	    Statement statement = ((Connection) obtainDataSource()
		    .getConnection()).createStatement();
	    for (String s : sql) {
		statement.execute(s);
	    }
	} catch (Exception e) {
	    throw new RuntimeException("Error dropping schema", e);
	}
    }

    /**
     * Sets up the Database-Schema.
     * */
    private void initDatabase() {
	try {
	    DataSource ds = obtainDataSource();
	    String sqlScript = getSqlSetUpScript();
	    String[] sqls = extractSql(sqlScript);
	    tableNames = extractTableNames(sqls);
	    Statement statement = ((Connection) ds.getConnection())
		    .createStatement();
	    for (String s : sqls) {
		statement.executeUpdate(s);
		

	    }
	} catch (Exception rex) {
	    throw new RuntimeException("Error setting up database", rex);
	}
    }

    /**
     * @param sqls
     * @return
     */
    private String[] extractTableNames(String[] sqls) {
	List<String> result = new ArrayList<String>();
	for(String sql : sqls){
	    if(sql.startsWith("create table")){
		String tableName = sql.substring(13);
		tableName = tableName.substring(0, tableName.indexOf(" "));
		result.add(tableName);
	    }
	}
	return result.toArray(new String[0]);
    }

    private DataSource obtainDataSource() {
	jdbcDataSource ds = new jdbcDataSource();
	ds.setDatabase(databaseProperties.get("hibernate.connection.url"));
	ds.setUser("sa");
	ds.setPassword("");
	return ds;
    }

    private String[] extractSql(String sqlScript) throws IOException {
	List sqls = new ArrayList();
	BufferedReader reader = new BufferedReader(new FileReader(sqlScript));
	String line = null;
	while ((line = reader.readLine()) != null) {
	    sqls.add(line);
	}
	return (String[]) sqls.toArray(new String[0]);

    }

    protected abstract String getSqlSetUpScript();
    
    protected abstract String getSqlTearDownScript();
    
    protected abstract String getDatabaseProperties();
    
    /**
     * Traces all contents of a Database
     * */
    public void traceDatabase(Writer writer, String tableName) throws Exception{
	DataSource ds = obtainDataSource();
	for(String tbName : tableNames){
	    if(tableName == null || tableName.equals(tbName)){
	    ResultSet st = ds.getConnection().createStatement().executeQuery("select * from " + tbName);
	    int index = 1;
	    int cCnt = st.getMetaData().getColumnCount();
	    writer.write("--- Table " + tbName + "---" + "\n");
	    while(st.next()){
		writer.write("#Entity " + (index-1) + "\n");
		for (int i = 1; i < cCnt; i++) {
		    String result = st.getString(index);
		    String cName = st.getMetaData().getColumnLabel(i);
			writer.write(cName + " : ");
		    	writer.write(result);
			writer.append("\n");
			
			}    
		}
	    index++;
	    }
	}
    }
    
    public void traceDatabase(Writer writer) throws Exception{
	traceDatabase(writer, null);
    }
    
    /**
     * @param bean2
     */
    protected EntityManager injectEntityManager(String puName, String emName,  Object bean2) throws Exception {
	EntityManagerFactory factory = Persistence.createEntityManagerFactory(puName, initDataBaseProperties());
	EntityManager manager = factory.createEntityManager();
	Field field = bean2.getClass().getDeclaredField(emName);
	field.setAccessible(true);
	field.set(bean2, manager);
	return manager;
    }
    
    protected Map<String, String> initDataBaseProperties() throws IOException{
	
	if(databaseProperties == null){
	databaseProperties = new HashMap<String, String>();
	
	String path = getDatabaseProperties();
	Properties props = new Properties();
	props.load(new FileInputStream(new File(path)));
	Set<Map.Entry<Object, Object>> elements = props.entrySet();
	for(Map.Entry<Object, Object> resultObj : elements){
	    String key = resultObj.getKey().toString();
	    String value = resultObj.getValue().toString();
	    databaseProperties.put(key, value);
	}
	}
	return databaseProperties;
    }
    
    
}
