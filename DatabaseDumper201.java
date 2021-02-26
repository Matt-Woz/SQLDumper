import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.*;

/**
 * Class which needs to be implemented.  ONLY this class should be modified
 */
public class DatabaseDumper201 extends DatabaseDumper {

    /**
     * @param c    connection which the dumper should use
     * @param type a string naming the type of database being connected to e.g. sqlite
     */
    public DatabaseDumper201(Connection c, String type) {
        super(c, type);
    }

    /**
     * @param c connection to a database which will have a sql dump create for
     */
    public DatabaseDumper201(Connection c) {
        super(c, c.getClass().getCanonicalName());
    }


    public List<String> getTableNames()
    {
        List<String> result = new ArrayList<>();
        try {
            DatabaseMetaData md = getConnection().getMetaData();
            ResultSet rs = md.getTables(null, null, "%", null);
            while (rs.next()) {
                if (rs.getString(4).equalsIgnoreCase("TABLE") && !rs.getString(3).contains("sqlite_")) { //Filters out any index & sqlite tables
                    result.add(rs.getString(3)); //Add table to list
                }
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return result;
    }

    /**
     * Takes in all tables in database and sorts them by foreign key so no not-yet created table is referenced
     * @return List of sorted tables
     */
    public List<String> sortedTables()
    {
        List<String> sortedTables = new ArrayList<>();
        List<String>tables = getTableNames();
        try
        {
            DatabaseMetaData md = getConnection().getMetaData();
            for(String table : tables)
            {
                ResultSet foreignKeys = md.getImportedKeys(null, null, table);
                while(foreignKeys.next())
                {
                    int index = sortedTables.indexOf(table);
                    String referTableName = foreignKeys.getString("PKTABLE_NAME"); //Gets table that is referred to
                    if(index == -1) //If list does not contain element
                    {
                        if(!sortedTables.contains(referTableName)) //Sorting algorithm
                        {
                            sortedTables.add(referTableName);
                        }
                        if(!sortedTables.contains(table))
                        {
                            sortedTables.add(table);
                        }
                    }
                    else
                    {
                        if(!sortedTables.contains(referTableName))
                        {
                            sortedTables.add(index, referTableName);
                        }
                        else if(sortedTables.indexOf(referTableName) > index)
                        {
                            sortedTables.remove(referTableName);
                            sortedTables.add(index, referTableName);
                        }
                    }
                }
                if(!sortedTables.contains(table)) //If does list does not contain table, add it
                {
                    sortedTables.add(table);
                }

            }
        }
        catch (SQLException throwables)
        {
            throwables.printStackTrace();
        }
        return sortedTables;
    }

    @Override
    public List<String> getViewNames()
    {
        List<String> result = new ArrayList<>();
        try {
            DatabaseMetaData md = getConnection().getMetaData();
            ResultSet rs = md.getTables(null, null, null, new String[]{"VIEW"}); //Gets views as tables
            while (rs.next()) {
                String tableName = rs.getString("TABLE_NAME");
                result.add(tableName);
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return result;
    }

    public String getDriverInfo()
    {
        String info = "";
        try
        {
            DatabaseMetaData md = getConnection().getMetaData();
            int majorDBVersion = md.getDatabaseMajorVersion();
            int minorDBVersion = md.getDatabaseMinorVersion();
            int majorJDBCVersion = md.getJDBCMajorVersion();
            int minorJDBCVersion = md.getJDBCMinorVersion();
            info = "-- Major database version: " + majorDBVersion + "\n-- Minor database version: " + minorDBVersion + "\n-- Major JDBC version: "
                    + majorJDBCVersion + "\n-- Minor JDBC Version: " + minorJDBCVersion + "\n---\n";

        }
        catch (SQLException throwables)
        {
            throwables.printStackTrace();
        }
        return info;
    }

    @Override
    public String getDDLForTable(String tableName)
    {

        StringBuilder sqlCreateStatement = new StringBuilder("DROP TABLE IF EXISTS \""+ tableName + "\";\n" + "CREATE TABLE " + '"' + tableName + '"' + " (\n"); //Create table statement
        return getDDL(tableName, sqlCreateStatement);
    }

    @Override
    public String getInsertsForTable(String tableName)
    {
        StringBuilder insert = new StringBuilder();
        try {
            Statement statement = getConnection().createStatement();
            ResultSet rs = statement.executeQuery("SELECT * FROM " + tableName);
            getInserts(tableName, insert, rs);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return insert.toString();
    }

    @Override
    public String getDDLForView(String viewName)
    {
        StringBuilder sqlCreateStatement = new StringBuilder("CREATE TABLE " + "\"view_" + viewName + "\" (\n"); //Create table statement for the view
        return getDDL(viewName, sqlCreateStatement);
    }

    /**
     * private method with common code in both getDDLForTable and getDDLForView
     * @param tableName table being used for DDL
     * @param sqlCreateStatement create table statement
     * @return Create table/view statement
     */
    private String getDDL(String tableName, StringBuilder sqlCreateStatement) {
        boolean firstLine = true; //True if on the first line
        try {
            DatabaseMetaData md = getConnection().getMetaData();
            ResultSet rs = md.getColumns(null, null, tableName, "%");
            while (rs.next()) {
                if (!firstLine) {
                    sqlCreateStatement.append(", \n");
                }
                firstLine = false;
                String columnName = '"' + rs.getString("COLUMN_NAME") + '"'; //Gets column
                String typeName = rs.getString("TYPE_NAME"); //gets data type
                String isNullable = rs.getString("IS_NULLABLE"); //For not null values
                String nullable;
                if (isNullable.equalsIgnoreCase("n")) { //If no nulls allowed in column
                    nullable = " NOT NULL";
                } else {
                    nullable = "";
                }
                sqlCreateStatement.append("  ").append(columnName).append(" ").append(typeName).append(nullable);
            }
            rs.close();
            ResultSet primaryKeys = md.getPrimaryKeys(null, null, tableName);
            firstLine = true;
            boolean empty = true; //Used for when 2 fields in primary key
            while (primaryKeys.next()) {
                empty = false;
                if (firstLine) {
                    sqlCreateStatement.append(",\n  PRIMARY KEY (");
                } else {
                    sqlCreateStatement.append(", ");
                }
                firstLine = false;
                sqlCreateStatement.append('"').append(primaryKeys.getString("COLUMN_NAME")).append('"'); //Adds primary key attribute
            }
            if (!empty) {
                sqlCreateStatement.append(")");
            }
            primaryKeys.close();
            ResultSet foreignKeys = md.getImportedKeys(null, null, tableName); //Gets foreign keys
            while (foreignKeys.next()) {
                sqlCreateStatement.append(",\n");
                String foreignTable = foreignKeys.getString("PKTABLE_NAME"); //Gets table foreign key references
                String foreignColumn = foreignKeys.getString("PKCOLUMN_NAME"); //Gets column foreign key references
                String foreignKey = foreignKeys.getString("FKCOLUMN_NAME"); //Gets foreign key in this table
                sqlCreateStatement.append("  FOREIGN KEY ").append("(\"").append(foreignKey).append("\")").append(" REFERENCES \"").append(foreignTable).append("\"(\"").append(foreignColumn).append('"').append(')');

            }
            foreignKeys.close();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        sqlCreateStatement.append("\n);\n");
        return sqlCreateStatement.toString();
    }

    /**
     * Gets inserts for the view table
     * @param viewName view to get inserts for
     * @return string of inserts for view
     */
    public String getInsertsForView(String viewName)
    {
        StringBuilder insert = new StringBuilder();
        try {
            Statement statement = getConnection().createStatement();
            ResultSet rs = statement.executeQuery("SELECT * FROM " + viewName); //SQL query to get inserts
            viewName = "\"view_" + viewName + "\""; //Required view_ format
            getInserts(viewName, insert, rs);
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
        return insert.toString();
    }

    /**
     * private method with common code for getting inserts for tables and views
     * @param tableName table for which to get inserts
     * @param insert statement to
     * @param rs Result set used for iterating through inserts
     * @throws SQLException sqlException
     */
    private void getInserts(String tableName, StringBuilder insert, ResultSet rs) throws SQLException {
        ResultSetMetaData md = rs.getMetaData();
        String replacement;
        int columns = md.getColumnCount();
        while (rs.next()) {
            insert.append("INSERT INTO ").append(tableName).append(" VALUES(");
            for (int i = 1; i < columns + 1; i++) { //Iterates through all columns
                if (i > 1) {
                    insert.append(", ");
                }
                if (md.getColumnTypeName(i).equalsIgnoreCase("integer")) {
                    insert.append(rs.getString(i)); //append the int
                } else if(rs.getString(i) != null) { //Does not append if null
                    replacement = rs.getString(i).replace("'", "''"); //In case of '' in names
                    insert.append("'").append(replacement).append("'");
                }
            }
            insert.append(");\n");
        }
        rs.close();
    }

    @Override
    public String getDumpString()
    {
        String info = getDriverInfo();
        List<String> views = getViewNames();
        List<String> orderedTables = sortedTables();
        StringBuilder tables = new StringBuilder();
        StringBuilder inserts = new StringBuilder();
        StringBuilder indexes = new StringBuilder();
        StringBuilder viewTables = new StringBuilder();
        StringBuilder viewInserts = new StringBuilder();
        String comment = "\n-- Views: \n";

        for (String tableName : orderedTables) //Iterates through all tables
        {
            tables.append(getDDLForTable(tableName));
            tables.append("---\n");
            inserts.append(getInsertsForTable(tableName));
            indexes.append(getDatabaseIndexes());
        }
        for (String view : views) //Iterates through all views
        {
            viewTables.append(getDDLForView(view));
            viewTables.append("---\n");
            viewInserts.append(getInsertsForView(view));
        }
        String appendInserts = inserts.toString();
        String appendIndexes = indexes.toString();
        String appendViewInserts = viewInserts.toString();
        appendInserts = appendInserts.replace(";", ";\n---");
        appendIndexes = appendIndexes.replace(";", ";\n---");
        appendViewInserts = appendViewInserts.replace(";", "\n---");

        return info + tables.toString() + appendInserts + appendIndexes + comment + viewTables.toString() + appendViewInserts;
    }

    @Override
    public void dumpToFileName(String fileName)
    {
        try
        {
            FileWriter myWriter = new FileWriter(fileName);
            myWriter.write(getDumpString());
            myWriter.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void dumpToSystemOut()
    {
        dumpToFileName("DBDump.sql");
        System.out.println(getDumpString());
    }

    /**
     * private method for finding order of index - Sqlite bug only returns null
     * @param order asc or descending or null
     * @return order to append for sql
     */
    private String order(String order)
    {
        if(order == null)
        {
            return ""; //Return empty string
        }
        else if(order.equals("A"))
        {
            return "ASC";
        }
        else
        {
            return "DSC";
        }
    }

    @Override
    public String getDatabaseIndexes()
    {
        StringBuilder indexStatements = new StringBuilder();
        try
        {
            List<String> tables = getTableNames();
            for (String table : tables)
            {
                boolean index = false;
                boolean firstLine = true;

                StringBuilder statement = new StringBuilder();
                DatabaseMetaData md = getConnection().getMetaData();
                ResultSet rs = md.getIndexInfo(null, null, table, true, false); //Result set for index info
                while(rs.next())
                {
                    String getOrder = rs.getString("ASC_OR_DESC"); //Usually would get order for the index
                    String order = order(getOrder);
                    index = true;
                    String columnName = rs.getString("COLUMN_NAME");
                    if(firstLine)
                    {
                        String indexName = rs.getString("INDEX_NAME");
                        if(indexName.contains("sqlite_autoindex")) //Filters out sqlite autoindexes
                        {
                            index = false;
                            break;
                        }
                        statement.append("CREATE INDEX '").append(indexName).append("' ON ").append(table).append(" (").append(columnName).append(order);
                        firstLine = false;
                    }
                    else
                    {
                        statement.append(",").append(columnName).append(order);
                    }
                }
                if(index)
                {
                    statement.append(");\n");
                }
                indexStatements.append(statement);
            }

        } catch (SQLException throwables)
        {
            throwables.printStackTrace();
        }
        return indexStatements.toString();
    }
}