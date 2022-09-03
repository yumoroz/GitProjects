/*
 * AbstractProcessor
 * Created on Jun 8, 2008
 *
 */
package com.invictusinformatics.util;

//import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
//import java.sql.Types;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Map;


/**
 * @author Owner
 *
 */
public abstract class AbstractProcessor
{
	enum SqlTypes {POSTGRES, ORACLE};
	
    private String 	username = "";
    private String 	password = "";
    private String 	dbName = "";
    private boolean     commit = false;
    private static SqlTypes dbType = SqlTypes.POSTGRES;

    private Map<String, String> keyParamMap = null;

    private Connection connection = null;

    final public static int	MAX_CONTEXT_LENGTH = 1000; //column size in database


    /**
     *
     */
    public AbstractProcessor()
    {
    	super();
    }

    /**
     * @param username
     * @param password
     * @param database
     * @return Connection
     */
    static public Connection createDBConnection(String usernameIn, String passwordIn, String databaseIn)
    {
        Connection conn = null;
        String url = null;

        if (databaseIn.equalsIgnoreCase("cgmmtest"))
        {
            url = "jdbc:oracle:thin:@ulbcenter-06-b13:1521:cgmmtest"; //136.165.233.160:1521:cgmmtest";
        }
        else if (databaseIn.equalsIgnoreCase("cgmmprod"))
        {
            url = "jdbc:oracle:thin:@ulbcenter-06-b14:1521:cgmmprod";
        }
        else if (databaseIn.equalsIgnoreCase("localtest"))
        {
            url = "jdbc:oracle:thin:@localhost:1522:cgmmtest";
        }
        else if (databaseIn.equalsIgnoreCase("localprod"))
        {
            url = "jdbc:oracle:thin:@localhost:1521:cgmmprod";
        }
        else if (databaseIn.equalsIgnoreCase("INFXTST1"))
        {
            url = "jdbc:oracle:thin:@infxtst-rac1.hpc.louisville.edu:1521:INFXTST1";
        }
        else if (databaseIn.equalsIgnoreCase("UOLDBA1"))
        {
            url = "jdbc:oracle:thin:@uoldba-rac1.hpc.louisville.edu:1521:UOLDBA1";
        }
        else if (databaseIn.equalsIgnoreCase("INFXPRD1"))
        {
            url = "jdbc:oracle:thin:@cgmmprod-rac1.hpc.louisville.edu:1521:INFXPRD1";
        }
        else if (databaseIn.equalsIgnoreCase("INFXPRD"))
        {
            url = "jdbc:oracle:thin:@(DESCRIPTION = (LOAD_BALANCE=ON)"
                + "(ADDRESS = (PROTOCOL = TCP)(HOST = 136.165.228.195)(PORT = 1521))"
                + " (ADDRESS = (PROTOCOL = TCP)(HOST = 136.165.228.196)(PORT = 1521))"
                + " (ADDRESS = (PROTOCOL = TCP)(HOST = 136.165.228.197)(PORT = 1521))"
                + " (ADDRESS = (PROTOCOL = TCP)(HOST = 136.165.228.198)(PORT = 1521))"
                + " (CONNECT_DATA ="
                + "  (SERVER = DEDICATED)"
                + "  (SERVICE_NAME = INFXPRD.hpc.louisville.edu)"
                + "  (failover_mode = (type=select)(method=basic)(retries=180)(delay=5))"
                + " ))";
        }
        else if (databaseIn.equalsIgnoreCase("kalbprod"))
        {
            url = "jdbc:oracle:thin:@ulbcenter-06-b06.it-servers.louisville.edu:1521:kalbprod";
        }
        else if (databaseIn.equalsIgnoreCase("INFRMTST"))
        {
            url = "jdbc:oracle:thin:@ulbcenter-06-b06.it-servers.louisville.edu:1521:INFRMTST";
        }
        else if (databaseIn.equalsIgnoreCase("INFRMPRD"))
        {
            url = "jdbc:oracle:thin:@ulbcenter-06-b13.it-servers.louisville.edu:1521:INFRMPRD";
        }
        else if (databaseIn.equalsIgnoreCase("localpostgres"))
        {
        	url = "jdbc:postgresql://localhost:5432/postgres";
        }

        System.out.println("Connecting to " + url + " as " + usernameIn);
        try
        {
        	switch (dbType)
        	{
        	case POSTGRES: 
        		Class.forName("org.postgresql.Driver");
        		break;
        	case ORACLE: 
        		Class.forName("oracle.jdbc.OracleDriver");
        		break;	
        	}
            conn = DriverManager.getConnection(url, usernameIn, passwordIn);
            conn.setAutoCommit(false);
        }
        catch (SQLException sqlException)
        {
            sqlException.printStackTrace();
            return null;
        }
        catch (ClassNotFoundException classNotFoundException)
        {
            classNotFoundException.printStackTrace();
            return null;
        }
        System.out.println("Connected!");
        return conn;
    }

    public Connection connectDB(String usernameIn, String passwordIn, String databaseIn)
    {
        Connection conn = createDBConnection(usernameIn, passwordIn, databaseIn);
        setConnection(conn);
        return conn;
    }

    protected Map<String, String> parseCommandLine( String[] argsIn )
    {
        Map<String, String> params = new HashMap<String, String>();
        for (int iArg = 0; iArg < argsIn.length; iArg++)
        {
            String [] tokens = argsIn[iArg].split("=");
            String key = null;
            String param = null;
            if (tokens.length != 2)
            {
                System.err.println("Parameter " + argsIn[iArg] + " is not is correct format: key=value" );
                //System.exit(1);
                key = argsIn[iArg];
            }
            else
            {
            	key = tokens[0];
            	param = tokens[1];
            }
            params.put(key, param);

            if (key.equals("user"))
            {
                setUsername(param);
            }
            else if (key.equals("password"))
            {
            	setPassword(param);
            }
            else if (key.equals("db"))
            {
            	setDbName(param);
            }
            else if (key.equals("commit"))
            {
                if (param.equalsIgnoreCase("yes"))
                {
                	setCommit(true);
                }
                else if (param.equalsIgnoreCase("no"))
                {
                	setCommit(false);
                }
                else
                {
                    System.err.println("Parameter " + param + " is the wrong value for commit ");
                    System.err.println("Use commit=yes or commit=no, default is no");
                }
            }
        }

        setKeyParamMap(params);
        return params;
    }

    /**
     * @param string speciesNameIn
     * @return
     * @throws SQLException
     */
    protected long getSpeciesIdFromDB(String speciesNameIn) throws SQLException
    {
        long speciesId = -1;
        //try {
            Statement st = getConnection().createStatement();
            String query = "select species_id from species where species_name = '"
                    + speciesNameIn + "'";
            System.out.println(query);
            ResultSet result = st.executeQuery(query);

            if (result.next())
            {
                    speciesId = result.getLong("species_id");
            }

            result.close();
            st.close();
        //} catch (SQLException e) {
                // TODO Auto-generated catch block
        //	e.printStackTrace();
        //}

        return speciesId;
    }

    protected long getChromosomeIdFromDB(String chromosomeNameIn, String genomeBuildNameIn) throws SQLException
    {
        long t1 = (new GregorianCalendar()).getTimeInMillis();
        long chrId = -1;

        Statement st = getConnection().createStatement();
        String query = "select chromosome_id from chromosome c, genome_build gb "
                + " where gb.genome_build_id = c.genome_build_id and genome_build_name = '" + genomeBuildNameIn
                + "' and chromosome_name = '" + chromosomeNameIn + "'";
        //System.out.println(query);
        ResultSet result = st.executeQuery(query);

        if (result.next())
        {
            chrId = result.getLong("chromosome_id");
        }
        result.close();
        st.close();

        long t2 = (new GregorianCalendar()).getTimeInMillis();
        System.out.println("Query done in " + (t2-t1) + " ms\t" + query);

        return chrId;
    }


/*
    public long createPolymorphism(PolymorphismSubmission polySubIn) throws SQLException
    {
        String leftContext = polySubIn.getLeftContext();
        String rightContext = polySubIn.getRightContext();
	if (leftContext.length() > MAX_CONTEXT_LENGTH)
        {
        	System.out.println("Cut left context from " + leftContext.length() + " to " + MAX_CONTEXT_LENGTH);
        	leftContext = leftContext.substring(leftContext.length() - MAX_CONTEXT_LENGTH );
        }
        if (rightContext.length() > MAX_CONTEXT_LENGTH)
        {
        	System.out.println("Cut right context from " + rightContext.length() + " to " + MAX_CONTEXT_LENGTH);
        	rightContext = rightContext.substring(0, MAX_CONTEXT_LENGTH);
        }

		long t1 = (new GregorianCalendar()).getTimeInMillis();
		String templatep = "{call create_polymorphism(?, ?, ?, ?, ?, ?, ?)}";
		//PreparedStatement pSt = getConnection().prepareStatement(templatep);
		CallableStatement pSt = getConnection().prepareCall(templatep);
		// persistPolymorphism through storedprocedure
		pSt.setLong(1, polySubIn.getSpeciesId());
		pSt.setString(2, polySubIn.getAlleles());
		pSt.setString(3, leftContext);
		pSt.setString(4, rightContext);
		pSt.setString(5, "Active");
		pSt.setString(6, polySubIn.getDnaType());
		pSt.registerOutParameter(7,Types.BIGINT);


		boolean hadResults = pSt.execute();

		long polymorphismId = pSt.getLong(7);

		// close the polymorphism preparedstatement
		pSt.close();

		long t2 = (new GregorianCalendar()).getTimeInMillis();
		System.out.println("Procedure executed in " + (t2-t1) + " ms\t" + templatep);
		System.out.println("Values: " + polySubIn.getSpeciesId() + ',' + polySubIn.getAlleles()
			+ ',' + leftContext.length() + "ch," + rightContext.length() + "ch,Active," + polySubIn.getDnaType());

		return polymorphismId;
	}


	public Feature persistFeature(Feature featureIn) throws SQLException
	{
		long t1 = (new GregorianCalendar()).getTimeInMillis();
	    String templateIn = "{call cgemm.create_feature(?, ?, ?, ?, ?, ?, ?, ?)}";
	    //System.out.println(templateIn);
	    //PreparedStatement tSt = conn.prepareStatement(templatep);
	    CallableStatement fSt = (CallableStatement) getConnection().prepareCall(templateIn);
	    fSt.setLong(1, featureIn.getObjectId());
	    fSt.setString(2, featureIn.getFeatureType());
	    fSt.setLong(3, featureIn.getReferenceId());
	    fSt.setString(4, featureIn.getReferenceType());
	    fSt.setInt(5, featureIn.getRefStartPos());
	    fSt.setInt(6, featureIn.getRefEndPos());
	    fSt.setString(7, featureIn.getOrientation());
	    fSt.registerOutParameter(8,Types.BIGINT);

	    boolean hadResults = fSt.execute();
	    long featureIdOut = fSt.getLong(8);

	    featureIn.setFeatureId(featureIdOut);
	    //System.out.println("Feature ID:" + featureIdOut);
	    fSt.close();

		long t2 = (new GregorianCalendar()).getTimeInMillis();
		System.out.println("Procedure executed in " + (t2-t1) + " ms\t" + templateIn);

	    return featureIn;
	} // End persistFeature(...)

	public Feature checkFeature(Feature featureIn) throws SQLException
	{
		long t1 = (new GregorianCalendar()).getTimeInMillis();
		long featureId = -1;
		long objectId = -1;

		Statement st = getConnection().createStatement();
		String query = "select feature_id, object_id from feature "
			+ " where feature_type = '" + featureIn.getFeatureType() + "'"
//			+ " and reference_type = '" + featureIn.getReferenceType() + "'"
			+ " and reference_id = " + featureIn.getReferenceId()
			+ " and ref_start_pos = " + featureIn.getRefStartPos()
			+ " and ref_end_pos = " + featureIn.getRefEndPos();
		//System.out.println(query);
		ResultSet result = st.executeQuery(query);

		if (result.next())
		{
			featureId = result.getLong("feature_id");
			objectId = result.getLong("object_id");
			featureIn.setFeatureId(featureId);
			featureIn.setObjectId(objectId);
		}
		result.close();
		st.close();

		long t2 = (new GregorianCalendar()).getTimeInMillis();
		System.out.println("Query done in " + (t2-t1) + " ms\t" + query);

	    return featureIn;
	} // End persistFeature(...)


	protected long persistPolymorphismSubmission(PolymorphismSubmission polySubmission, String sessionId)
	{
        long polymorphismSubmissionIdOut = -1;
        CallableStatement tSt = null;
        try
		{
            String storedProcedure = "{call cgemm.create_polymorphism_submission(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)}";
            tSt = (CallableStatement) getConnection().prepareCall(storedProcedure);
            String leftContext = polySubmission.getLeftContext();
            String rightContext = polySubmission.getRightContext();

            if (leftContext.length() > MAX_CONTEXT_LENGTH)
            {
            	System.out.println("Cut left context from " + leftContext.length() + " to " + MAX_CONTEXT_LENGTH);
            	leftContext = leftContext.substring(leftContext.length() - MAX_CONTEXT_LENGTH );
            }
            if (rightContext.length() > MAX_CONTEXT_LENGTH)
            {
            	System.out.println("Cut right context from " + rightContext.length() + " to " + MAX_CONTEXT_LENGTH);
            	rightContext = rightContext.substring(0, MAX_CONTEXT_LENGTH);
            }

            // persistpolymorphismSubmission matching what's in the stored procedure
            // get_Default_polymorphismSubmission_Name from the DB
            tSt.setLong(1, polySubmission.getSpeciesId());
            tSt.setString(2, sessionId);
            tSt.setString(3, polySubmission.getAlleles());
            tSt.setString(4, leftContext);
            tSt.setString(5, rightContext);
            tSt.setString(6, polySubmission.getDnaType());
            tSt.setString(7, polySubmission.getAccession());
            tSt.setString(8, polySubmission.getVersion());
            tSt.setString(9, polySubmission.getSource());
            tSt.registerOutParameter(10, Types.BIGINT);

            System.out.println(storedProcedure);
            System.out.println("Values: " + polySubmission.getSpeciesId() + ',' + sessionId	+ ',' + polySubmission.getAlleles()
            	+ ',' + leftContext.length() + "ch," + rightContext.length() + ',' + polySubmission.getDnaType()
            	+ ',' + polySubmission.getAccession() + ',' + polySubmission.getVersion() + ',' + polySubmission.getSource());

            boolean hadResults = tSt.execute();
            polymorphismSubmissionIdOut = tSt.getLong(10);
            // set polymorphismSubmissionId
            polySubmission.setPolymorphismSubmissionId(polymorphismSubmissionIdOut);
            //	cal = Calendar.getInstance(TimeZone.getDefault());
            System.out.println("Stored PolymorphismSubmission, ID=" + polymorphismSubmissionIdOut);
            //	tSt.close();
        }
        catch(SQLException sqlException)
		{
        	System.err.print("Failed insert submission accessionId=" + polySubmission.getAccession() + '\t');
        	if (sqlException.getMessage().indexOf( "value too large for column") > 0)
			{
        		System.err.println(sqlException.getMessage());
        	}
			else
			{
				sqlException.printStackTrace();
			}
        }
        finally
		{
            try {
                tSt.close();
                //  rs.close();
            } catch (SQLException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }

        return polymorphismSubmissionIdOut;
	}
*/
    public long getNewIdFromDB() throws SQLException
    {
        long id = -1;

        Statement st = getConnection().createStatement();
        String query = "SELECT get_seq_nextval from dual";
        //System.out.println(query);
        ResultSet result = st.executeQuery(query);

        if (result.next())
        {
            id = result.getLong(1);
        }
        result.close();
        st.close();

        return id;
    }

    public static boolean alterTable(Connection connIn, String tabNameIn, String loggingIn) throws SQLException
    {
        String query = "ALTER TABLE " + tabNameIn + " " +  loggingIn;
	System.out.println(query);
        Statement alterStmt;
        try {
                alterStmt = connIn.createStatement();
                alterStmt.executeUpdate(query);
                alterStmt.close();
                return true;
        }
        catch (SQLException e) {
                e.printStackTrace();
                return false;
        }
    }

    public void alterTable(String tabNameIn, String loggingIn) throws SQLException
    {
    	alterTable(getConnection(), tabNameIn, loggingIn);
    }

    /**
     * @return Returns the commit.
     */
    public boolean isCommit()
    {
        return commit;
    }
    /**
     * @param commitIn The commit to set.
     */
    public void setCommit(boolean commitIn)
    {
        commit = commitIn;
    }

    /**
     * @return Returns the dbName.
     */
    public String getDbName()
    {
        return dbName;
    }
    /**
     * @param dbNameIn The dbName to set.
     */
    public void setDbName(String dbNameIn)
    {
        dbName = dbNameIn;
    }
    /**
     * @return Returns the password.
     */

    public String getPassword()
    {
        return password;
    }
    /**
     * @param passwordIn The password to set.
     */
    public void setPassword(String passwordIn)
    {
        password = passwordIn;
    }

    /**
     * @return Returns the username.
     */
    public String getUsername()
    {
        return username;
    }
    /**
     * @param usernameIn The username to set.
     */
    public void setUsername(String usernameIn)
    {
        username = usernameIn;
    }
    /**
     * @return Returns the connection.
     */
    public Connection getConnection()
    {
        return connection;
    }
    /**
     * @param connectionIn The connection to set.
     */
    public void setConnection(Connection connectionIn)
    {
        connection = connectionIn;
    }

    /**
     * @return the keyParamMap
     */
    public Map<String, String> getKeyParamMap()
    {
        return keyParamMap;
    }

    /**
     * @param mapIn the keyParamMap to set
     */
    public void setKeyParamMap(Map<String, String> mapIn)
    {
        keyParamMap = mapIn;
    }

	public static SqlTypes getDbType() {
		return dbType;
	}

	public static void setDbType(SqlTypes dbType) {
		AbstractProcessor.dbType = dbType;
	}
}
