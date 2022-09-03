/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.invictusinformatics.PostgresSubjectTest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.StringTokenizer;
import java.util.ArrayList;

/**
 *
 * @author tedkalbfleisch
 */
public class PostgresSubjectTest {

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) 
    {

    	/*
    	 * import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
    	 */
    
    	
        try
        {
            
  /*
            BufferedReader bufferedReader = new BufferedReader(new FileReader(new File("/Users/tedkalbfleisch/eclipse-workspace/YakDataProcessingPostGres/USYAK/test_result_756365_YakParentagePanel.csv")));
                                                                                        
            String geneSeekRunName = "756365";
            long yakGeneticsUserGroupId = 7919139036L; //USYAK
            //long yakGeneticsUserGroupId = 7918700600L; //IYAK
            String line = null;
            String[] genotypeData = null;
            
            String[] labels = bufferedReader.readLine().split(",");
            String barcodeLabel = "";
            for(int i=1;i<labels[0].length();i++){
                barcodeLabel = barcodeLabel + labels[0].charAt(i);
            }
            labels[0] = barcodeLabel;
            HashMap experimentalSamples = new HashMap();
            //HashMap subjectNamesHash = new HashMap();
            HashMap genotypes = null;
            String sampleId = null;
            String experimentalSampleName = null;
            
            while((line=bufferedReader.readLine())!=null){
                
                genotypeData = line.split(",");
                
                if(genotypeData.length<100){
                    continue;
                }
                
                genotypes = new HashMap();
                
                for(int i=0;i<genotypeData.length;i++){
                    if(labels[i].trim().equals("BarCode")){
                        experimentalSampleName = genotypeData[i].trim();
                        System.out.println(experimentalSampleName);
                    }
                    if(labels[i].equals("Sample ID")){
                        sampleId = genotypeData[i].trim();
                    }
                    genotypes.put(labels[i],genotypeData[i]);
                }
                
                experimentalSamples.put(experimentalSampleName, genotypes);
                //subjectNamesHash.put(sampleId, subjectName);
                
                //System.out.println(genotypeData.length);
            }
            
            bufferedReader.close();
           
 */           
            
            //String password = getPasswd("kalbflei");
            Connection  conn = createConnection("kalbflei", "");

/*
            long yakParentageTestId = 7918699980L;
            long dataSetRecordId = createDataSetRecord(conn,geneSeekRunName,yakParentageTestId);            
            
            CallableStatement requestAccess = getRequestAccessStmt(conn);
            CallableStatement grantAccess = getGrantAccessStmt(conn);
            CallableStatement createExperimentStmt = getCreateExperimentStmt(conn,yakParentageTestId, dataSetRecordId);
            CallableStatement createGenotypeStmt = getCreateGenotypeStmt(conn);
            CallableStatement createGenotypeConfirmationStmt = getCreateGenotypeConfirmationStmt(conn);
            
            PreparedStatement procReqStmt = getProcRequestQuery(conn);
            PreparedStatement getGenotypeConfirmationIdStmt = getGenotypeConfirmationId(conn);

            System.out.println(dataSetRecordId);
            
            int success = setDataSetAccess(requestAccess,procReqStmt,grantAccess,"DATA_SET_RECORD",dataSetRecordId,yakGeneticsUserGroupId);

            long[] polyConfirmationIds = getPolyConfirmationIds(conn,yakParentageTestId,labels);
            long[] assayIds            = getAssayIds(conn,yakParentageTestId,polyConfirmationIds);
            String[] alleles           = getAlleles(conn,polyConfirmationIds);
            
            
            //String[] sampleNames = getSampleNames(subjectNamesHash);
            //String[] subjectNames = getSubjectNames(sampleNames,subjectNamesHash);
            String[] experimentalSampleNames = getExperimentalSampleNames(experimentalSamples);            
            long[] experimentalSampleIds = getExperimentalSampleIds(conn,experimentalSampleNames);
            long experimentId = -1L;
            long genotypeConfirmationId = -1L;
            String genotype = null;
            
            for(int i=0;i<experimentalSampleNames.length;i++){
                
                try{
                    experimentId = createExperiment(createExperimentStmt,experimentalSampleIds[i]);
                }catch(SQLException sqlException){
                    System.err.println(experimentalSampleName);
                    sqlException.printStackTrace();
                    System.exit(1);
                }
                success = setDataSetAccess(requestAccess,procReqStmt,grantAccess,"EXPERIMENT",experimentId,yakGeneticsUserGroupId);
                System.out.println(success);
                
                genotypes = (HashMap)experimentalSamples.get(experimentalSampleNames[i]);
                for(int j=0;j<labels.length;j++){
                    
                    try{
                        if(polyConfirmationIds[j]==-1L || genotypes.get(labels[j])==null){
                            continue;
                        }
                    }catch(NullPointerException nullPointerException){
                        System.err.println(labels[j]);
                        //System.err.println(labels[j]);
                        System.exit(1);
                    }
                    
                    genotype = getGenotype((String)genotypes.get(labels[j]),alleles[j]);
                    
                    if(genotype==null)
                        continue;
                    
                    genotypeConfirmationId = createGenotypeConfirmation(createGenotypeConfirmationStmt,
                                                createGenotypeStmt,
                                                getGenotypeConfirmationIdStmt,
                                                polyConfirmationIds[j],
                                                experimentalSampleIds[i],
                                                assayIds[j],
                                                experimentId,
                                                genotype);

                    success = setDataSetAccess(requestAccess,procReqStmt,grantAccess,"GENO_CONFIRMATION",genotypeConfirmationId,yakGeneticsUserGroupId);
                    System.out.println(success);                    
                    
                    System.out.println(genotypeConfirmationId);
                }
                
            }
            //commit          
            
            conn.rollback();
            //conn.commit();
            
            requestAccess.close();
            grantAccess.close();
            createExperimentStmt.close();
            createGenotypeStmt.close();
            createGenotypeConfirmationStmt.close();
            
            procReqStmt.close();
            getGenotypeConfirmationIdStmt.close();       
            */
            
            ArrayList<String> subjectNames = readSubjects(conn);
            
            
            conn.close();
        }
    	/*catch(IOException ioException){
            ioException.printStackTrace();
        }*/
    	catch(SQLException sqlException){
            sqlException.printStackTrace();
        }
    
    }
    
    private static ArrayList<String> readSubjects(Connection conn) throws SQLException 
    {
		// TODO Auto-generated method stub
		/*
"SUBJECT_ID", "SPECIES_ID", "SUBJECT_NAME", "SOURCE", "GENDER", "STATUS"
		 */
    	
    	
    	ArrayList<Long> subjectIds = new ArrayList<Long>();
    	ArrayList<String> subjectNames = new ArrayList<String>();
            
        String query = "select \"SUBJECT_ID\", \"SPECIES_ID\", \"SUBJECT_NAME\", \"SOURCE\", \"GENDER\", \"STATUS\" \n" +
                            "from cgemm.subject_tab \n" +
                            "where \"SOURCE\" = ?" ;
 //University of Idaho            
        PreparedStatement pStmt = conn.prepareCall(query);
        ResultSet result = null;
        
        //for(int i=0;i<polyConfirmationIds.length;i++){
            
        pStmt.setString(1, "University of Idaho");
            
        result = pStmt.executeQuery();
        
    	System.out.println("SubjectId \tSpeciesId \tSubjectName");
        while (result.next())
        {
        	long subjectId = result.getLong(1);
        	long speciesId = result.getLong(2);
        	String subjectName = result.getString(3);
        	subjectIds.add(subjectId);
        	subjectNames.add(subjectName);
        	System.out.println(subjectId + "\t" + speciesId + "\t" + subjectName);
        }
            
        result.close();
            
       
        pStmt.close();
        
        return subjectNames;
  	
    	
	}

	private static String getPasswd(String username){
    
        String homeDir = System.getenv("HOME");

        String password = null;
        
        try {
            
            FileReader fileReader = new FileReader(new File(homeDir,".passwd"));
            BufferedReader bufferedReader = new BufferedReader(fileReader);
            StringTokenizer st = null;
            String line = null;
            
            while((line=bufferedReader.readLine())!=null){
                if(line.startsWith("#")){
                    continue;
                }
                
                st = new StringTokenizer(line);
                
                if(st.nextToken().trim().equals(username.trim())){
                    password = st.nextToken();
                    return password;
                }
            }
            
        }catch(IOException ioException){
            ioException.printStackTrace();
        }

        return null;
        
    }
    
    private static Connection createConnection(String usernameIn, String passwordIn)
    {
        
        Connection conn = null;
        try {
            /*
            Class.forName("oracle.jdbc.driver.OracleDriver");
            conn = DriverManager.getConnection
                    ("jdbc:oracle:thin:@localhost:1521:INFRMPRD",
                    //("jdbc:oracle:thin:@ip-172-31-11-15:1521:INFRMPRD",
                    usernameIn,passwordIn);
            conn.setAutoCommit(false);
            */
        	

        	Class.forName("org.postgresql.Driver");
        	/*
            conn = DriverManager.getConnection
                    ("jdbc:postgresql://database-1.czz8ejjnqstf.us-west-2.rds.amazonaws.com:5432/INFRMPRD",
                    //("jdbc:oracle:thin:@ip-172-31-11-15:1521:INFRMPRD",
                    "postgres","postgres");
                    */
        	
        	conn = DriverManager.getConnection
            		//("jdbc:postgresql://localhost:5431/tedkalbfleisch", "postgres", "postgres");
        			("jdbc:postgresql://localhost:5432/postgres", "postgres", "admin");
            conn.setAutoCommit(false);
        } catch (ClassNotFoundException classNotFoundException) {
            // TODO Auto-generated catch block
            classNotFoundException.printStackTrace();
        } catch (SQLException sqlException) {
            // TODO Auto-generated catch block
            sqlException.printStackTrace();
        }

        return conn;
        
    }
    
    
    private static CallableStatement getRequestAccessStmt(Connection conn) throws SQLException{
        
        CallableStatement cStmt;

        cStmt = conn.prepareCall("{? = call cgemm.request_user_group_access_func(?, ?, ?)}");
        
        return cStmt;
    }
    
    private static CallableStatement getGrantAccessStmt(Connection conn) throws SQLException{
        
        CallableStatement cStmt;

        cStmt = conn.prepareCall("{? = call cgemm.grant_user_group_access_func(?, ?)}");
        
        return cStmt;
    }
    
    private static CallableStatement getCreateExperimentStmt(Connection conn,long testId, long dataSetRecordId) throws SQLException{
        
        CallableStatement cStmt;

        cStmt = conn.prepareCall("{? = call cgemm.create_experiment_func(?, " + testId + "," + dataSetRecordId + ")}");
        
        return cStmt;
    }
    
    private static CallableStatement getCreateGenotypeStmt(Connection conn) throws SQLException{
        
        CallableStatement cStmt;

        cStmt = conn.prepareCall("{? = call cgemm.create_genotype_func(?, ?, ?, cgemm.get_usr_id())}");
        
        return cStmt;
    }  
    
    
    private static CallableStatement getCreateGenotypeConfirmationStmt(Connection conn) throws SQLException{
        
        CallableStatement cStmt;

        cStmt = conn.prepareCall("{? = call cgemm.create_genotype_confirmation_func(cgemm.get_seq_nextval(),?, ?, ?, ?, aws_oracle_ext.SYSDATE(), cgemm.get_usr_id())}");
        
        return cStmt;
    }    
    
    private static long createExperiment(CallableStatement createExperimentStmt,long experimentalSampleId) throws SQLException{
        
    	createExperimentStmt.registerOutParameter(1, 2);
        createExperimentStmt.setLong(2, experimentalSampleId);
        
        long experimentId = -1L;
        try {
	        createExperimentStmt.execute();
	        experimentId = createExperimentStmt.getBigDecimal(1).longValue();
        }catch(SQLException sqlException) {
        	System.err.println("ExperimentalSampleId:" + experimentalSampleId);
        	System.exit(1);
        }
        
        return experimentId;
        
    }     

    private static long createGenotypeConfirmation(CallableStatement createGenotypeConfirmationStmt,
                                                    CallableStatement createGenotypeStmt,
                                                    PreparedStatement getGenotypeConfirmationId,
                                                    long polyConfirmationId,
                                                    long experimentalSampleId,
                                                    long assayId,
                                                    long experimentId,
                                                    String alleles) throws SQLException{
        
        createGenotypeStmt.setLong(1,polyConfirmationId);
        createGenotypeStmt.setLong(2,experimentalSampleId);
        createGenotypeStmt.setString(3,alleles);
        createGenotypeStmt.registerOutParameter(4, 2);
        
        createGenotypeStmt.execute();
        
        long genotypeId = createGenotypeStmt.getBigDecimal(4).longValue();
        
        createGenotypeConfirmationStmt.registerOutParameter(1, 2);
        createGenotypeConfirmationStmt.setLong(2, genotypeId);
        createGenotypeConfirmationStmt.setLong(3, assayId);
        createGenotypeConfirmationStmt.setString(4, alleles);
        createGenotypeConfirmationStmt.setLong(5, experimentId);
        
        createGenotypeConfirmationStmt.execute();
        
        getGenotypeConfirmationId.setLong(1, genotypeId);
        getGenotypeConfirmationId.setLong(2, assayId);
        getGenotypeConfirmationId.setLong(3, experimentId);
        
        ResultSet result = getGenotypeConfirmationId.executeQuery();
        
        long genotypeConfirmationId = -1L;
        
        if(result.next()){
            genotypeConfirmationId = result.getLong(1);
        }
        result.close();
        
        return genotypeConfirmationId;
    }    
    
    private static PreparedStatement getGenotypeConfirmationId(Connection conn) throws SQLException{
        
        String query = "select \"GENO_CONFIRMATION_ID\"\n" +
                       "from cgemm.geno_confirmation_tab\n" +
                       "where \"GENOTYPE_ID\" = ?\n" +
                       "and \"ASSAY_ID\" = ?\n" +
                       "and \"EXPERIMENT_ID\" = ?";
        
        PreparedStatement pStmt = conn.prepareCall(query);
        
        return pStmt;
     }
    
    private static PreparedStatement getProcRequestQuery(Connection conn) throws SQLException{
        
        
        String query = "select \"PROCESS_REQUEST_ID\" from cgemm.process_request_tab\n" +
                       "where \"PROCESS_STEP\" = 'GrantUserGroupAccess'\n" +          
                       "and \"PROCESS_STATUS\"= 'Queued'\n" +
                       "and \"OBJECT_ID\" = ?";
        PreparedStatement pStmt = conn.prepareCall(query);
        
        return pStmt;
        
    }
    
    private static int setDataSetAccess(CallableStatement requestAccess,PreparedStatement procReqStmt,CallableStatement grantAccess,String dataType,long dataId, long userGroupId) throws SQLException{
        
    	requestAccess.registerOutParameter(1, 2);
        requestAccess.setString(2, dataType);
        requestAccess.setLong(3, dataId);
        requestAccess.setLong(4, userGroupId);
        
        
        requestAccess.execute();
        long grantUserGroupId = requestAccess.getBigDecimal(1).longValue();
        
        
        procReqStmt.setLong(1, grantUserGroupId);
        ResultSet result = procReqStmt.executeQuery();
        long processRequestId = -1L;
        if(result.next()){
            processRequestId = result.getLong(1);
        }
        result.close();
        
        grantAccess.registerOutParameter(1, Types.INTEGER);
        grantAccess.setLong(2, processRequestId);
        grantAccess.setString(3, "Accept");
        
        
        grantAccess.execute();
        
        int success = grantAccess.getInt(1);
        
        return success;
        
        
    }
    
    private static long createDataSetRecord(Connection conn,String geneSeekRunName,long testId) throws SQLException{
        
    	//CallableStatement cStmt = conn.prepareCall("{? = call cgemm.get_usr_id()}");
        //CallableStatement cStmt = conn.prepareCall("{? = call cgemm.create_data_set_record('" + geneSeekRunName + "'," + (double)testId + ", 'Yak Parentage/Introgression Test:" + geneSeekRunName + "')}");
        CallableStatement cStmt = conn.prepareCall("{? = call cgemm.create_data_set_record_func(?,?,?)}");
        
        cStmt.setString(2, geneSeekRunName);
        cStmt.setLong(3, testId);
        cStmt.setString(4, "Yak Parentage/Introgression Test:" + geneSeekRunName);
        
       
        cStmt.registerOutParameter(1, Types.DECIMAL);
        
        cStmt.execute();
        
        long dataSetRecordId = cStmt.getBigDecimal(1).longValue();
        System.out.println("DataSetRecordId=" + dataSetRecordId);
        cStmt.close();
        return dataSetRecordId;
        
    }
    
    private static long[] getPolyConfirmationIds(Connection conn, long testId, String[] markerNames) throws SQLException{
        
        long[] polyConfirmationIds = new long[markerNames.length];
        
         String query = "select \"POLY_CONFIRMATION_ID\" \n" +
                        "from cgemm.poly_confirmation_tab pc,\n" +
                        "cgemm.assay_tab a\n" +
                        "where pc.\"ACCESSION\" = a.\"ASSAY_NAME\"\n" +          
                        "and a.\"TEST_ID\" = " + testId + "\n" +
                        "and a.\"ASSAY_NAME\" = ?";
        PreparedStatement pStmt = conn.prepareCall(query);
        ResultSet result = null;
        
        for(int i=0;i<markerNames.length;i++){
            
            pStmt.setString(1, "Bg_Bt_UMD3_" + markerNames[i].trim().replace("-", ":"));
            
            result = pStmt.executeQuery();
            
            if(result.next()){
                polyConfirmationIds[i] = result.getLong(1);
            }else{
                polyConfirmationIds[i] = -1L;
                System.err.println("Could not find accession for:" + markerNames[i]);
                System.err.flush();
            }
            
            result.close();
            
        }
        pStmt.close();
        
        return polyConfirmationIds;
    }
    
private static long[] getAssayIds(Connection conn, long testId, long[] polyConfirmationIds) throws SQLException{
        
        long[] assayIds = new long[polyConfirmationIds.length];
        
         String query = "select \"ASSAY_ID\" \n" +
                        "from cgemm.poly_confirmation_tab pc,\n" +
                        "cgemm.assay_tab a\n" +
                        "where a.\"TEST_ID\" = " + testId + "\n" +
                        "and pc.\"ACCESSION\" = a.\"ASSAY_NAME\"\n" +          
                        "and pc.\"POLY_CONFIRMATION_ID\" = ?" ;
         
        PreparedStatement pStmt = conn.prepareCall(query);
        ResultSet result = null;
        
        for(int i=0;i<polyConfirmationIds.length;i++){
            
            pStmt.setLong(1, polyConfirmationIds[i]);
            
            result = pStmt.executeQuery();
            
            if(result.next()){
                assayIds[i] = result.getLong(1);
            }else{
                assayIds[i] = -1L;
                System.err.println("Could not find assayId for:" + polyConfirmationIds[i]);
                System.err.flush();
            }
            
            result.close();
            
        }
        pStmt.close();
        
        return polyConfirmationIds;
    }    
    
private static String[] getAlleles(Connection conn, long[] polyConfirmationIds) throws SQLException{
        
        String[] alleles = new String[polyConfirmationIds.length];
        
         String query = "select \"ALLELES\" \n" +
                        "from cgemm.poly_confirmation_TAB pc\n" + 
                        "where pc.\"POLY_CONFIRMATION_ID\" = ?";
         
        PreparedStatement pStmt = conn.prepareCall(query);
        ResultSet result = null;
        
        for(int i=0;i<polyConfirmationIds.length;i++){
            
            pStmt.setLong(1, polyConfirmationIds[i]);
            
            result = pStmt.executeQuery();
            
            if(result.next()){
                alleles[i] = result.getString(1);
            }else{
                alleles[i] =null;
                System.err.println("Could not find alleles for:" + polyConfirmationIds[i]);
                System.err.flush();
            }
            
            result.close();
            
        }
        pStmt.close();
        
        return alleles;
    }

    private static long[] getExperimentalSampleIds(Connection conn, String[] experimentalSampleNames) throws SQLException{
        
        long[] experimentalSampleIds = new long[experimentalSampleNames.length];
        
         String query = "select es.\"EXPERIMENTAL_SAMPLE_ID\" \n" +
                        "from cgemm.experimental_sample_tab es\n" +
                        "where es.\"EXPERIMENTAL_SAMPLE_NAME\" = ?";
         
        PreparedStatement pStmt = conn.prepareCall(query);
        ResultSet result = null;
        
        for(int i=0;i<experimentalSampleNames.length;i++){
            
            pStmt.setString(1, experimentalSampleNames[i]);
            
            
            result = pStmt.executeQuery();
            
            if(result.next()){
                experimentalSampleIds[i] = result.getLong(1);
            }else{
                experimentalSampleIds[i] = -1L;
                System.err.println("Could not find ExperimentalSampleId for:" + experimentalSampleNames[i]);
                System.err.flush();
            }
            
            result.close();
            
        }
        pStmt.close();
        
        return experimentalSampleIds;
    }
    
    private static String[] getSampleNames(HashMap subjectNames){
        
        Set set = subjectNames.keySet();
        Iterator iterator = set.iterator();
        
        String[] sampleNames = new String[subjectNames.size()];
        
        int index = 0;
        while(iterator.hasNext()){
            
            sampleNames[index] = (String)iterator.next();
            index++;
            
        }
        
        return sampleNames;
        
    }
    
    private static String[] getSubjectNames(String[] sampleNames, HashMap subjectNamesHash){
        

        
        String[] subjectNames = new String[subjectNamesHash.size()];
        
        for(int i=0;i<sampleNames.length;i++){
            
            subjectNames[i] = (String)subjectNamesHash.get(sampleNames[i]);
            
        }
        
        return subjectNames;
        
    }
    
    private static String[] getExperimentalSampleNames(HashMap experimentaSamples){
        

        
        String[] experimentalSampleNames = new String[experimentaSamples.size()];
        
        Set set = experimentaSamples.keySet();
        Iterator iterator = set.iterator();
        int index = 0;        
        
        while(iterator.hasNext()){
            
            experimentalSampleNames[index] = (String)iterator.next();
            index++;
        }
        
        return experimentalSampleNames;
        
    }
    
    private static String getGenotype(String measuredGenotype,String storedGenotype){
        
        if(measuredGenotype.trim().equals("")){
            return null;
        }
        
        measuredGenotype = measuredGenotype.toUpperCase();
        
        String[] alleles = storedGenotype.split("/");
        
        for(int i=0;i<alleles.length;i++){
            alleles[i] = alleles[i].toUpperCase();
            if(measuredGenotype.equals(alleles[i] + "/" + alleles[i])){
                return alleles[i] + "/" + alleles[i];
            }
        }
        
        for(int i=0;i<alleles.length;i++){
            for(int j=i+1;j<alleles.length;j++){
            
                if(measuredGenotype.equals(alleles[i] + "/" + alleles[j]) || measuredGenotype.equals(alleles[j] + "/" + alleles[i])){
                    return alleles[i] + "/" + alleles[j];
                }
        
            }
            
        }
        
        for(int i=0;i<alleles.length;i++){
            for(int j=0;j<alleles.length;j++){
            
                if(measuredGenotype.equals(alleles[i] + alleles[j])){
                    if(i<j)
                        return alleles[i] + "/" + alleles[j];
                    else
                        return alleles[j] + "/" + alleles[i];
                }
        
            }
            
        }        
        
        System.err.println("Problem with:" + measuredGenotype + "\t" + storedGenotype);
        System.err.flush();
        return null;
    }
    
}
