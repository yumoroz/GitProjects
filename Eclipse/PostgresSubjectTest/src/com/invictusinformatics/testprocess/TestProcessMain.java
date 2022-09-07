package com.invictusinformatics.testprocess;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

import com.invictusinformatics.dataobjects.Animal;
import com.invictusinformatics.dataobjects.Ethnicity;
import com.invictusinformatics.dataobjects.Parent;
import com.invictusinformatics.util.AbstractProcessor;

public class TestProcessMain extends AbstractProcessor
{

	public TestProcessMain() 
	{
		// TODO Auto-generated constructor stub
	}

	public static void main(String[] args) 
	{
		Connection  conn = createDBConnection("postgres", "admin", "localpostgres");
		
		String userName = "USYAK";  // "ISGC"; //"USMARC";//  "USYAK";  // source rather than userName? or both?
		try 
		{
			HashMap<Long, Animal> animalsMap = loadAnimals(conn, userName);
			Long[] subjectIds = animalsMap.keySet().toArray(new Long[0]);
			for (int i = 0; i < subjectIds.length; i++)
			{
				Animal animal = animalsMap.get(subjectIds[i]);
				if (animal.getSampleIds().size() > 1)
				{
					System.out.println("Subject " + animal.getSubjectId() + " has " + animal.getSampleIds().size() + " samples");
				}
				
				fillAnimalData(conn, animal);
			}
			
			StringBuffer animalsJsonBuf = new StringBuffer();
			animalsJsonBuf.append("{\"animals\": [");
			
			for (int i = 0; i < 3 /*subjectIds.length*/; i++)
			{
				Animal animal = animalsMap.get(subjectIds[i]);
				StringBuffer animalSB = getAnimalJsonString(animal);
				if (i > 0)
				{
					animalsJsonBuf.append(",\n");
				}
				animalsJsonBuf.append(animalSB);
				//System.out.println(animalStr);
			}
			animalsJsonBuf.append("]}");
			System.out.println(animalsJsonBuf.toString());
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}

	public static StringBuffer getAnimalJsonString(Animal animal) 
	{
		StringBuffer buf = new StringBuffer();
		buf.append("{\"animalName\": \"");
		buf.append(animal.getSubjectName());
		buf.append("\", \"species\": \""); // ,\n or \t or space? 
		buf.append(animal.getSpeciesName());
		buf.append('"');
		
		if (animal.getBreedAssociations() != null && !animal.getBreedAssociations().isEmpty())
		{
			buf.append(", \"associations\": ["); // ,\n
			for (int i = 0; i < animal.getBreedAssociations().size(); i++)
			{
				if (i > 0)
				{
					buf.append(", ");
				}
				buf.append('"');
				buf.append(animal.getBreedAssociations().get(i));
				buf.append('"');
			}
			buf.append(']');
		}
		
		if (animal.getOwners() != null && !animal.getOwners().isEmpty())
		{
			buf.append(", \"owners\": ["); // ,\n
			for (int i = 0; i < animal.getOwners().size(); i++)
			{
				if (i > 0)
				{
					buf.append(", ");
				}
				buf.append('"');
				buf.append(animal.getOwners().get(i));
				buf.append('"');
			}
			buf.append(']');
		}
		
		if (animal.getSire() != null)
		{
			buf.append(", \"sire\": \"");
			buf.append(animal.getSire().getSubjectName());
			buf.append('"');
			buf.append(", \"sireConf\": ");
			buf.append(animal.getSire().isVerified());
			//buf.append('"');
		}

		
		if (animal.getDam() != null)
		{
			buf.append(", \"dam\": \"");
			buf.append(animal.getDam().getSubjectName());
			buf.append('"');
			buf.append(", \"damConf\": ");
			buf.append(animal.getDam().isVerified());
			//buf.append('"');
		}
		
		if (animal.getProgeny() != null && !animal.getProgeny().isEmpty())
		{
			buf.append(", \"progeny\": ["); // ,\n
			for (int i = 0; i < animal.getProgeny().size(); i++)
			{
				if (i > 0)
				{
					buf.append(", ");
				}
				buf.append('"');
				buf.append(animal.getProgeny().get(i).getSubjectName());
				buf.append('"');
			}
			buf.append(']');
			
			buf.append(", \"progenyConf\": ["); // ,\n
			for (int i = 0; i < animal.getProgeny().size(); i++)
			{
				if (i > 0)
				{
					buf.append(", ");
				}
				//buf.append('"');
				buf.append(animal.getProgeny().get(i).isVerified());
				//buf.append('"');
			}
			buf.append(']');
		}
		
		if (animal.getTests() != null && !animal.getTests().isEmpty())
		{
			buf.append(", \"tests\": ["); // ,\n
			for (int i = 0; i < animal.getTests().size(); i++)
			{
				if (i > 0)
				{
					buf.append(", ");
				}
				buf.append('"');
				buf.append(animal.getTests().get(i));
				buf.append('"');
			}
			buf.append(']');
		}
		
		buf.append("}");
		
		return buf;
	}

	private static void fillAnimalData(Connection conn, Animal animal) throws SQLException 
	{
		/*Animal animal2 = */ 
		loadAnimalFamily(conn, animal);
		loadAnimalBreeds(conn, animal);
		loadAnimalOwners(conn, animal);
		
	}

	private static void loadAnimalOwners(Connection conn, Animal animal) 
	{
		//FIXME mock data
		animal.addOwner("Yuri");
		animal.addOwner("Ted");
		animal.addBreedAssociation("Invictus");
	}

	private static Animal loadAnimalBreeds(Connection conn, Animal animal) throws SQLException 
	{
		
		String query = "select \"SUB_ETHNICITY_ID\",  \"ETHNICITY\", \"FRACTION\" \n" 
        		+ "from cgemm.sub_ethnicity_tab \n" 
        		+ "where \"SUBJECT_ID\" = ? ";
           
        PreparedStatement pStmt = conn.prepareCall(query);
        ResultSet result = null;
                    
        pStmt.setLong(1, animal.getSubjectId());
            
        result = pStmt.executeQuery();
        
        while (result.next())
        {
        	long subEthnicityId = result.getLong("SUB_ETHNICITY_ID"); // not used currently
        	String ethnicity = result.getString("ETHNICITY");
        	double fraction = result.getDouble("FRACTION");

        	Ethnicity breed = new Ethnicity(ethnicity, fraction);
        	animal.addEthnicity(breed);
        }
            
        result.close();
        pStmt.close();
        
        
	/*	
		if (animal.getEthnicity() != null && animal.getEthnicity().size() > 0)
		{
			System.out.print(animal.getSubjectName());
			for (int i = 0; i < animal.getEthnicity().size(); i++)
			{
				System.out.print("   \'" + animal.getEthnicity().get(i).getEthnicity() + "\'=" + animal.getEthnicity().get(i).getFraction());
			}
			System.out.println();
		}
*/
        
        return animal; // not used

	}

	public static Animal loadAnimalFamily(Connection conn, Animal animal) throws SQLException 
	{
/*
select pv."SUBJECT_ID", "PARENT_SUBJECT_ID", s."SUBJECT_ID",  s."SUBJECT_NAME", s."GENDER" , "PARENT_SEX" , "VERIFIED" , "PARENT_VERIFICATION_ID"
from cgemm.parent_verification_tab pv , cgemm.subject_tab s
where s."SUBJECT_ID" = pv."PARENT_SUBJECT_ID"
 and (pv."SUBJECT_ID" = 7919143106 or pv."PARENT_SUBJECT_ID" = 7919143106) 
order by pv."SUBJECT_ID"; 		
 */
		
		String query = "select pv.\"SUBJECT_ID\", \"PARENT_SUBJECT_ID\",  s.\"SUBJECT_NAME\", \"PARENT_VERIFICATION_ID\", s.\"GENDER\", \"PARENT_SEX\" , \"VERIFIED\"\n" 
        		+ "from cgemm.parent_verification_tab pv, cgemm.subject_tab s \n" 
        		+ "where s.\"SUBJECT_ID\" = pv.\"PARENT_SUBJECT_ID\" and pv.\"SUBJECT_ID\" = ?"
        		+ " or (s.\"SUBJECT_ID\" = pv.\"SUBJECT_ID\" and \"PARENT_SUBJECT_ID\" = ?) \n" 
        		+ "order by pv.\"SUBJECT_ID\" ";
           
        PreparedStatement pStmt = conn.prepareCall(query);
        ResultSet result = null;
        
        //for(int i=0;i<polyConfirmationIds.length;i++){
            
        pStmt.setLong(1, animal.getSubjectId());
        pStmt.setLong(2, animal.getSubjectId());
            
        result = pStmt.executeQuery();
        
        while (result.next())
        {
        	long subjectId = result.getLong("SUBJECT_ID");
        	long parentSubjectId = result.getLong("PARENT_SUBJECT_ID");
        	long parentVerId = result.getLong("PARENT_VERIFICATION_ID");
        	String subjectName = result.getString("SUBJECT_NAME");
        	String parentSex = result.getString("PARENT_SEX");
        	String gender = result.getString("GENDER");
        	String verifyStr = result.getString("VERIFIED");
        	boolean verified = false;
        	if (verifyStr != null && !verifyStr.isEmpty())
        	{
        		if (verifyStr.charAt(0) == 'T' || verifyStr.charAt(0) == 'Y')
				{
        			verified = true;
				}
        	}
        	
        	if (subjectId != animal.getSubjectId()) // progeny
        	{
        		
        		//Parent (long subId, long parentVerifId, long parentSubId, String sex, boolean ver)
        		Parent child = new Parent(subjectId, parentVerId, parentSubjectId, gender, verified);
        		child.setSubjectName(subjectName);
        		animal.addProgeny(child);
        	}
        	else //parent
        	{
        		Parent parent = new Parent(parentSubjectId, parentVerId, -1, gender, verified);
        		parent.setSubjectName(subjectName);
        		if (parentSex.startsWith("M"))
        		{
        			animal.setSire(parent);
        		}
        		if (parentSex.startsWith("F"))
        		{
        			animal.setDam(parent);
        		}
        	}
        	
        }
            
        result.close();
        pStmt.close();
        
        /*
		System.out.print(animal.getSubjectName());
		if (animal.getSire() != null)
		{
			System.out.print(" Sire=" + animal.getSire().getSubjectName());
		}
		if (animal.getDam() != null)
		{
			System.out.print(" Dam=" + animal.getDam().getSubjectName());
		}
		if (animal.getProgeny() != null)
		{
			for (int i = 0; i < animal.getProgeny().size(); i++)
			{
				System.out.print("   \'" + animal.getProgeny().get(i).getSubjectName() + "\'");
			}
		}
		System.out.println();*/
        
        return animal; // not used
	}

	private static HashMap<Long, Animal> loadAnimals(Connection conn, String userName) throws SQLException 
	{
    	//ArrayList<Animal> animals = new ArrayList<Animal>();
    	//ArrayList<String> subjectNames = new ArrayList<String>();
    	HashMap<Long, Animal> animalsMap = new HashMap<Long, Animal>();
    	
    	/*
select s."SUBJECT_ID", s."SUBJECT_NAME", s."SPECIES_ID" , sp."SPECIES_NAME", s."GENDER"
 , sm."SAMPLE_ID", sm."SAMPLE_NAME"   
from 
 cgemm.subject_tab s 
 , cgemm.species_tab sp
 , cgemm.sample_tab sm
 where 
  s."SPECIES_ID" = sp."SPECIES_ID" 
  and sm."SUBJECT_ID" = s."SUBJECT_ID" 
  and s."SOURCE"  = 'USYAK'    	 
    	 */
    	
            
        String query = "select s.\"SUBJECT_ID\", s.\"SUBJECT_NAME\", s.\"SPECIES_ID\", sp.\"SPECIES_NAME\", s.\"GENDER\", s.\"STATUS\", sm.\"SAMPLE_ID\", sm.\"SAMPLE_NAME\" \n" 
        		+ "from cgemm.subject_tab s, cgemm.species_tab sp, cgemm.sample_tab sm \n" 
        		+ "where s.\"SPECIES_ID\" = sp.\"SPECIES_ID\" "
        		+ " and sm.\"SUBJECT_ID\" = s.\"SUBJECT_ID\" "
        		+ " and s.\"SOURCE\" = ? \n" 
        		+ "order by s.\"SUBJECT_ID\", s.\"SUBJECT_NAME\"";
           
        PreparedStatement pStmt = conn.prepareCall(query);
        ResultSet result = null;
        
        //for(int i=0;i<polyConfirmationIds.length;i++){
            
        pStmt.setString(1, userName);
            
        result = pStmt.executeQuery();
        
    	System.out.println("SubjectId \tSpeciesId \tSubjectName");
        while (result.next())
        {
        	long subjectId = result.getLong("SUBJECT_ID");
        	String subjectName = result.getString("SUBJECT_NAME");
        	long speciesId = result.getLong("SPECIES_ID");
        	String speciesName = result.getString("SPECIES_NAME");
        	String gender = result.getString("GENDER");
        	String status = result.getString("STATUS"); //maybe only Active?
        	long sampleId = result.getLong("SAMPLE_ID");
        	String sampleName = result.getString("SAMPLE_NAME");
        	Animal animal = animalsMap.get(subjectId);
        	if (animal == null)
        	{
        		animal = new Animal(subjectId, subjectName, speciesId, speciesName, userName, gender, status);
        		animal.addSampleId(sampleId);
        		animalsMap.put(subjectId, animal);
        	}
        	else
        	{
        		animal.addSampleId(sampleId);
        		System.err.println("SubjectID = " + subjectId + ", sampleId = " + sampleId + " already in the set with sampleId = " + animal.getSampleIds().get(0));
        	}
        	//subjectIds.add(subjectId);
        	//subjectNames.add(subjectName);
        	//System.out.println(subjectId + "\t" + speciesId + "\t" + subjectName);
        }
            
        result.close();
        pStmt.close();
        
		return animalsMap;
	}

}
