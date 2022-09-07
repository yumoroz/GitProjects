package com.invictusinformatics.dataobjects;

import java.util.ArrayList;
import java.util.Date;

public class Subject
{
    private long subjectId;
    private String subjectName;
    private long speciesId;
    private String speciesName;
    private String source;
    private String gender;
    private ArrayList<Ethnicity> ethnicity = new ArrayList<Ethnicity>();
    private String status;
    private ArrayList<Long> sampleIds = new ArrayList<Long>(); //new long[0];
    private Date dateCreated;
    private long createdBy;
    private Date dateUpdated;
    private long updatedBy;

    public Subject() {
    	setSubjectId(-1); 
    }
    
    public Subject(long subId) 
    {
    	setSubjectId(subId); 
    }
    
    public Subject(long subId, String subName, long spId, String spName, String src, String gen, String stat) 
    {
    	subjectId = subId; 
    	subjectName = subName;
    	speciesId = spId;
    	speciesName = spName;
    	source = src;
    	gender = gen;
    	status = stat;
    }
    

    public long getSubjectId() {
        return subjectId;
    }

    public void setSubjectId(long subjectIdIn) {
        this.subjectId = subjectIdIn;
    }

    public String getSubjectName() {
        return subjectName;
    }

    public void setSubjectName(String subjectNameIn) {
        this.subjectName = subjectNameIn;
    }
    
    public long getSpeciesId() {
        return speciesId;
    }

    public void setSpeciesId(long speciesIdIn) {
        this.speciesId = speciesIdIn;
    }
    
    public String getSpeciesName() {
        return speciesName;
    }

    public void setSpeciesName(String speciesNameIn) {
        this.speciesName = speciesNameIn;
    }
    
    
    public void setSampleIds(ArrayList<Long> sampleIdsIn)
    {
        sampleIds = sampleIdsIn;
    }
    public ArrayList<Long> getSampleIds()
    {
        return sampleIds;
    }
    public void addSampleId(long samnpleIdIn)
    {
    	sampleIds.add(samnpleIdIn);
    }
    
    public void setEthnicity(ArrayList<Ethnicity> ethnicityIn)
    {
        ethnicity = ethnicityIn;
    }
    
    public ArrayList<Ethnicity> getEthnicity()
    {
        return ethnicity;
    }
    
    public void addEthnicity(Ethnicity ethnicityIn)
    {
    	if (ethnicity == null)
    	{
    		ethnicity = new ArrayList<Ethnicity>();
    	}
    	ethnicity.add(ethnicityIn);
    }
    
/**
 * 
 * @param subject source
 */
    public void setSource(String sourceIn) {
        this.source = sourceIn;
    }
    public String getSource() {
        return source;
    }
    /**
     * 
     * @param subject gender
     */
    public void setGender(String genderIn) {
        this.gender = genderIn;
    }
    public String getGender() {
        return gender;
    }
    
    public void setStatus(String statusIn)
    {
    		status = statusIn;
    }
    
    public String getStatus()
    {
    		return status;
    }
    
    public Date getDateCreated()
    {
    		return dateCreated;
    }
    
    public void setDateCreated(Date dateCreatedIn)
    {
    		dateCreated = dateCreatedIn;
    }
    
    public long getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(long createdByIn) {
        createdBy = createdByIn;
    }

    public Date getDateUpdated()
    {
    		return dateUpdated;
    }
    
    public void setDateUpdated(Date dateUpdatedIn)
    {
    		dateUpdated = dateUpdatedIn;
    }
    
    public long getUpdatedBy() {
        return updatedBy;
    }

    public void setUpdatedBy(long updatedByIn) {
        updatedBy = updatedByIn;
    }
    /**
     * returns a string representation of the Subject object.
     * @return A string representation of the Subject object.
     */
   public String toString() 
   {
       String retValue=null;
       retValue+="Subject ID: " + subjectId;
       retValue+="\nSpecies ID: " + this.getSpeciesId();
       retValue+="\nSubject Name: " + this.getSubjectName();
       retValue+="\nSource: "+ this.getSource();
       retValue+="\nGender: " + this.getGender();
       retValue+="\nStatus: " + this.getStatus();
       return retValue;
   }
   
   
   
}
