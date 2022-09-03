package com.invictusinformatics.dataobjects;

public class Ethnicity 
{
	private String ethnicity = null;
    private double fraction = 0.0;
    
    public Ethnicity(String ethnicityIn, double fractionIn) 
    {
        setEthnicity(ethnicityIn);
        setFraction(fractionIn);
    }
    
    public void setEthnicity(String ethnicityIn){
        ethnicity = ethnicityIn;
    }
    
    public String getEthnicity(){
        return ethnicity;
    }
    
    public void setFraction(double fractionIn){
        fraction = fractionIn;
    }
    
    public double getFraction(){
        return fraction;
    }

}
