package com.invictusinformatics.dataobjects;

import java.util.ArrayList;

public class Animal extends Subject
{
	private ArrayList owners; //or custodians TODO create tab
	private ArrayList breedAssociations; //TODO create tab
	private Parent sire;
	private Parent dam;
	private ArrayList<Parent> progeny;
	private ArrayList<Test> tests;
	
	
	public Animal() 
	{
		super();
	}
	
	public Animal(long subId, String subName, long spId, String spName, String src, String gen, String stat) 
    {
		super(subId, subName, spId, spName, src, gen, stat);
    }

	public void addProgeny(Parent child) 
	{
		if (progeny == null)
		{
			progeny = new ArrayList<Parent>();
		}
		progeny.add(child);
		
	}

	/**
	 * @return the sire
	 */
	public Parent getSire() {
		return sire;
	}
	/**
	 * @param sire the sire to set
	 */
	public void setSire(Parent sire) {
		this.sire = sire;
	}

	/**
	 * @return the dam
	 */
	public Parent getDam() {
		return dam;
	}

	/**
	 * @param dam the dam to set
	 */
	public void setDam(Parent dam) {
		this.dam = dam;
	}

	/**
	 * @return the progeny
	 */
	public ArrayList<Parent> getProgeny() {
		return progeny;
	}

	/**
	 * @param progeny the progeny to set
	 */
	public void setProgeny(ArrayList<Parent> progeny) {
		this.progeny = progeny;
	}

}
