package com.invictusinformatics.dataobjects;

import java.util.ArrayList;

public class Animal extends Subject
{
	private ArrayList<String> owners = new ArrayList<String>(); //or custodians TODO create tab
	private ArrayList<String> breedAssociations = new ArrayList<String>(); //TODO create tab
	private Parent sire = null;
	private Parent dam = null;
	private ArrayList<Parent> progeny = new ArrayList<Parent>();
	private ArrayList<Test> tests = new ArrayList<Test>();;
	
	
	public Animal() 
	{
		super();
	}
	
	public Animal(long subId, String subName, long spId, String spName, String src, String gen, String stat) 
    {
		super(subId, subName, spId, spName, src, gen, stat);
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
	public void addProgeny(Parent child) 
	{
		if (progeny == null)
		{
			progeny = new ArrayList<Parent>();
		}
		progeny.add(child);
		
	}

	/**
	 * @return the owners
	 */
	public ArrayList<String> getOwners() {
		return owners;
	}
	/**
	 * @param owners the owners to set
	 */
	public void setOwners(ArrayList<String> owners) {
		this.owners = owners;
	}
	public void addOwner(String owner) 
	{
		if (owners == null)
		{
			owners = new ArrayList<String>();
		}
		owners.add(owner);
		
	}

	/**
	 * @return the breedAssociations
	 */
	public ArrayList<String> getBreedAssociations() {
		return breedAssociations;
	}

	/**
	 * @param breedAssociations the breedAssociations to set
	 */
	public void setBreedAssociations(ArrayList<String> breedAssociations) {
		this.breedAssociations = breedAssociations;
	}

	public void addBreedAssociation(String breedAssoc) 
	{
		if (breedAssociations == null)
		{
			breedAssociations = new ArrayList<String>();
		}
		breedAssociations.add(breedAssoc);	
	}
	/**
	 * @return the tests
	 */
	public ArrayList<Test> getTests() {
		return tests;
	}

	/**
	 * @param tests the tests to set
	 */
	public void setTests(ArrayList<Test> tests) {
		this.tests = tests;
	}
	public void addTest(Test test)
	{
		if (tests == null)
		{
			tests = new ArrayList<Test>();
		}
		tests.add(test);
	}

	
}
