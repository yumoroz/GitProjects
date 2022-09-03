package com.invictusinformatics.dataobjects;

public class Parent extends Subject 
{
	private long parentVerificationId;
	private long parentSubjectId;
	private boolean verified;
	
	public Parent (long subId, long parentVerifId, long parentSubId, String sex, boolean ver)
	{
		super (subId);
		setGender(sex);
		parentVerificationId = parentVerifId;
		parentSubjectId = parentSubId;
		verified = ver;
	}
	
	public Parent (long subId, long parentSubId, String sex, boolean ver)
	{
		super (subId);
		setGender(sex);
		parentSubjectId = parentSubId;
		verified = ver;
	}
	
	/**
	 * @return the parentSubjectId
	 */
	public long getParentSubjectId() {
		return parentSubjectId;
	}
	/**
	 * @param parentSubjectId the parentSubjectId to set
	 */
	public void setParentSubjectId(long parentSubjectId) {
		this.parentSubjectId = parentSubjectId;
	}
	
	/**
	 * @return the verified
	 */
	public boolean isVerified() {
		return verified;
	}
	/**
	 * @param verified the verified to set
	 */
	public void setVerified(boolean verified) {
		this.verified = verified;
	}
	
	/**
	 * @return the parentVerificationId
	 */
	public long getParentVerificationId() {
		return parentVerificationId;
	}
	/**
	 * @param parentVerificationId the parentVerificationId to set
	 */
	public void setParentVerificationId(long parentVerificationId) {
		this.parentVerificationId = parentVerificationId;
	}
	
}
