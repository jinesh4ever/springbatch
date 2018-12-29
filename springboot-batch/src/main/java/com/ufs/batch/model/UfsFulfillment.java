package com.ufs.batch.model;

import java.sql.Timestamp;

public class UfsFulfillment {
	 private String vendor;
	 private boolean status;
	 private String vendor_category;
	 private Timestamp processed_time;
	
	 public String getVendor() {
		return vendor;
	}
	public void setVendor(String vendor) {
		this.vendor = vendor;
	}
	public boolean getStatus() {
		return status;
	}
	public void setStatus(boolean status) {
		this.status = status;
	}
	public String getVendor_category() {
		return vendor_category;
	}
	public void setVendor_category(String vendor_category) {
		this.vendor_category = vendor_category;
	}
	public Timestamp getProcessed_time() {
		return processed_time;
	}
	public void setProcessed_time(Timestamp processed_time) {
		this.processed_time = processed_time;
	}
	 
	}