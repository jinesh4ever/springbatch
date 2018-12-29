package com.ufs.batch.processor;

import org.springframework.batch.item.ItemProcessor;

import com.ufs.batch.common.Common;
import com.ufs.batch.model.UfsFulfillment;


public class UserItemProcessor implements ItemProcessor<UfsFulfillment, UfsFulfillment> {

 @Override
 public UfsFulfillment process(UfsFulfillment ufsFulfillment) throws Exception {
	 ufsFulfillment.setVendor(ufsFulfillment.getVendor().toUpperCase());
	 ufsFulfillment.setProcessed_time(Common.getCurrentTimestamp());
 return ufsFulfillment;
 }

}