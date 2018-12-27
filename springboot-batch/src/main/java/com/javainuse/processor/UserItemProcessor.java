package com.javainuse.processor;

import org.springframework.batch.item.ItemProcessor;

import com.javainuse.model.User;


public class UserItemProcessor implements ItemProcessor<User, User> {

 @Override
 public User process(User user) throws Exception {
 System.out.print("user-----------------------"+user.toString());
 return user;
 }

}