package com.javainuse.config;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.core.io.Resource;
import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.MultiResourceItemReader;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import com.javainuse.model.User;
import com.javainuse.processor.UserItemProcessor;

@Configuration
@EnableBatchProcessing
public class BatchConfig {

 @Autowired
 public JobBuilderFactory jobBuilderFactory;
 
 @Autowired
 public StepBuilderFactory stepBuilderFactory;
 
 @Autowired
 public DataSource dataSource;
 
 @Value("input/inputData.csv")
 private Resource[] inputResources;
 
 private Resource outputResource = new FileSystemResource("output/outputData.csv");
 
 @Bean
 public DataSource dataSource() {
  final DriverManagerDataSource dataSource = new DriverManagerDataSource();
  try{
  dataSource.setDriverClassName("org.postgresql.Driver");
  dataSource.setUrl("jdbc:postgresql://localhost:5432/springbatch");
  dataSource.setUsername("postgres");
  dataSource.setPassword("openpgpwd");
  }catch(Exception e) {
	  e.printStackTrace();
	  System.out.println(e.getMessage());
  }
  
  return dataSource;
 }
 
 @Bean
 public MultiResourceItemReader<User> multiResourceItemReader()
 {
     MultiResourceItemReader<User> resourceItemReader = new MultiResourceItemReader<User>();
     resourceItemReader.setResources(inputResources);
     resourceItemReader.setDelegate(fileReader());
     return resourceItemReader;
 }
 
 @SuppressWarnings({ "rawtypes", "unchecked" })
 @Bean
 public FlatFileItemReader<User> fileReader()
 {
     //Create reader instance
     FlatFileItemReader<User> reader = new FlatFileItemReader<User>();
      
     //Set number of lines to skips. Use it if file has header rows.
     reader.setLinesToSkip(1);  
      
     //Configure how each line will be parsed and mapped to different values
     reader.setLineMapper(new DefaultLineMapper() {
         {
             //3 columns in each row
             setLineTokenizer(new DelimitedLineTokenizer() {
                 {
                     setNames(new String[] { "id", "name" });
                 }
             });
             //Set values in Employee class
             setFieldSetMapper(new BeanWrapperFieldSetMapper<User>() {
                 {
                     setTargetType(User.class);
                 }
             });
         }
     });
     return reader;
 }
 
 @Bean
 public JdbcBatchItemWriter<User> databaseWriter() {
     JdbcBatchItemWriter<User> itemWriter = new JdbcBatchItemWriter<User>();
     itemWriter.setDataSource(dataSource());
     itemWriter.setSql("INSERT INTO user1 (ID, name) VALUES (:id, :name)");
     itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<User>());
     return itemWriter;
 }

 
 @Bean
 public JdbcCursorItemReader<User> reader(){
  JdbcCursorItemReader<User> reader = new JdbcCursorItemReader<User>();
  reader.setDataSource(dataSource);
  reader.setSql("SELECT id,name FROM user1");
  reader.setRowMapper(new UserRowMapper());
  
  return reader;
 }
 
 public class UserRowMapper implements RowMapper<User>{

  @Override
  public User mapRow(ResultSet rs, int rowNum) throws SQLException {
   User user = new User();
   user.setId(rs.getInt("id"));
   user.setName(rs.getString("name"));
   
   return user;
  }
  
 }
 
 @Bean
 public UserItemProcessor processor(){
  return new UserItemProcessor();
 }
 
 @Bean
 public FlatFileItemWriter<User> writer(){
  FlatFileItemWriter<User> writer = new FlatFileItemWriter<User>();
  writer.setResource(outputResource);
  writer.setAppendAllowed(true);
  writer.setLineAggregator(new DelimitedLineAggregator<User>() {{
   setDelimiter(",");
   setFieldExtractor(new BeanWrapperFieldExtractor<User>() {{
    setNames(new String[] { "id", "name" });
   }});
  }});
  
  return writer;
 }
 
 
 
 @Bean
 public Step step1() {
  return stepBuilderFactory.get("step1").<User, User> chunk(10)
    .reader(reader())
    .processor(processor())
    .writer(writer())
    .build();
 }
 
 @Bean
 public Step step2() {
     return stepBuilderFactory
             .get("step2")
             .<User, User>chunk(5)
             .reader(multiResourceItemReader())
             .processor(processor())
             .writer(databaseWriter())
             .build();
 }
 
 @Bean
 public Job exportUserJob() {
  return jobBuilderFactory.get("exportUserJob")
    .incrementer(new RunIdIncrementer())
    .flow(step2())
    .end()
    .build();
 }
 
}