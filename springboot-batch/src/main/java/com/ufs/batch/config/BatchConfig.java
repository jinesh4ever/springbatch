package com.ufs.batch.config;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.launch.support.SimpleJobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.MapJobRepositoryFactoryBean;
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
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.scheduling.annotation.Scheduled;

import com.ufs.batch.common.Common;
import com.ufs.batch.model.UfsFulfillment;
import com.ufs.batch.processor.UserItemProcessor;


@Configuration
@EnableBatchProcessing
public class BatchConfig {

	@Autowired
	private SimpleJobLauncher jobLauncher;
	
	@Autowired
	public JobBuilderFactory jobBuilderFactory;

	@Autowired
	public StepBuilderFactory stepBuilderFactory;

	@Autowired
	public DataSource dataSource;

	@Value("classPath:input/inputData.csv")
	private Resource[] inputResources;

	private Resource outputResource = new
	FileSystemResource("output/outputData.csv");

	@Bean
	public DataSource dataSource() {
		final DriverManagerDataSource dataSource = new DriverManagerDataSource();
		try {
			dataSource.setDriverClassName("org.postgresql.Driver");
			dataSource.setUrl("jdbc:postgresql://localhost:5432/springbatch");
			dataSource.setUsername("postgres");
			dataSource.setPassword("openpgpwd");
		} catch (Exception e) {
		}

		return dataSource;
	}

	@Bean
	public MultiResourceItemReader<UfsFulfillment> multiResourceItemReader() {
		MultiResourceItemReader<UfsFulfillment> resourceItemReader = new MultiResourceItemReader<UfsFulfillment>();
		resourceItemReader.setResources(inputResources);
		resourceItemReader.setDelegate(fileReader());
		return resourceItemReader;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Bean
	public FlatFileItemReader<UfsFulfillment> fileReader() {
		FlatFileItemReader<UfsFulfillment> reader = new FlatFileItemReader<UfsFulfillment>();
		reader.setLinesToSkip(1);
		reader.setLineMapper(new DefaultLineMapper() {
			{
				setLineTokenizer(new DelimitedLineTokenizer() {
					{
						setNames(new String[] { "vendor", "status", "vendor_category" });
					}
				});
				setFieldSetMapper(new BeanWrapperFieldSetMapper<UfsFulfillment>() {
					{
						setTargetType(UfsFulfillment.class);
					}
				});
			}
		});
		return reader;
	}

	@Bean
	public JdbcBatchItemWriter<UfsFulfillment> databaseWriter() {
		JdbcBatchItemWriter<UfsFulfillment> itemWriter = new JdbcBatchItemWriter<UfsFulfillment>();
		itemWriter.setDataSource(dataSource());
		itemWriter.setSql("INSERT INTO ufs_batch_process (vendor, status, processed_time) VALUES (:vendor, :status, :processed_time)");
		itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<UfsFulfillment>());
		return itemWriter;
	}

	@Bean
	public JdbcCursorItemReader<UfsFulfillment> reader() {
		JdbcCursorItemReader<UfsFulfillment> reader = new JdbcCursorItemReader<UfsFulfillment>();
		reader.setDataSource(dataSource);
		reader.setSql("SELECT vendor, status, vendor_category FROM ufs_fulfillment");
		reader.setRowMapper(new UserRowMapper());

		return reader;
	}

	public class UserRowMapper implements RowMapper<UfsFulfillment> {

		@Override
		public UfsFulfillment mapRow(ResultSet rs, int rowNum) throws SQLException {
			UfsFulfillment user = new UfsFulfillment();
			user.setVendor(rs.getString("vendor"));
			user.setStatus(rs.getBoolean("status"));
			user.setVendor_category(rs.getString("vendor_category"));
			user.setProcessed_time(Common.getCurrentTimestamp());
			return user;
		}

	}

	@Bean
	public UserItemProcessor fileDataprocessor() {
		return new UserItemProcessor();
	}
	
	@Bean
	public UserItemProcessor databaseDataprocessor() {
		return new UserItemProcessor();
	}

	@Bean
	public FlatFileItemWriter<UfsFulfillment> fileWriter() {
		FlatFileItemWriter<UfsFulfillment> writer = new FlatFileItemWriter<UfsFulfillment>();
		writer.setResource(outputResource);
		writer.setAppendAllowed(true);
		writer.setLineAggregator(new DelimitedLineAggregator<UfsFulfillment>() {
			{
				setDelimiter(",");
				setFieldExtractor(new BeanWrapperFieldExtractor<UfsFulfillment>() {
					{
						setNames(new String[] { "vendor", "status", "vendor_category", "processed_time" });
					}
				});
			}
		});

		return writer;
	}

	@Bean
	public Step step1() {
		return stepBuilderFactory.get("step1").<UfsFulfillment, UfsFulfillment>chunk(10).reader(reader()).processor(databaseDataprocessor())
				.writer(fileWriter()).build();
	}

	@Bean
	public Step step2() {
		return stepBuilderFactory.get("step2").<UfsFulfillment, UfsFulfillment>chunk(5).reader(reader())
				.processor(fileDataprocessor()).writer(databaseWriter()).build();
	}
	
	@Bean
	public Step step3() {
		return stepBuilderFactory.get("step3").<UfsFulfillment, UfsFulfillment>chunk(5).reader(multiResourceItemReader())
				.processor(fileDataprocessor()).writer(databaseWriter()).build();
	}

	@Bean
	public Job exportUserJob() {
		return jobBuilderFactory.get("exportUserJob").incrementer(new RunIdIncrementer()).start(step1()).next(step2()).next(step3())
				.build();
	}
	

	@Bean
	public JobRepository jobRepository(MapJobRepositoryFactoryBean factory) throws Exception {
		return factory.getObject();
	}

	@Scheduled(cron = "10 * * * * *")
	public void perform() throws Exception {

		System.out.println("Job Started at :" + new Date());

		JobParameters param = new JobParametersBuilder().addString("JobID", String.valueOf(System.currentTimeMillis()))
				.toJobParameters();

		JobExecution execution = jobLauncher.run(exportUserJob(), param);

		System.out.println("Job finished with status :" + execution.getStatus());
	}
	
	@Bean
	public SimpleJobLauncher jobLauncher(JobRepository jobRepository) {
		SimpleJobLauncher launcher = new SimpleJobLauncher();
		launcher.setJobRepository(jobRepository);
		return launcher;
	}
	
	@Bean
	public ResourcelessTransactionManager transactionManager() {
		return new ResourcelessTransactionManager();
	}

	@Bean
	public MapJobRepositoryFactoryBean mapJobRepositoryFactory(ResourcelessTransactionManager txManager)
			throws Exception {

		MapJobRepositoryFactoryBean factory = new MapJobRepositoryFactoryBean(txManager);

		factory.afterPropertiesSet();

		return factory;
	}

}