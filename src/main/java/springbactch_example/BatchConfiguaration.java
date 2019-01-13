package springbactch_example;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import javax.sql.DataSource;


import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.database.ItemPreparedStatementSetter;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.xml.StaxEventItemReader;
import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.oxm.xstream.XStreamMarshaller;

import com.web.sample.model.Person;

@Configuration
@EnableBatchProcessing
public class BatchConfiguaration {
	
	
	@Autowired
	public JobBuilderFactory jobbuilderfactory;
	
	@Autowired
	public StepBuilderFactory stepbuilderfactory;
	
	@Autowired
	public DataSource datasource;
	
	@Bean
	public DataSource datasource()
	{
		final DriverManagerDataSource datasource=new DriverManagerDataSource();
		datasource.setDriverClassName("com.mysql.jdbc.Driver");
		datasource.setUrl("jdbc:mysql://localhost:3306/traning");
		datasource.setUsername("root");
		datasource.setPassword("traning");
		
		return datasource;
		
}
	@Bean
	public StaxEventItemReader<Person> reader(){
		StaxEventItemReader<Person> reader=new StaxEventItemReader<Person>();
		reader.setResource(new ClassPathResource("Person.xml"));
		
		reader.setFragmentRootElementName("Person");
		
		Map<String,String> aliases= new HashMap<String,String>();
		aliases.put("Person","com.web.sample.model.Person");
		
	XStreamMarshaller xstreammarshaller=new XStreamMarshaller();
		try {
			xstreammarshaller.setAliases(aliases);
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		reader.setUnmarshaller(xstreammarshaller);
		
		
		return reader;
		
	}
	
	@Bean
	public JdbcBatchItemWriter<Person> writer()
	{
		
		JdbcBatchItemWriter<Person> writer=new JdbcBatchItemWriter<Person>();
		writer.setDataSource(datasource);
writer.setSql("INSERT INTO person (name,dob,gender,mobile_no,email,country,state,pincode,image)"+"VALUES (?,?,?,?,?,?,?,?,?)");
writer.setItemPreparedStatementSetter(new UserItemPreparedStatsetter());	

return writer;

	}

	private class UserItemPreparedStatsetter implements ItemPreparedStatementSetter<Person>{
		@Override
		public void setValues(Person person,PreparedStatement p)throws SQLException{
			
			p.setString(1,person.getName());
			p.setDate(2,Date.valueOf(person.getDOB()));
			p.setString(3,person.getGender());
			p.setInt(4,person.getMobileNo());
			p.setString(5,person.getEmail());
			p.setString(6,person.getCountry());
			p.setString(7,person.getState());
			p.setInt(8,person.getPincode());
			InputStream inputStream;
		try {
			inputStream = new FileInputStream(new File(person.getPhoto()));
			p.setBlob(9,inputStream);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		}
	}
		@Bean
		public Step step1()
		{
			
			return stepbuilderfactory.get("step1")//returns StepBuilder 
					.<Person,Person> chunk(100)//
					.reader(reader())
					.writer(writer())
					.build();
		}
		@Bean
		public Job importUserJob(){
			
			return jobbuilderfactory.get("importUserJob")
					.incrementer(new RunIdIncrementer())
					.flow(step1())
					.end()
					.build();
		}
		
}
