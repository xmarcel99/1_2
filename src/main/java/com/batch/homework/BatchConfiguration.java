package com.batch.homework;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;

@Configuration
@EnableBatchProcessing
public class BatchConfiguration {
    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;

    BatchConfiguration(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
    }

    @Bean
    FlatFileItemReader<InputPerson> reader() {
        FlatFileItemReader<InputPerson> reader = new FlatFileItemReader<>();
        reader.setResource(new ClassPathResource("input.csv"));

        DelimitedLineTokenizer tokenizer = new DelimitedLineTokenizer();
        tokenizer.setNames("id", "firstname", "lastname", "birthdate");

        BeanWrapperFieldSetMapper<InputPerson> mapper = new BeanWrapperFieldSetMapper<>();
        mapper.setTargetType(InputPerson.class);

        DefaultLineMapper<InputPerson> lineMapper = new DefaultLineMapper<>();
        lineMapper.setLineTokenizer(tokenizer);
        lineMapper.setFieldSetMapper(mapper);

        reader.setLineMapper(lineMapper);

        return reader;
    }

    @Bean
    PersonProcessor processor() {
        return new PersonProcessor();
    }

    @Bean
    FlatFileItemWriter<OutputPerson> writer() {
        BeanWrapperFieldExtractor<OutputPerson> extractor = new BeanWrapperFieldExtractor<>();
        extractor.setNames(new String[]{"id", "firstname", "lastname", "age"});

        DelimitedLineAggregator<OutputPerson> aggregator = new DelimitedLineAggregator<>();
        aggregator.setDelimiter(",");
        aggregator.setFieldExtractor(extractor);

        FlatFileItemWriter<OutputPerson> writer = new FlatFileItemWriter<>();
        writer.setResource(new FileSystemResource("output.csv"));
        writer.setShouldDeleteIfExists(true);
        writer.setLineAggregator(aggregator);

        return writer;
    }

    @Bean
    Step calculateAge(

            ItemReader<InputPerson> reader,
            ItemProcessor<InputPerson, OutputPerson> processor,
            ItemWriter<OutputPerson> writer) {

        return stepBuilderFactory.get("calculateAge")
                .<InputPerson, OutputPerson>chunk(2)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();

    }

    @Bean
    Job calculateAgeJob(Step calculateAge) {
        return jobBuilderFactory.get("calculateAgeJob")
                .incrementer(new RunIdIncrementer())
                .flow(calculateAge)
                .end()
                .build();
    }
}
