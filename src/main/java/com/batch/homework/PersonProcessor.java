package com.batch.homework;

import org.springframework.batch.item.ItemProcessor;

import java.time.LocalDate;
import java.time.Period;

public class PersonProcessor implements ItemProcessor<InputPerson, OutputPerson> {

    @Override
    public OutputPerson process(InputPerson item) {
        String personAge = String.valueOf(Period.between(LocalDate.parse(item.getBirthdate()), LocalDate.now()).getYears());
        return new OutputPerson(item.getId(), item.getFirstname(), item.getLastname(), personAge);
    }
}
