package com.batch.homework;


public class OutputPerson {

    private String id;
    private String firstname;
    private String lastname;
    private String age;

    public OutputPerson(String id, String firstname, String lastname, String age) {
        this.id = id;
        this.firstname = firstname;
        this.lastname = lastname;
        this.age = age;
    }

    public OutputPerson() {
    }

    public void setFirstname(String firstname) {
        this.firstname = firstname;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setLastname(String lastname) {
        this.lastname = lastname;
    }

    public void setAge(String age) {
        this.age = age;
    }

    public String getFirstname() {
        return firstname;
    }

    public String getLastname() {
        return lastname;
    }

    public String getAge() {
        return age;
    }

    public String getId() {
        return id;
    }
}
