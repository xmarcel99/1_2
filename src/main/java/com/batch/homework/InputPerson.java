package com.batch.homework;


public class InputPerson {

    private String id;
    private String firstname;
    private String lastname;
    private String birthdate;

    public InputPerson(String id, String firstname, String lastname, String birthdate) {
        this.id = id;
        this.firstname = firstname;
        this.lastname = lastname;
        this.birthdate = birthdate;
    }

    public InputPerson() {
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

    public void setBirthdate(String birthdate) {
        this.birthdate = birthdate;
    }

    public String getFirstname() {
        return firstname;
    }

    public String getLastname() {
        return lastname;
    }

    public String getBirthdate() {
        return birthdate;
    }

    public String getId() {
        return id;
    }
}
