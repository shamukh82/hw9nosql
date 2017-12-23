package com.hse.hw9.nosql;
/**
 * Created by smukherjee5 on 11/4/17.
 */


import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.PreparedStatement;





public class CassandraClient {

    private static Cluster cluster;
    private static Session session;
    public static void main(String[] args) {

        try {
            cluster = Cluster.builder()
                    .addContactPoint("127.0.0.1")
                    .build();
            session = cluster.connect();


            addPerson(2001, "john", "smith-java1","San Jose","5554443333","5554443331","5554443332");
            addPerson(2002, "john", "smith-java2","San Francisco","5554443333","5554443331","5554443332");
            addPerson(2003, "john", "smith-java3","Foster City","5554443333","5554443331","5554443332");

            ResultSet rs = session.execute("select person_id,fname,lname,city,cellphone1,cellphone2,cellphone3 from mykeyspace.person where person_id in (2001,2002,2003)");

            System.out.println("Before update");
            for (Row row : rs) {
                System.out.println(row.getInt("person_id")+ "||"+
                        row.getString("fname")+ "||"+
                        row.getString("lname")+ "||"+
                        row.getString("city")+ "||"+
                        row.getString("cellphone1")+ "||"+
                        row.getString("cellphone2")+ "||"+
                        row.getString("cellphone1"));
            }

            updatePersonCity(2003,"Burlingame");

            System.out.println("After update");
            rs = session.execute("select person_id,fname,lname,city,cellphone1,cellphone2,cellphone3 from mykeyspace.person where person_id in (2001,2002,2003)");


            for (Row row : rs) {
                System.out.println(row.getInt("person_id")+ "||"+
                        row.getString("fname")+ "||"+
                        row.getString("lname")+ "||"+
                        row.getString("city")+ "||"+
                        row.getString("cellphone1")+ "||"+
                        row.getString("cellphone2")+ "||"+
                        row.getString("cellphone1"));
            }


                        } finally {
            if (cluster != null) cluster.close();
        }
    }


    private static void addPerson(int person_id,String fname,
                        String lname,String city,String cellphone1,String cellphone2,String cellphone3) {
        PreparedStatement prepared = session.prepare("insert into mykeyspace.person " +
                                                            "(person_id,fname, lname,city,cellphone1,cellphone2,cellphone3) " +
                                                                    "values (?,?,?,?,?,?,?)");
        session.execute(prepared.bind(person_id,fname,lname,city,cellphone1,cellphone2,cellphone3));
    }

    private static void updatePersonCity(int person_id,String city) {
        PreparedStatement prepared = session.prepare("update mykeyspace.person set city = ? where person_id =?");
        session.execute(prepared.bind(city,person_id));
    }




}
