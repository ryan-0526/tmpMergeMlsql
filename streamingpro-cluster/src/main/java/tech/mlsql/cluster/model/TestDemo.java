package tech.mlsql.cluster.model;

import net.csdn.jpa.model.Model;

import java.util.List;


public class TestDemo extends Model {

    private int id;
    private String name;



    public static TestDemo findId(Integer id) {
        return TestDemo.findById(id);
    }

    public static List<TestDemo> items() {

        return  TestDemo.findAll();
    }


    public String getName() {
        return name;
    }


    public void setName(String name) {
        this.name = name;
    }

    public void setId(int id){
        this.id = id;

    }
    public int getId(){
        return id;
    }

}
