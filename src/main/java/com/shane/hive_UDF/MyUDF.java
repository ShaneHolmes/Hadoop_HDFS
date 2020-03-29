package com.shane.hive_UDF;


import org.apache.hadoop.hive.ql.exec.UDF;

public class MyUDF extends UDF {

    public int evaluate(int a, int b){
        return a*a+b*b;
    }

    public int evaluate(int a, int b, int c){
        return a*a+b*b+c*c;
    }
}
