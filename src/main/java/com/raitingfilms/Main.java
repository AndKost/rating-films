package com.raitingfilms;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.*;
import org.apache.spark.api.java.*;
import scala.Tuple2;
import java.util.Comparator;
import java.io.Serializable;
import java.io.File;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import com.google.common.base.Optional;

import java.util.Arrays;

public class Main {

    public static void main(String[] args) {

        String pathToData = "/home/kost/workspace/rating-films/src/main/resources/u.data";
        String pathToFilmInfo = "/home/kost/workspace/rating-films/src/main/resources/u.item";

        SparkConf conf = new SparkConf().setAppName("com.raitingfilms.Main").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);

        //Создали контекст, создали класс и вызвали run все
/*       TotalTopFilms job = new TotalTopFilms(context);
        List<Tuple2<String, Integer>> outputTop10Films = job.run(pathToData, pathToFilmInfo);

        System.out.println("Total top10 rating:");
        System.out.println(outputTop10Films);*/

        // top10Films.saveAsTextFile("/home/kost/workspace/spark-examples/src/test/resources/resultutest2/");
            RatingByGenre jobGenre = new RatingByGenre(context);
        List<Tuple2<String, Integer>> outputTop10FilmsGenre = jobGenre.run(pathToData, pathToFilmInfo, "Action");
        System.out.println("Total top10 genre rating:");
        //System.out.println(outputTop10FilmsGenre);
    }


}
