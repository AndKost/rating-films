package com.raitingfilms;


import com.raitingfilms.jobs.RatingByGenre;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import com.raitingfilms.MongoDbSaver;
import com.mongodb.DBObject;

import java.util.List;

import java.text.ParseException;

public class Main {

    public static enum Genres { unknown, Action, Adventure, Animation, Children, Comedy, Crime, Documentary, Drama, Fantasy,
        FilmNoir, Horror, Musical, Mystery, Romance, SciFi, Thriller, War, Western }

    public static void main(String[] args) throws ParseException {

        String pathToData = "/home/kost/workspace/rating-films/src/main/resources/u.data";
        String pathToFilmInfo = "/home/kost/workspace/rating-films/src/main/resources/u.item";

        SparkConf conf = new SparkConf().setAppName("com.raitingfilms.Main").setMaster("local");
        JavaSparkContext context = new JavaSparkContext(conf);

/*       TotalTopFilms job = new TotalTopFilms(context);
        List<Tuple2<String, Integer>> outputTop10Films = job.run(pathToData, pathToFilmInfo);

        System.out.println("Total top10 rating:");
        System.out.println(outputTop10Films);*/

        // top10Films.saveAsTextFile("/home/kost/workspace/spark-examples/src/test/resources/resultutest2/");


        //Show top10 films by genre
        RatingByGenre jobGenre = new RatingByGenre(context);

        Genres inputGen = Genres.Action;
        List<Tuple2<String, Integer>> outputTop10FilmsGenre = jobGenre.run(pathToData, pathToFilmInfo, inputGen);

        System.out.println("Total top10 genre rating " + inputGen.toString() + ": ");
        System.out.println(outputTop10FilmsGenre);

        MongoDbSaver saver = new MongoDbSaver();
        String result = saver.save(outputTop10FilmsGenre);

        System.out.println(result);

    }


}
