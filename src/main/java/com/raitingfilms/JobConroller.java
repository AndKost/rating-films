package com.raitingfilms;

import com.raitingfilms.mainjobs.RatingByGenre;
import com.raitingfilms.mainjobs.TopFilmsByAge;
import com.raitingfilms.mainjobs.TopGenreForUsers;
import com.raitingfilms.mainjobs.TotalTopFilms;
import com.raitingfilms.mainjobs.extra.AvgCount;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.text.ParseException;
import java.util.List;
import java.util.Map;

/**
 * Created by kost on 4/23/16.
 */
public class JobConroller {

    String pathToData = "../../workspace/rating-films/src/main/resources/u.data";
    String pathToFilmInfo = "../../workspace/rating-films/src/main/resources/u.item";
    String pathToUserInfo = "../../workspace/rating-films/src/main/resources/u.user";

    SparkConf conf;
    JavaSparkContext context;

    MongoDbSaver saver;

    public JobConroller() {
        conf = new SparkConf().setAppName("com.raitingfilms.Main").setMaster("local");
        context = new JavaSparkContext(conf);
        saver = new MongoDbSaver();
    }

    //Total top rating
    public void calcTotalTopFilms() throws ParseException {
        TotalTopFilms job = new TotalTopFilms(context);
        List<Tuple2<String, AvgCount>> res = job.run(pathToData, pathToFilmInfo);
        saver.saveList(res, "totalTopFilm");
    }

    //Top films by genre
    public void calcTotalTopFilmsByGenre(){
        RatingByGenre job = new RatingByGenre(context);
        Map<String, Iterable<String>> res = job.run(pathToData, pathToFilmInfo);
        saver.saveMap(res, "topFilmByGenre");
    }

    //Top films by ages
    public void calcTotalTopFilmsByAge(){
        TopFilmsByAge job = new TopFilmsByAge(context);
        Map<String, Iterable<String>> res = job.run(pathToData, pathToFilmInfo, pathToUserInfo);

        saver.saveMap(res, "topFilmByAge");

    }

    //Top genre by user
    public void calcTotalTopGenreByUsers(){
        TopGenreForUsers job = new TopGenreForUsers(context);
        Map<String, Iterable<String>> res = job.run(pathToData, pathToFilmInfo);
        saver.saveMap(res, "topGenreByUser");
    }

}
