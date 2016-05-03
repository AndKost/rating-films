package com.raitingfilms;

import com.raitingfilms.mainjobs.RatingByGenre;
import com.raitingfilms.mainjobs.TopFilmsByAge;
import com.raitingfilms.mainjobs.TopGenreForUsers;
import com.raitingfilms.mainjobs.TotalTopFilms;
import com.raitingfilms.mainjobs.extra.AvgCount;
import com.raitingfilms.optionjobs.CountJob;
import com.raitingfilms.optionjobs.TopAndDiscussedFilmsByOccupation;
import com.raitingfilms.optionjobs.TopAndDiscussedFilmsByGender;
import com.raitingfilms.optionjobs.TopRatingFilmsByReleaseDateByGenre;
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

    static String pathToData = "../../workspace/rating-films/src/main/resources/u.data";
    static String pathToFilmInfo = "../../workspace/rating-films/src/main/resources/u.item";
    static String pathToUserInfo = "../../workspace/rating-films/src/main/resources/u.user";

    SparkConf conf;
    JavaSparkContext context;

    MongoDbSaver saver;

    public JobConroller() {
        conf = new SparkConf().setAppName("com.raitingfilms.Main").setMaster("local");
        context = new JavaSparkContext(conf);
        saver = new MongoDbSaver("ratingFilmsDB");
    }

    //Total top rating
    public void calcTotalTopFilms() throws ParseException {
        TotalTopFilms totalFilmJob = new TotalTopFilms(context);
        List<Tuple2<String, AvgCount>> resTitleFilmRating = totalFilmJob.run(pathToData, pathToFilmInfo);

        saver.saveList(resTitleFilmRating, "totalTopFilm");
    }

    //Top films by genre
    public void calcTotalTopFilmsByGenre(){
        RatingByGenre filmsByGenreJob = new RatingByGenre(context);
        Map<String, Iterable<String>> res = filmsByGenreJob.run(pathToData, pathToFilmInfo);

        saver.saveMap(res, "topFilmByGenre");
    }

    //Top films by ages
    public void calcTotalTopFilmsByAge(){
        TopFilmsByAge topByAgeJob = new TopFilmsByAge(context);
        Map<String, Iterable<String>> resAgeTitleFilm = topByAgeJob.run(pathToData, pathToFilmInfo, pathToUserInfo);

        saver.saveMap(resAgeTitleFilm, "topFilmByAge");
    }

    //Top genre by user
    public void calcTotalTopGenreByUsers(){
        TopGenreForUsers topGenreByUserJob = new TopGenreForUsers(context);
        Map<String, Iterable<String>> res = topGenreByUserJob.run(pathToData, pathToFilmInfo);

        saver.saveMap(res, "topGenreByUser");
    }

    //Top film by gender
    public void calcTopFilmsByGender() throws ParseException {
        boolean topRating = false;
        TopAndDiscussedFilmsByGender topByGenderJob = new TopAndDiscussedFilmsByGender(context, topRating);
        Map<String, Iterable<String>> resGenderFilmRating = topByGenderJob.run(pathToData, pathToFilmInfo, pathToUserInfo);

        saver.saveMap(resGenderFilmRating, "topFilmByGender");
    }

    //Most discussed film by gender
    public void calcMostDiscussedFilmsByGender() throws ParseException {
        boolean isMostDiscussed = true;
        TopAndDiscussedFilmsByGender topByGenderJob = new TopAndDiscussedFilmsByGender(context, isMostDiscussed);
        Map<String, Iterable<String>> resGenderFilmRating = topByGenderJob.run(pathToData, pathToFilmInfo, pathToUserInfo);

        saver.saveMap(resGenderFilmRating, "mostDiscussedFilmByGender");
    }

    //Top film by occupation
    public void calcTopFilmsByOccupation() throws ParseException {
        boolean topRating = false;
        TopAndDiscussedFilmsByOccupation topByOccupationJob = new TopAndDiscussedFilmsByOccupation(context, topRating);
        Map<String, Iterable<String>> resOccupationFilmRating = topByOccupationJob.run(pathToData, pathToFilmInfo, pathToUserInfo);

        saver.saveMap(resOccupationFilmRating, "topFilmByOccupation");
    }

    //Most discussed film by occupation
    public void calcMostDiscussedFilmsByOccupation() throws ParseException {
        boolean isMostDiscussed = true;
        TopAndDiscussedFilmsByOccupation mostDisByOccupationJob = new TopAndDiscussedFilmsByOccupation(context, isMostDiscussed);
        Map<String, Iterable<String>> resOccupationMostDis = mostDisByOccupationJob.run(pathToData, pathToFilmInfo, pathToUserInfo);

        saver.saveMap(resOccupationMostDis, "mostDiscussedFilmByOccupation");
    }

    //Top rating films by release date by genre
    public void calcTopFilmsByYearByGenre() throws ParseException {
        TopRatingFilmsByReleaseDateByGenre mostDisByByYearByGenre = new TopRatingFilmsByReleaseDateByGenre(context);
        Map<String, Iterable<String>> resByYearByGenre = mostDisByByYearByGenre.run(pathToData, pathToFilmInfo);

        saver.saveMap(resByYearByGenre, "topFilmByYearByGenre");
    }


}
