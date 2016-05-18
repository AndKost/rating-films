package com.raitingfilms;

import com.raitingfilms.mainjobs.RatingByGenre;
import com.raitingfilms.mainjobs.TopFilmsByAge;
import com.raitingfilms.mainjobs.TopGenreForUsers;
import com.raitingfilms.mainjobs.TotalTopFilms;
import com.raitingfilms.mainjobs.extra.AvgCount;
import com.raitingfilms.optionjobs.*;
import org.apache.avro.generic.GenericData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.text.ParseException;
import java.util.ArrayList;
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

        saver.saveTotalTopFilms(resTitleFilmRating, "totalTopFilm");
    }

    //Top films by genre
    public void calcTotalTopFilmsByGenre(){
        RatingByGenre filmsByGenreJob = new RatingByGenre(context);
        JavaPairRDD<String, Iterable<String>> res = filmsByGenreJob.run(pathToData, pathToFilmInfo);

        List<String> headerInfo = new ArrayList<>();
        headerInfo.add("genre");
        headerInfo.add("topFilms");
        saver.savePairRDD(res, "topFilmByGenre", headerInfo);
    }

    //Top films by ages
    public void calcTotalTopFilmsByAge(){
        TopFilmsByAge topByAgeJob = new TopFilmsByAge(context);
        JavaPairRDD<String, Iterable<String>> resAgeTitleFilm = topByAgeJob.run(pathToData, pathToFilmInfo, pathToUserInfo);

        List<String> headerInfo = new ArrayList<>();
        headerInfo.add("age");
        headerInfo.add("topFilms");

        saver.savePairRDD(resAgeTitleFilm, "topFilmByAge", headerInfo);
    }

    //Top genre by user
    public void calcTotalTopGenreByUsers(){
        TopGenreForUsers topGenreByUserJob = new TopGenreForUsers(context);
        JavaPairRDD<String, Iterable<String>> res = topGenreByUserJob.run(pathToData, pathToFilmInfo);

        List<String> headerInfo = new ArrayList<>();
        headerInfo.add("user");
        headerInfo.add("topFilms");

        saver.savePairRDD(res, "topGenreByUser", headerInfo);
    }

    //Top film by gender
    public void calcTopFilmsByGender() throws ParseException {
        boolean topRating = false;
        TopAndDiscussedFilmsByGender topByGenderJob = new TopAndDiscussedFilmsByGender(context, topRating);
        JavaPairRDD<String, Iterable<String>> resGenderFilmRating = topByGenderJob.run(pathToData, pathToFilmInfo, pathToUserInfo);

        List<String> headerInfo = new ArrayList<>();
        headerInfo.add("gender");
        headerInfo.add("topFilms");

        saver.savePairRDD(resGenderFilmRating, "topFilmByGender", headerInfo);
    }

    //Most discussed film by gender
    public void calcMostDiscussedFilmsByGender() throws ParseException {
        boolean isMostDiscussed = true;
        TopAndDiscussedFilmsByGender topByGenderJob = new TopAndDiscussedFilmsByGender(context, isMostDiscussed);
        JavaPairRDD<String, Iterable<String>> resGenderFilmRating = topByGenderJob.run(pathToData, pathToFilmInfo, pathToUserInfo);

        List<String> headerInfo = new ArrayList<>();
        headerInfo.add("gender");
        headerInfo.add("mostDiscussedFilms");

        saver.savePairRDD(resGenderFilmRating, "mostDiscussedFilmByGender", headerInfo);
    }

    //Top film by occupation
    public void calcTopFilmsByOccupation() throws ParseException {
        boolean topRating = false;
        TopAndDiscussedFilmsByOccupation topByOccupationJob = new TopAndDiscussedFilmsByOccupation(context, topRating);
        JavaPairRDD<String, Iterable<String>> resOccupationFilmRating = topByOccupationJob.run(pathToData, pathToFilmInfo, pathToUserInfo);

        List<String> headerInfo = new ArrayList<>();
        headerInfo.add("occupation");
        headerInfo.add("topFilms");

        saver.savePairRDD(resOccupationFilmRating, "topFilmByOccupation", headerInfo);
    }

    //Most discussed film by occupation
    public void calcMostDiscussedFilmsByOccupation() throws ParseException {
        boolean isMostDiscussed = true;
        TopAndDiscussedFilmsByOccupation mostDisByOccupationJob = new TopAndDiscussedFilmsByOccupation(context, isMostDiscussed);
        JavaPairRDD<String, Iterable<String>> resOccupationMostDis = mostDisByOccupationJob.run(pathToData, pathToFilmInfo, pathToUserInfo);

        List<String> headerInfo = new ArrayList<>();
        headerInfo.add("occupation");
        headerInfo.add("mostDiscussedFilms");

        saver.savePairRDD(resOccupationMostDis, "mostDiscussedFilmByOccupation", headerInfo);
    }

    //Top rating films by release date by genre
    public void calcTopFilmsByYearByGenre() throws ParseException {
        TopRatingFilmsByReleaseDateByGenre mostDisByByYearByGenre = new TopRatingFilmsByReleaseDateByGenre(context);
        JavaPairRDD<String, Iterable<String>> resByYearByGenre = mostDisByByYearByGenre.run(pathToData, pathToFilmInfo);

        List<String> headerInfo = new ArrayList<>();
        headerInfo.add("release date and genre");
        headerInfo.add("topFilms");

        saver.savePairRDD(resByYearByGenre, "topFilmByYearByGenre", headerInfo);
    }

    //Three last genre by user
    public void calcThreLastGenreByUser() throws ParseException {
        ThreeLastGenresByUser lastGenres = new ThreeLastGenresByUser(context);
        JavaPairRDD<String, Iterable<String>> resByUser = lastGenres.run(pathToData, pathToFilmInfo);

        List<String> headerInfo = new ArrayList<>();
        headerInfo.add("user");
        headerInfo.add("genres");

        saver.savePairRDD(resByUser, "lastGenreByUser", headerInfo);
    }

    //Top genres by most discussed films by gender
    public void calcTopGenresByDiscussedByGender() throws ParseException {
        TopGenreByDiscussedByGender topGenresByGender = new TopGenreByDiscussedByGender(context);
        JavaPairRDD<String, Iterable<String>> resByGender = topGenresByGender.run(pathToData, pathToFilmInfo, pathToUserInfo);

        List<String> headerInfo = new ArrayList<>();
        headerInfo.add("gender");
        headerInfo.add("genres");

        saver.savePairRDD(resByGender, "topGenresByDiscussedByUser", headerInfo);
    }



    //Top genres by most discussed films by occupation
    public void calcTopGenresByDiscussedByOccupation() throws ParseException {
        TopGenreByDiscussedByGender topGenresByOccupation = new TopGenreByDiscussedByGender(context);
        JavaPairRDD<String, Iterable<String>> resByOccupation = topGenresByOccupation.run(pathToData, pathToFilmInfo, pathToUserInfo);

        List<String> headerInfo = new ArrayList<>();
        headerInfo.add("occupation");
        headerInfo.add("genres");

        saver.savePairRDD(resByOccupation, "topGenresByDiscussedByOccupation", headerInfo);
    }

    //Top film by year
    public void calcTopFilmByYear() throws ParseException {
        TopFilmsByYear topFilmsByYear = new TopFilmsByYear(context);
        JavaPairRDD<String, Iterable<String>> resByYear = topFilmsByYear.run(pathToData, pathToFilmInfo);

        List<String> headerInfo = new ArrayList<>();
        headerInfo.add("year");
        headerInfo.add("films");

        saver.savePairRDD(resByYear, "topFilmsByYear", headerInfo);
    }

}
