package com.raitingfilms.optionjobs;

import com.raitingfilms.mainjobs.extra.AvgCount;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by kost on 5/6/16.
 */
public class TopGenreByDiscussedByGender extends CountJob {

    public TopGenreByDiscussedByGender(JavaSparkContext context) {
        super(context);
    }


    public JavaPairRDD<String, Iterable<String>> run(String pathData, String pathItem, String pathUser) {

        JavaRDD<String> fileData = context.textFile(pathData);

        //Parse u.data text file and get (filmId, <userId, startRating>)
        JavaPairRDD<Integer, Tuple2<Integer, AvgCount>> filmIdUserIdRatingPair = fileData.mapToPair(mapUdataFilmIdIntUserIdRat);

        JavaRDD<String> fileUItem = context.textFile(pathItem);

        //Parse from uitem text file and get pair (filmId, genre)
        JavaPairRDD<Integer, String> filmTitlePair = fileUItem.flatMapToPair((s) -> {

            String[] row = s.split("\\|");
            Integer filmId = Integer.parseInt(row[0]);

            //Contain (filmId, genre). Film can have many genres
            List<Tuple2<Integer, String>> lstFilmIdGenre = new ArrayList<>();

            //List of genres for each film
            List<String> genresFilm = parseGenre(row);

            for (String genreIt : genresFilm) {
                lstFilmIdGenre.add(new Tuple2<>(filmId, genreIt));
            }

            return lstFilmIdGenre;
        });

        //Join pairs to change filmId to genre and get (genre, <userId startRating>)
        JavaRDD<Tuple2<String, Tuple2<Integer, AvgCount>>> joinTitlerKey = filmTitlePair.join(filmIdUserIdRatingPair).values();

        //Make key userId and get pairs (userId, <genre, rating>)
        JavaPairRDD<Integer, Tuple2<String, AvgCount>> userIdKeyPairs = joinTitlerKey.mapToPair(convertToIntKeyStrRat);

        JavaRDD<String> fileUser = context.textFile(pathUser);

        //Parse u.user text file and get pairs (userId, gender)
        JavaPairRDD<Integer, String> userIdGenderPairs = fileUser.mapToPair(mapUuserUserIdGender);

        //Join pairs for change userId to gender and get pairs (gender <genre, rating>)
        JavaRDD<Tuple2<String, Tuple2<String, AvgCount>>> genderKey = userIdGenderPairs.join(userIdKeyPairs).values();

        //Make key <gender, genre> to calculate average rating and get (<gender, genre>, rating)
        JavaPairRDD<Tuple2<String, String>, AvgCount> forAvgPairs = genderKey.mapToPair(convertToStr1Str2KeyRat);

        //Calculate average rating
        JavaPairRDD<Tuple2<String, String>, AvgCount> avgCounts = forAvgPairs.reduceByKey(reduceByKeyAvgRating);

        //Make key gender get (gender, <genre, avgRating>)
        JavaPairRDD<String, Tuple2<String, AvgCount>> userIdKeyAvgRat = avgCounts.mapToPair(convertToStr1KeyStrRat);

        //Group by key
        JavaPairRDD<String, Iterable<Tuple2<String, AvgCount>>> filmsGroupByGender = userIdKeyAvgRat.groupByKey();

        //Sort and get top genre
        JavaPairRDD<String, Iterable<String>> result = filmsGroupByGender.mapToPair(CountJob.sortAndTakeByCntDis);

        return result;
    }

}
