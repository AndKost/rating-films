package com.raitingfilms.mainjobs;

import com.raitingfilms.mainjobs.extra.AvgCount;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by kost on 4/19/16.
 */
public class TopGenreForUsers extends RatingJob implements Serializable {

    public TopGenreForUsers(JavaSparkContext context) {
        super(context);
    }

    public Map<String, Iterable<String>> run(String pathData, String pathItem) {

        //Parse u.data text file and get (filmId, <userId, startRating>)
        //startRating is AvgCount class wich contain rating value and number
        JavaRDD<String> fileData = context.textFile(pathData);

        //userId - string for universe saving in mongoDB
        JavaPairRDD<Integer, Tuple2<String, AvgCount>> filmRatingPair = fileData.mapToPair(
                (s) -> {
                        String[] row = s.split("\t");
                        String userId = row[0];
                        Integer filmId = Integer.parseInt(row[1]);
                        Integer rating = Integer.parseInt(row[2]);
                        AvgCount startRating =  new AvgCount(rating, 1);
                        Tuple2<String, AvgCount> tupleUserIdStartRat = new Tuple2<>(userId, startRating);
                        return new Tuple2<>(filmId, tupleUserIdStartRat);
                    }
        );

        //Parse u.item text file and get pair (idFilm, genre). One film may be different genres
        JavaRDD<String> fileUItem = context.textFile(pathItem);

        JavaPairRDD<Integer, String> filmGenrePair = fileUItem.flatMapToPair(
                (PairFlatMapFunction<String, Integer, String>) s -> {

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
                }
        );

        //Join for change filmId to genre and get (genre, <userId startRating>)
        JavaRDD<Tuple2<String, Tuple2<String, AvgCount> >> joinGenreKey = filmGenrePair.join(filmRatingPair).values();

        //Make key <genre, userid> for calculate average rating (<genre, userid> startRating)
        JavaPairRDD<Tuple2<String, String>, AvgCount> genreUserIdKeyPairs = joinGenreKey.mapToPair(convertToStr1Str2KeyRat);

        //Calculate average rating for each film
        JavaPairRDD<Tuple2<String, String>, AvgCount> resultAvgRating = genreUserIdKeyPairs.reduceByKey(reduceByKeyAvgRating);

        //Make key userId  and get (userId, <genre, avgRating>)
        JavaPairRDD<String, Tuple2<String, AvgCount>> userIdKeyAvgRat = resultAvgRating.mapToPair(convertToStrKeyStrRat);

        //Collect genre for each user
        JavaPairRDD<String, Iterable<Tuple2<String, AvgCount>>> genreGroupByUser = userIdKeyAvgRat.groupByKey();

        //Sort and get top genre
        JavaPairRDD<String, Iterable<String>> userGenresResult = genreGroupByUser.mapToPair(sortAndTakeByAvgRating);

        return userGenresResult.collectAsMap();
    }


}
