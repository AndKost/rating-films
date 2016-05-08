package com.raitingfilms.optionjobs;

import com.raitingfilms.mainjobs.extra.AvgCount;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import scala.Tuple3;

import java.util.*;

/**
 * Created by kost on 5/2/16.
 */
public class TopRatingFilmsByReleaseDateByGenre extends CountJob {

    public TopRatingFilmsByReleaseDateByGenre(JavaSparkContext context) {
        super(context);
    }

    public JavaPairRDD<String, Iterable<String>> run(String pathData, String pathItem) {

        JavaRDD<String> fileData = context.textFile(pathData);

        //Parse u.date text file to pair(idFilm, startRating) startRating is AvgCount contain rating and number
        JavaPairRDD<Integer, AvgCount> filmIdRating = fileData.mapToPair(mapUdataFilmIdAvgRat);

        JavaRDD<String> fileItems = context.textFile(pathItem);

        //Parse u.item and get pair (idFilm, <filmTitle, genre, releaseDate>)
        JavaPairRDD<Integer, Tuple3<String, String, String>> filmTitleGenreReleaseDatePair = fileItems.flatMapToPair(
                (s) -> {
                    String[] row = s.split("\\|");
                    Integer filmId = Integer.parseInt(row[0]);
                    String filmTitle = row[1];
                    String releaseDate = parseYearFromDate(row[2]);

                    //Each film can contain many records
                    List<Tuple2<Integer, Tuple3<String, String, String>>> lstFilmIdTitleGenre = new ArrayList<>();

                    //Get list of genres for each film
                    List<String> genresFilm = parseGenre(row);

                    for (String genreIt : genresFilm) {
                        lstFilmIdTitleGenre.add(new Tuple2<>(filmId, new Tuple3<>(filmTitle, genreIt, releaseDate)));
                    }

                    return lstFilmIdTitleGenre;
                }
        );

        //Join pairs for change ifFilm to filmTitle and get pairs (<filmTitle, genre, releaseYear>, avgRating)
        JavaRDD<Tuple2<Tuple3<String, String, String>, AvgCount>> joinPair = filmTitleGenreReleaseDatePair.join(filmIdRating).values();

        //Convert to pairRDD
        JavaPairRDD<Tuple3<String, String, String>, AvgCount> toPairRDD = JavaPairRDD.fromJavaRDD(joinPair);

        //Calculate average rating for each film
        JavaPairRDD<Tuple3<String, String, String>, AvgCount> avgRating = toPairRDD.reduceByKey(reduceByKeyAvgRating);

        //Make key (<releaseYear, genre>, <title, avgRating>)
        JavaPairRDD<String, Tuple2<String, AvgCount>> yearGenreKeyPairs = avgRating.mapToPair(
                (s) -> {
                    String filmTitle = s._1()._1();
                    String genre = s._1()._2();
                    String releaseYear = s._1()._3();
                    AvgCount rating = s._2;
                    String conYearGenre = releaseYear + " " + genre;
                    Tuple2<String, AvgCount> tupleTitleRating = new Tuple2<>(filmTitle, rating);
                    return new Tuple2<>(conYearGenre, tupleTitleRating);
                });

        //Collect films by <releaseYear, genre>
        JavaPairRDD<String, Iterable<Tuple2<String, AvgCount>>> filmsGroupByYearGenre = yearGenreKeyPairs.groupByKey();

        //Sort and get top films
        JavaPairRDD<String, Iterable<String>> resultGenreFilms = filmsGroupByYearGenre.mapToPair(sortAndTakeByAvgRating);

        return resultGenreFilms;
    }
}