package com.raitingfilms.mainjobs;

import com.raitingfilms.mainjobs.extra.AvgCount;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by kost on 4/17/16.
 */
public class RatingByGenre extends RatingJob implements Serializable {

    public RatingByGenre(JavaSparkContext context) {
        super(context);
    }

    public JavaPairRDD<String, Iterable<String>> run(String pathData, String pathItem) {

        JavaRDD<String> fileData = context.textFile(pathData);

        //Parse u.date text file to pair(idFilm, startRating) startRating is AvgCount contain rating and number
        JavaPairRDD<Integer, AvgCount> filmIdRating = fileData.mapToPair(mapUdataFilmIdAvgRat);

        //Calculate average rating for each film
        JavaPairRDD<Integer, AvgCount> avgRating = filmIdRating.reduceByKey(reduceByKeyAvgRating);

        JavaRDD<String> fileItems = context.textFile(pathItem);

        //Parse u.item and get pair (idFilm, <namefilm, genre>)
        JavaPairRDD<Integer, Tuple2<String, String>> filmGenrePair = fileItems.flatMapToPair(
                (s) -> {
                        String[] row = s.split("\\|");
                        Integer filmId = Integer.parseInt(row[0]);
                        String filmTitle = row[1];

                        //Each film can contain many records
                        List<Tuple2<Integer, Tuple2<String, String>>> lstFilmIdTitleGenre = new ArrayList<>();

                        //Get list of genres for each film
                        List<String> genresFilm = parseGenre(row);

                        for (String genreIt : genresFilm) {
                            lstFilmIdTitleGenre.add(new Tuple2<>(filmId, new Tuple2<>(filmTitle, genreIt)));
                        }

                    return lstFilmIdTitleGenre;
                    }
        );

        //Join pairs for change ifFilm to filmTitle and get pairs (<filmTitle, genre>, avgRating)
        JavaRDD<Tuple2<Tuple2<String, String>, AvgCount>> joinPair = filmGenrePair.join(avgRating).values();

        //Make genre is key get pairs (genre, <filmTitle, avgRating>)
        JavaPairRDD<String, Tuple2<String, AvgCount>> genreKey = joinPair.mapToPair(convertToStrKeyStrRat);

        //Collect films by genre
        JavaPairRDD<String, Iterable<Tuple2<String, AvgCount>>> filmsGroupByGenre = genreKey.groupByKey();

        //Sort and get top films
        JavaPairRDD<String, Iterable<String>> resultGenreFilms = filmsGroupByGenre.mapToPair(sortAndTakeByAvgRating);

        return resultGenreFilms;
    }
}
