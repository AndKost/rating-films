package com.raitingfilms.optionjobs;

import com.raitingfilms.mainjobs.extra.AvgCount;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Created by kost on 5/6/16.
 */
public class TopFilmsByYear extends CountJob {
    public TopFilmsByYear(JavaSparkContext context) {
        super(context);
    }

    public JavaPairRDD<String, Iterable<String>> run(String pathData, String pathItem) {

        JavaRDD<String> fileData = context.textFile(pathData);

        //Parse u.data file and get (filmmId, startRating)
        JavaPairRDD<Integer, AvgCount> filmIdUserIdStartRating = fileData.mapToPair(mapUdataFilmIdAvgRat);

        //Calculate average rating
        JavaPairRDD<Integer, AvgCount> filmIdUserIdAvgRating = filmIdUserIdStartRating.reduceByKey(reduceByKeyAvgRating);

        JavaRDD<String> fileItems = context.textFile(pathItem);

        //Parse u.item and get pair (filmId, <filmTitle, releaseYear>)
        JavaPairRDD<Integer, Tuple2<String, String>> filmTitleGenreReleaseDatePair = fileItems.mapToPair(
                (s) -> {
                    String[] row = s.split("\\|");
                    Integer filmId = Integer.parseInt(row[0]);
                    String filmTitle = row[1];
                    String releaseYear = parseYearFromDate(row[2]);
                    Tuple2<String, String> tupleTitleYear = new Tuple2<>(filmTitle, releaseYear);
                    return new Tuple2<>(filmId, tupleTitleYear);
                }
        );

        //Joint to change filmId to <filmTitle, releaseYear> get pairs (<filmTitle, releaseYear> startRating)
        JavaRDD<Tuple2<Tuple2<String, String>, AvgCount>> joinTitleYearKey = filmTitleGenreReleaseDatePair.join(filmIdUserIdAvgRating).values();

        //Make release year key and get pairs (releaseYear, <title, avgRating>)
        JavaPairRDD<String, Tuple2<String, AvgCount>> yearKeyPairs = joinTitleYearKey.mapToPair(convertToStrKeyStrRat);

        //Collect films for each year
        JavaPairRDD<String, Iterable<Tuple2<String, AvgCount>>> filmsGroupByYear = yearKeyPairs.groupByKey();

        //Sort and get top films
        JavaPairRDD<String, Iterable<String>> yearFilmsResult = filmsGroupByYear.mapToPair(sortAndTakeByAvgRating);

        return yearFilmsResult;
    }

}
