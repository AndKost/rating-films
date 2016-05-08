package com.raitingfilms.optionjobs;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Created by kost on 5/4/16.
 */
public class ThreeLastGenresByUser extends CountJob {

    public ThreeLastGenresByUser(JavaSparkContext context) {
        super(context);
    }

    public JavaPairRDD<String, Iterable<String>> run(String pathData, String pathItem) {

        JavaRDD<String> fileData = context.textFile(pathData);

        //Parse u.data file and get (filmmId, <userId, timestamp>)
        JavaPairRDD<Integer, Tuple2<String, Integer>> filmIdUserIdTimePair = fileData.mapToPair(mapUdataItemIdKeyUserIdTimestamp);

        JavaRDD<String> fileUItem = context.textFile(pathItem);

        //Parse u.item text file and get pair (filmId, genre)
        JavaPairRDD<Integer, String> filmGenrePair = fileUItem.flatMapToPair(generateFilmIdGenrePairs);

        //Join pairs to change filmId to gender and get (gender, <userId stimestamp>)
        JavaRDD<Tuple2<String, Tuple2<String, Integer>>> joinGenreKey = filmGenrePair.join(filmIdUserIdTimePair).values();

        //Make key userId get (userId, <genre, timestamp>)
        JavaPairRDD<String, Tuple2<String, Integer>> userIdKey = joinGenreKey.mapToPair(convertToStr2KeyStr1Int);

        //Collect genre for each user
        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> genreGroupByUser = userIdKey.groupByKey();

        //Sort and get top occupation
        JavaPairRDD<String, Iterable<String>> result = genreGroupByUser.mapToPair(sortAndTakeByTimeStamp);

        return result;
    }

}
