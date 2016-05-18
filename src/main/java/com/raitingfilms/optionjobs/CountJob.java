package com.raitingfilms.optionjobs;

import com.raitingfilms.mainjobs.RatingJob;
import com.raitingfilms.mainjobs.extra.AvgCount;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.*;

/**
 * Created by kost on 5/1/16.
 */
//Class to calclulate mostDiscussed films
public class CountJob extends RatingJob {

    public CountJob(JavaSparkContext context) {
        super(context);
    }

    public static PairFunction<Tuple2<String, Iterable<Tuple2<String, AvgCount>>>,  String, Iterable<String>> sortAndTakeByCntDis =
            (PairFunction<Tuple2<String, Iterable<Tuple2<String, AvgCount>>>, String, Iterable<String>>) a -> {

                String frstVal = a._1();

                //Sort and take top
                Iterable<Tuple2<String, AvgCount>> filmIterable =  a._2();

                List<Tuple2<String, AvgCount>> lstFilms = new ArrayList<>();
                Iterator<Tuple2<String, AvgCount>> filmIterator = filmIterable.iterator();

                while (filmIterator.hasNext()) {
                    lstFilms.add(filmIterator.next());
                }

                //Sort by count od discussed
                Collections.sort(lstFilms, (followFilm, nextFilm) -> {
                    if (nextFilm._2().num < followFilm._2().num) return -1;
                    if (nextFilm._2().num > followFilm._2().num) return 1;
                    return 0;
                });

                //Check count of result
                if (lstFilms.size() >= TOP_COUNT)
                    lstFilms = lstFilms.subList(0,TOP_COUNT);

                List<String> result = new LinkedList<>();
                for (Tuple2<String, AvgCount> t : lstFilms){
                    result.add(t._1);
                }

                return new Tuple2<>(frstVal, result);
            };

    //Sort and take function for compare genre with timestamp
    public PairFunction<Tuple2<String, Iterable<Tuple2<String, Integer>>>,  String, Iterable<String>> sortAndTakeByTimeStamp =
            (PairFunction<Tuple2<String, Iterable<Tuple2<String, Integer>>>, String, Iterable<String>>) a -> {

                 String userId = a._1();

                //Sort and take top
                Iterable<Tuple2<String, Integer>> filmTimestampIterable =  a._2();

                List<Tuple2<String, Integer>> lstFilms = new ArrayList<>();
                Iterator<Tuple2<String, Integer>> filmTimeIterator = filmTimestampIterable.iterator();

                while (filmTimeIterator.hasNext()) {
                    lstFilms.add(filmTimeIterator.next());
                }

                //Sort by timestamp
                Collections.sort(lstFilms, (followGenreTime, nextGenreTime) -> {
                    if (nextGenreTime._2() < followGenreTime._2()) return -1;
                    if (nextGenreTime._2() > followGenreTime._2()) return 1;
                    return 0;
                });

                Integer threeLastFilm = 3;

                //Check count of result
                if (lstFilms.size() > threeLastFilm)
                    lstFilms = lstFilms.subList(0, threeLastFilm);

                List<String> result = new LinkedList<>();
                for (Tuple2<String, Integer> t : lstFilms){
                    result.add(t._1);
                }

                return new Tuple2<>(userId, result);
            };

    //Parse years from line, example from '02-Jan-1992' to '1992'
    protected String parseYearFromDate(String fullDate) {
        Integer startYear = 6;
        Integer finishYear = 10;

        if (fullDate.length() > 1)
            return fullDate.substring(startYear, finishYear);
        return "unknown";
    }
}
