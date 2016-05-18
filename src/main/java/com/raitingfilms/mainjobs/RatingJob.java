package com.raitingfilms.mainjobs;

import com.raitingfilms.mainjobs.extra.AvgCount;
import com.raitingfilms.mainjobs.extra.ParseTextFile;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.*;

/**
 * Created by kost on 4/24/16.
 */
public class RatingJob extends ParseTextFile implements Serializable {

    protected static JavaSparkContext context;
    protected static int TOP_COUNT = 5;

    public RatingJob(JavaSparkContext context) {
        this.context = context;
    }

    //Compare average rating of films. Static to work in sortAndTake function
    protected static class filmComparator implements Comparator<Tuple2<String, AvgCount>>, Serializable {
        @Override
        public int compare(Tuple2<String, AvgCount> folowFilm, Tuple2<String, AvgCount> nextFilm){
            if (nextFilm._2().avg() < folowFilm._2().avg()) return -1;
            if (nextFilm._2().avg() > folowFilm._2().avg()) return 1;
            return 0;
        }
    }

    //Calculate average rating
    protected Function2<AvgCount, AvgCount, AvgCount> reduceByKeyAvgRating = (Function2<AvgCount, AvgCount, AvgCount>)
            (AvgRatingFollow, AvgRatingNext) -> {
                AvgRatingFollow.total += AvgRatingNext.total;
                AvgRatingFollow.num += AvgRatingNext.num;
                return AvgRatingFollow;
        };

    //Convert pair from (stringFirst, <stringSecond, rating>) to (<stringFirst,stringSecond>, rating)
    protected PairFunction<Tuple2<String, Tuple2<String, AvgCount>>, Tuple2<String, String>, AvgCount> convertToStr1Str2KeyRat =
            (s) -> {
                String strValFirst = s._1();
                String strValSecond = s._2()._1;
                AvgCount rating = s._2._2;
                Tuple2<String, String> tupleTitleAge = new Tuple2<>(strValFirst, strValSecond);
                return new Tuple2<>(tupleTitleAge, rating);
        };

    //Convert pair from (<stringFirst, stringSecond>, rating>) to (stringSecond,<stringFirst, rating>)
    protected PairFunction<Tuple2<Tuple2<String, String>, AvgCount>, String, Tuple2<String, AvgCount>> convertToStrKeyStrRat =
        (s) -> {
            String strValTupleSecond = s._1()._2;
            AvgCount avg = s._2;
            String strValTupleFirst = s._1._1;
            Tuple2<String, AvgCount> tupleTitleAvg = new Tuple2<>(strValTupleFirst, avg);
            return new Tuple2<>(strValTupleSecond, tupleTitleAvg);
        };

    //Convert pair from (String, <Integer, rating>) to (Integer,<string, rating>)
    protected PairFunction<Tuple2<String, Tuple2<Integer, AvgCount>>, Integer, Tuple2<String, AvgCount>> convertToIntKeyStrRat =
        (s) -> {
            String strValFirst = s._1();
            Integer intVal = s._2()._1;
            AvgCount rating = s._2._2;
            Tuple2<String, AvgCount> tupleTitleStartRat = new Tuple2<>(strValFirst, rating);
            return new Tuple2<>(intVal, tupleTitleStartRat);
        };

    //Convert pair from (<String1, String2>, rating) to (String1,<String2, rating>)
    protected PairFunction<Tuple2<Tuple2<String,String>, AvgCount>, String, Tuple2<String, AvgCount>> convertToStr1KeyStrRat =
        (s) -> {
            String strValTupleFirst = s._1()._1;
            String strValTupleSecond = s._1()._2;
            AvgCount rating = s._2;
            Tuple2<String, AvgCount> tmpTuple = new Tuple2<>(strValTupleSecond, rating);
            return new Tuple2<>(strValTupleFirst, tmpTuple);
        };

    //Convert pair from (String1, <Int1, Int2>) to (Int1,<String1, Int2>)
    protected PairFunction<Tuple2<String, Tuple2<String, Integer>>, String, Tuple2<String, Integer>> convertToStr2KeyStr1Int =
            (s) -> {
                String strValFirst = s._1();
                String intValTupleFirst = s._2()._1;
                Integer intValTupleSecond = s._2._2;
                Tuple2<String, Integer> tupleStrInt = new Tuple2<>(strValFirst, intValTupleSecond);
                return new Tuple2<>(intValTupleFirst, tupleStrInt);
            };

    //Parse u.item text file and generate pair (filmId, genre)
    protected PairFlatMapFunction<String, Integer, String> generateFilmIdGenrePairs =
            (s) -> {

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
            };

    //Get list of genre
    protected List<String> parseGenre(String[] row) {
        List<String> lstGenres = new ArrayList<>();

        //Separate by genre
        if (row[5].equals("1")) {
            lstGenres.add("unknown");
        }
        if (row[6].equals("1")) {
            lstGenres.add("Action");
        }
        if (row[7].equals("1")) {
            lstGenres.add("Adventure");
        }
        if (row[8].equals("1")) {
            lstGenres.add("Animation");
        }
        if (row[9].equals("1")) {
            lstGenres.add("Children's");
        }
        if (row[10].equals("1")) {
            lstGenres.add("Comedy");
        }
        if (row[11].equals("1")) {
            lstGenres.add("Crime");
        }
        if (row[12].equals("1")) {
            lstGenres.add("Documentary");
        }
        if (row[13].equals("1")) {
            lstGenres.add("Drama");
        }
        if (row[14].equals("1")) {
            lstGenres.add("Fantasy");
        }
        if (row[15].equals("1")) {
            lstGenres.add("Film-Noir");
        }
        if (row[16].equals("1")) {
            lstGenres.add("Horror");
        }
        if (row[17].equals("1")) {
            lstGenres.add("Musical");
        }
        if (row[18].equals("1")) {
            lstGenres.add("Mystery");
        }
        if (row[19].equals("1")) {
            lstGenres.add("Romance");
        }
        if (row[20].equals("1")) {
            lstGenres.add("Sci-Fi");
        }
        if (row[21].equals("1")) {
            lstGenres.add("Thriller");
        }
        if (row[22].equals("1")) {
            lstGenres.add("War");
        }
        if (row[23].equals("1")) {
            lstGenres.add("Western");
        }

        return lstGenres;
    }


    //Sort and take list of films in group by key
    public PairFunction<Tuple2<String, Iterable<Tuple2<String, AvgCount>>>,  String, Iterable<String>> sortAndTakeByAvgRating =
            (PairFunction<Tuple2<String, Iterable<Tuple2<String, AvgCount>>>, String, Iterable<String>>) a -> {

                String frstVal = a._1();

                //Sort and take top
                Iterable<Tuple2<String, AvgCount>> filmIterable =  a._2();

                List<Tuple2<String, AvgCount>> lstFilms = new ArrayList<>();
                Iterator<Tuple2<String, AvgCount>> filmIter = filmIterable.iterator();

                while (filmIter.hasNext()) {
                    lstFilms.add(filmIter.next());
                }

                //Sort by average
                Collections.sort(lstFilms, new filmComparator());

                //Check count of result
                if (lstFilms.size() >= TOP_COUNT)
                    lstFilms = lstFilms.subList(0,TOP_COUNT);

                List<String> result = new LinkedList<>();
                for (Tuple2<String, AvgCount> tmpFilm : lstFilms){
                    result.add(tmpFilm._1);
                }

                return new Tuple2<>(frstVal, result);
            };
}
