package com.raitingfilms.mainjobs.extra;

import com.raitingfilms.mainjobs.extra.AvgCount;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by kost on 4/30/16.
 */
public class ParseTextFile {

    //Parse u.item text file and get pair (filmId, title)
    protected PairFunction<String, Integer, String> mapUitemFilmIdTitle =
            (s) -> {
                String[] row = s.split("\\|");
                Integer filmId = Integer.parseInt(row[0]);
                String filmTitle = row[1];
                return new Tuple2<>(filmId, filmTitle);
            };

    //Parse u.data text file and get pair (filmId, startRating)
    protected PairFunction<String, Integer, AvgCount>  mapUdataFilmIdAvgRat =
            (s) -> {
                String[] row = s.split("\t");
                Integer filmId = Integer.parseInt(row[1]);
                Integer rating = Integer.parseInt(row[2]);
                return new Tuple2<>(filmId, new AvgCount(rating, 1));
            };

    //Parse u.data text file and get pair (filmId <userID, startRating>) avgCount contain rating and number
    protected PairFunction<String, Integer, Tuple2<Integer, AvgCount>> mapUdataFilmIdIntUserIdRat =
            (s) -> {
                String[] row = s.split("\t");
                Integer userId = Integer.parseInt(row[0]);
                Integer filmId = Integer.parseInt(row[1]);
                Integer rating = Integer.parseInt(row[2]);
                Tuple2<Integer, AvgCount> tmp = new Tuple2<>(userId, new AvgCount(rating, 1));
                return new Tuple2<>(filmId, tmp);
            };

    //Parse u.user text file and get pairs (userId, gender)
    protected PairFunction<String, Integer, String> mapUuserUserIdGender =
        (s) -> {
            String[] row = s.split("\\|");
            Integer userId = Integer.parseInt(row[0]);
            String gender = row[2];
            return new Tuple2<>(userId, gender);
        };

    //Parse u.user text file and get pairs (userId, occupation)
    protected PairFunction<String, Integer, String> mapUuserUserIdOccupation =
        (s) -> {
            String[] row = s.split("\\|");
            Integer userId = Integer.parseInt(row[0]);
            String occupation = row[3];
            return new Tuple2<>(userId, occupation);
        };

}
