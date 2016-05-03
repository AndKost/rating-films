package com.raitingfilms;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.raitingfilms.mainjobs.extra.AvgCount;
import org.bson.Document;
import scala.Tuple2;

import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
/**
 * Created by kost on 4/18/16.
 */
public class MongoDbSaver {

    public MongoClient mongoClient;

    private String nameDB = "ratingFilmsDB";

    public MongoDbSaver(String nameDB) {
        mongoClient = new MongoClient();
        //If want to save in other data base
        this.nameDB = nameDB;
    }

    public void saveMap(Map<String, Iterable<String>> inputMap, String collectionName) {

        MongoDatabase db = mongoClient.getDatabase(nameDB);

        List<Document> recordValues = new ArrayList<>();

        for (Map.Entry<String, Iterable<String>> t : inputMap.entrySet()) {
            String key = t.getKey();
            Iterable<String> iterStr = t.getValue();
            recordValues.add(new Document(key, iterStr));
        }
        db.getCollection(collectionName).insertMany(recordValues);
    }

    public void saveList(List<Tuple2<String, AvgCount>> listFilm, String collectionName) throws ParseException {

        MongoDatabase ratingFilmsDB = mongoClient.getDatabase(nameDB);

        List<Document> recordValues = new ArrayList<>();

        for (Tuple2<String, AvgCount> recValTmp :listFilm) {
            String filmTitle = recValTmp._1;
            float avgRating = recValTmp._2.avg();
            //Round avgRating to tenth
            DecimalFormat df1 = new DecimalFormat("0.##");
            String roundAvgRating = df1.format(avgRating);

            recordValues.add(new Document(filmTitle, roundAvgRating));
        }
        ratingFilmsDB.getCollection(collectionName).insertMany(recordValues);
    }
}
