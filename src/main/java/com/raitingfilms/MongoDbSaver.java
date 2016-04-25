package com.raitingfilms;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.raitingfilms.mainjobs.extra.AvgCount;
import org.bson.Document;
import scala.Tuple2;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
/**
 * Created by kost on 4/18/16.
 */
public class MongoDbSaver {

    public static MongoDatabase db;
    public static MongoClient mongoClient;

    private String nameDB = "ratingFilmsDB";

    public String saveMap(Map<String, Iterable<String>> inputMap, String collectionName) {

        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase(nameDB);

        List<Document> documents = new ArrayList<Document>();

        for (Map.Entry<String, Iterable<String>> t : inputMap.entrySet()) {
            String key = t.getKey();
            Iterable<String> iterStr = t.getValue();
            documents.add(new Document(key, iterStr));
        }

        db.getCollection(collectionName).insertMany(documents);
        return "Recording is finish";
    }

    public String saveList(List<Tuple2<String, AvgCount>> listFilm, String collectionName) throws ParseException {

        MongoClient mongoClient = new MongoClient();
        MongoDatabase db = mongoClient.getDatabase(nameDB);

        List<Document> documents = new ArrayList<Document>();

        for (Tuple2<String, AvgCount> t :listFilm) {
            String filmName = t._1;
            documents.add(new Document("nameFilm" , filmName));
        }
        db.getCollection(collectionName).insertMany(documents);

        return "Recording is finish";
    }
}
