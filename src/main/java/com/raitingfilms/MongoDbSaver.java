package com.raitingfilms;

import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.DBObject;
import com.mongodb.hadoop.MongoOutputFormat;
import com.raitingfilms.mainjobs.extra.AvgCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.bson.BSONObject;
import scala.Tuple2;

import java.text.ParseException;
import java.util.List;
/**
 * Created by kost on 4/18/16.
 */
public class MongoDbSaver {

    private final String nameDB;

    public MongoDbSaver(String nameDB) {
        //If want to save in other data base
        this.nameDB = nameDB;
    }

    public void saveTotalTopFilms(JavaRDD<Tuple2<String, AvgCount>> listFilm, String collectionName) throws ParseException {

        // create BSON output RDD from predictions
        JavaPairRDD<Object,BSONObject> predictions = listFilm.mapToPair(
                s -> {
                    DBObject doc = BasicDBObjectBuilder.start()
                            .add("film title", s._1)
                            .add("rating", s._2)
                            .get();
                    // null key means an ObjectId will be generated on insert
                    return new Tuple2<Object, BSONObject>(null, doc);
                }
        );

        // create MongoDB output Configuration
        Configuration outputConfig = new Configuration();
        outputConfig.set("mongo.output.format", "com.mongodb.hadoop.MongoOutputFormat");
        outputConfig.set("mongo.output.uri", "mongodb://localhost:27017/" + nameDB + "." + collectionName);

        predictions.saveAsNewAPIHadoopFile("file:///not-applicable",
                Object.class, Object.class, MongoOutputFormat.class, outputConfig);

    }

    public void savePairRDD(JavaPairRDD<String, Iterable<String>> inputRDD, String collectionName, List<String> headerInfo) {

        //headerInfo containd headers for mongoDB structure
        String firstTitle = headerInfo.get(0);
        String secondTitle = headerInfo.get(1);
        // create BSON output RDD from predictions
        JavaPairRDD<Object,BSONObject> predictions = inputRDD.mapToPair(
                s -> {
                        DBObject doc = BasicDBObjectBuilder.start()
                                .add(firstTitle, s._1)
                                .add(secondTitle, s._2)
                                .get();
                        // null key means an ObjectId will be generated on insert
                        return new Tuple2<Object, BSONObject>(null, doc);
                    }
        );

        // create MongoDB output Configuration
        Configuration outputConfig = new Configuration();
        outputConfig.set("mongo.output.format", "com.mongodb.hadoop.MongoOutputFormat");
        outputConfig.set("mongo.output.uri", "mongodb://localhost:27017/" + nameDB + "." + collectionName);

        predictions.saveAsNewAPIHadoopFile("file:///appfile",
                Object.class, Object.class, MongoOutputFormat.class, outputConfig);

    }
}
