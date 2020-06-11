package com.example.zadb;

import com.mongodb.DB;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


public class Application {

    public static void main(String[] args) {

        String client = "mongodb://zabd2020:aggregations@db2020-aggregations-shard-00-00-2sawx.mongodb.net:27017/admin?authSource=admin&ssl=true";
        MongoClientURI uri = new MongoClientURI(client);
        MongoClient mongoClient = new MongoClient(uri);

        MongoDatabase mongoDatabase = mongoClient.getDatabase("db-aggregations");
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection("movies");
        FindIterable<Document> findIterable = mongoCollection.find();
        MongoCursor<Document> cursor = findIterable.iterator();

        try{
            while(cursor.hasNext()){
                System.out.println(cursor.next().toJson());
            }
        } finally {
            cursor.close();
        }
    }

}