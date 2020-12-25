const express = require('express')
const app = express()

var bodyParser = require('body-parser')
const path = require('path');
var mongo = require('mongodb');
var mongoClient = mongo.MongoClient
//var url = "mongodb://localhost:27017/";
var url = "mongodb+srv://jamalhashim:Tp123456789!@cluster0-lavns.azure.mongodb.net/test?retryWrites=true&w=majority"

const port = 3000
app.use(bodyParser.json());
app.use(bodyParser.urlencoded({ extended: true }));

const newEntry = function(){
    return {
        timestamp:0,
        cow:[],
        milk:[],
        environmental:[]        
    }
}


mongoClient.connect(url, function (err, db) {
    if (err) {
        console.log(err)
    };
    console.log("connected to db");
    this.runApp(db)
}.bind(this));

this.runApp = function (db){
dbFarmer1 = db.db("farmer1")
app.post('/submit_data', function (req, res) { 
    var farmerId = req.body.farmer_id
    var cowId = req.body.cow_id
    var dataType = req.body.data_type
    var timestamp = req.body.timestamp //should be epoch int in seconds
    var data = req.body.data

    if(!(farmerId === 1)){
        res.send("Failure - Farmer Id not found")
        return;
    }
    var dateObj = new Date(timestamp*1000)
    dateObj = new Date(dateObj.getTime() + (dateObj.getTimezoneOffset() * 60000));
    var nearestDate = new Date(Date.UTC(dateObj.getFullYear(), dateObj.getMonth(), dateObj.getDate(), 0, 0, 0)).getTime()/1000
    var cowCollection = "cow"+cowId
    dbFarmer1.collection(cowCollection).count({timestamp:nearestDate}, function (err, result){
        if(err){
            res.send('Failure ' + err)
            return
        }
        if(result>=1){
            var pushquery = {}
            if(dataType=="cow"){
                pushquery = { $push: { cow: data } }
            }
            if(dataType=="milk"){
                pushquery = { $push: { milk: data } }
            }
            if(dataType=="environmental"){
                pushquery = { $push: { environmental: data } }
            }
            dbFarmer1.collection(cowCollection).updateOne({timestamp:nearestDate}, pushquery, function (err, result){
                if(!err){
                    res.send('Success')
                }
                else{
                    res.send('Failure ' + err)
                }
            })
        }
        else{
            var entry = newEntry()
            entry.timestamp = nearestDate
            if(dataType=="cow"){
                entry.cow.push(data)
            }
            if(dataType=="milk"){
                entry.milk.push(data)
            }
            if(dataType=="environmental"){
                entry.environmental.push(data)
            }
            dbFarmer1.collection(cowCollection).insertOne(entry, function(err, result){
                if(!err){
                    res.send('Success')
                }
                else{
                    res.send('Failure ' + err)
                }
            })
        }
    })

    


})
}

app.listen(port, () => console.log(`Example app listening at http://localhost:${port}`))