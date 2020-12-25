# Dairy Project

## Project Objective

Find the association between milk composition and environmental conditions (weather station data, barn environmental data, and body temperature) and prediction of production shifts based on environmental data.

## What our project does.

-   Processes initial training data to combine features and labels for the ML model
    
-   Listens for streams of inputted information from multiple farms at once
    
-   Cleans data and eliminates outliers
    
-   Segregates and stores data in a database
    
-   Makes weekly predictions on the quality of milk based on the past week’s data
    
-   Relays those predictions to the farmers

## Project Architecture

 ### Data processing
-   Python script for filtering and combining the dairy data
-   Spark code for filtering and combining our streamed data
    

### API
    
-   Multiple Node JS servers hosted on Azure VM’s behind an Azure load balance
-   MongoDB Atlas cluster to store data
### Spark/SparkML
-   Loads data from MongoDB Atlas into a dataframe
 
-   Trains ML model based on given training data and valid MongoDB training data
    
-   Executes ML model for last 7 days of cow/environmental data

## Setup

brew install scala
download intellij, open, go to configure, get scala plugin
brew install sbt 
go to cloud/test3/test3
run sbt in the terminal
in the shell, run "update"
after that, in the shell, run "run"
There should be a line in there that says
TEXT FILE LENGTH:
4
scala 2.12.10

