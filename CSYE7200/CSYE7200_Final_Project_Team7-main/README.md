# CSYE7200_Final_Project_Team7
![GitHub top language](https://img.shields.io/github/languages/top/ljch9725/CSYE7200_Final_Project_Team7.svg)

This project is designed as the final project of Northeastern University COE-CSYE7200 , taught by [Prof. Robin Hillyard](https://github.com/rchillyard).

## TeamMember

Team member:

| Name        | NuID      |
| ----------- | --------- |
| [@Jianchao Li](https://github.com/ljch9725) | 001054645 |
| [@Lin Zhu](https://github.com/Linzzz81)     | 001066973 |
| [@Yihao Gu](https://github.com/yougugugu)   | 001305641 |

## Abstract

Our goal is to predict the game result of League of Legends, using the information of first 15 minutes.The dataset is from [Kaggle](https://www.kaggle.com/chuckephron/leagueoflegends). We preprocessed the data and train 7 different models(Decision Tree, SVM, Logistic Regression, etc) with 9 features(Gold Diff, Kills, Structures, etc). Our program can automatically select the best model base on accuracy. Finally, we implement the UI by Play Framework. The users can input the information of first 15 minutes and then get the result of the match.

## Getting Started

First , clone or download the repository to local.

Open the **Final Project** file with ***IDEA***. Run ```Main.scala``` in ```Final Project/src/main/scala/Main.scala```.It will take 10-20 minutes to train all machine learning models when you first run ```Main.scala```. Follow the instruction in console to input required features and system will return predictions.

Another version of our project using ```Play Framework``` is also provided. Open the ***FinalProjectPlay/finalproject*** file with ***IDEA***. Open terminal and ```sbt run``` to run the project. Open the browser and enter ```localhost:9000```. It will take 20-40 minutes to train all machine learning models when you first access this website. Please refresh the website until all the training process is done. Follow the website instruction to input required features and the predictions will show under the website.


## Running the tests

Open the **Final Project** and ***FinalProjectPlay/finalproject***  files with ***IDEA***.

Run the tests ```sbt test``` in each terminals

## Built With

* [Scala](https://www.scala-lang.org/) - The program language to implement the program.
* [IntelliJ IDEA](https://www.jetbrains.com/idea/) - The IDE to development the system.
* [Spark](https://databricks.com/spark/about) - The framework to develop the Machine Learning process
* [Play](https://www.playframework.com/) - The framework to develop the UI with Scala

