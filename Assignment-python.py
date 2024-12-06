# Databricks notebook source
# MAGIC %md
# MAGIC # Data-Intensive Programming - Group assignment
# MAGIC
# MAGIC This is the **Python** version of the assignment. Switch to the Scala version, if you want to do the assignment in Scala.
# MAGIC
# MAGIC In all tasks, add your solutions to the cells following the task instructions. You are free to add new cells if you want.<br>
# MAGIC The example outputs, and some additional hints are given in a separate notebook in the same folder as this one.
# MAGIC
# MAGIC Don't forget to **submit your solutions to Moodle** once your group is finished with the assignment.
# MAGIC
# MAGIC ## Basic tasks (compulsory)
# MAGIC
# MAGIC There are in total nine basic tasks that every group must implement in order to have an accepted assignment.
# MAGIC
# MAGIC The basic task 1 is a separate task, and it deals with video game sales data. The task asks you to do some basic aggregation operations with Spark data frames.
# MAGIC
# MAGIC The other basic coding tasks (basic tasks 2-8) are all related and deal with data from [https://figshare.com/collections/Soccer_match_event_dataset/4415000/5](https://figshare.com/collections/Soccer_match_event_dataset/4415000/5) that contains information about events in [football](https://en.wikipedia.org/wiki/Association_football) matches in five European leagues during the season 2017-18. The tasks ask you to calculate the results of the matches based on the given data as well as do some further calculations. Special knowledge about football or the leagues is not required, and the task instructions should be sufficient in order to gain enough context for the tasks.
# MAGIC
# MAGIC Finally, the basic task 9 asks some information on your assignment working process.
# MAGIC
# MAGIC ## Advanced tasks (optional)
# MAGIC
# MAGIC There are in total of four advanced tasks that can be done to gain some course points. Despite the name, the advanced tasks may or may not be harder than the basic tasks.
# MAGIC
# MAGIC The advanced task 1 asks you to do all the basic tasks in an optimized way. It is possible that you gain some points from this without directly trying by just implementing the basic tasks efficiently. Logic errors and other issues that cause the basic tasks to give wrong results will be taken into account in the grading of the first advanced task. A maximum of 2 points will be given based on advanced task 1.
# MAGIC
# MAGIC The other three advanced tasks are separate tasks and their implementation does not affect the grade given for the advanced task 1.<br>
# MAGIC Only two of the three available tasks will be graded and each graded task can provide a maximum of 2 points to the total.<br>
# MAGIC If you attempt all three tasks, clearly mark which task you want to be used in the grading. Otherwise, the grader will randomly pick two of the tasks and ignore the third.
# MAGIC
# MAGIC Advanced task 2 continues with the football data and contains further questions that are done with the help of some additional data.<br>
# MAGIC Advanced task 3 deals with some image data and the questions are mostly related to the colors of the pixels in the images.<br>
# MAGIC Advanced task 4 asks you to do some classification related machine learning tasks with Spark.
# MAGIC
# MAGIC It is possible to gain partial points from the advanced tasks. I.e., if you have not completed the task fully but have implemented some part of the task, you might gain some appropriate portion of the points from the task. Logic errors, very inefficient solutions, and other issues will be taken into account in the task grading.
# MAGIC
# MAGIC ## Assignment grading
# MAGIC
# MAGIC Failing to do the basic tasks, means failing the assignment and thus also failing the course!<br>
# MAGIC "A close enough" solutions might be accepted => even if you fail to do some parts of the basic tasks, submit your work to Moodle.
# MAGIC
# MAGIC Accepted assignment submissions will be graded from 0 to 6 points.
# MAGIC
# MAGIC The maximum grade that can be achieved by doing only the basic tasks is 2/6 points (through advanced task 1).

# COMMAND ----------

# MAGIC %md
# MAGIC ## Short summary
# MAGIC
# MAGIC ##### Minimum requirements (points: 0-2 out of maximum of 6):
# MAGIC
# MAGIC - All basic tasks implemented (at least in "a close enough" manner)
# MAGIC - Moodle submission for the group
# MAGIC
# MAGIC ##### For those aiming for higher points (0-6):
# MAGIC
# MAGIC - All basic tasks implemented
# MAGIC - Optimized solutions for the basic tasks (advanced task 1) (0-2 points)
# MAGIC - Two of the other three advanced tasks (2-4) implemented
# MAGIC     - Clearly marked which of the two tasks should be graded
# MAGIC     - Each graded advanced task will give 0-2 points
# MAGIC - Moodle submission for the group

# COMMAND ----------

# import statements for the entire notebook
# add anything that is required here

import re
from typing import Dict, List, Tuple

from pyspark.sql import DataFrame
from pyspark.sql.functions import round, year, sum, col, count, array_contains, max, when, lit, min, expr, concat, avg, row_number, broadcast, concat_ws, ceil, explode
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, DateType, DoubleType

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 1 - Video game sales data
# MAGIC
# MAGIC The CSV file `assignment/sales/video_game_sales.csv` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2024-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2024gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) contains video game sales data (based on [https://www.kaggle.com/datasets/patkle/video-game-sales-data-from-vgchartzcom](https://www.kaggle.com/datasets/patkle/video-game-sales-data-from-vgchartzcom)).
# MAGIC
# MAGIC Load the data from the CSV file into a data frame. The column headers and the first few data lines should give sufficient information about the source dataset. The numbers in the sales columns are given in millions.
# MAGIC
# MAGIC Using the data, find answers to the following:
# MAGIC
# MAGIC - Which publisher has the highest total sales in video games in North America considering games released in years 2006-2015?
# MAGIC - How many titles in total for this publisher do not have sales data available for North America considering games released in years 2006-2015?
# MAGIC - Separating games released in different years and considering only this publisher and only games released in years 2006-2015, what are the total sales, in North America and globally, for each year?
# MAGIC     - I.e., what are the total sales (in North America and globally) for games released by this publisher in year 2006? And the same for year 2007? ...
# MAGIC

# COMMAND ----------

videoGameSales_schema = StructType([
    StructField("title", StringType(), True),
    StructField("publisher", StringType(), True),
    StructField("developer", StringType(), True),
    StructField("release_date", DateType(), True),
    StructField("platform", StringType(), True),
    StructField("total_sales", DoubleType(), True),
    StructField("na_sales", DoubleType(), True),
    StructField("japan_sales", DoubleType(), True),
    StructField("pal_sales", DoubleType(), True),
    StructField("other_sales", DoubleType(), True),
    StructField("user_score", DoubleType(), True),
    StructField("critic_score", DoubleType(), True)
])

videoGameSalesDF: DataFrame = spark.read  \
  .option("header", "true") \
  .option("sep", "|") \
  .schema(videoGameSales_schema) \
  .csv("abfss://shared@tunics320f2024gen2.dfs.core.windows.net/assignment/sales/video_game_sales.csv")

videoGameSalesDF = videoGameSalesDF.select("title", "publisher", "developer", "release_date", "platform", "total_sales", "na_sales")
filteredDF = videoGameSalesDF.filter((year(videoGameSalesDF.release_date) >= 2006) & (year(videoGameSalesDF.release_date) <= 2015))

bestNAPublisher = filteredDF.groupBy("publisher").sum("na_sales").orderBy("sum(na_sales)", ascending=False).first()["publisher"]

titlesWithMissingSalesData = filteredDF.filter((filteredDF.publisher == bestNAPublisher) & (filteredDF.na_sales.isNull())).count()

bestNAPublisherSales = filteredDF.filter(filteredDF.publisher == bestNAPublisher).groupBy(year("release_date").alias("year")).agg(
    round(sum("na_sales"), 2).alias("na_sales"),
    round(sum("total_sales"), 2).alias("total_sales")
).orderBy("year")

print(f"The publisher with the highest total video game sales in North America is: '{bestNAPublisher}'")
print(f"The number of titles with missing sales data for North America: {titlesWithMissingSalesData}")
print("Sales data for the publisher:")
bestNAPublisherSales.show()


# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 2 - Event data from football matches
# MAGIC
# MAGIC A parquet file in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2024-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2024gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) at folder `assignment/football/events.parquet` based on [https://figshare.com/collections/Soccer_match_event_dataset/4415000/5](https://figshare.com/collections/Soccer_match_event_dataset/4415000/5) contains information about events in [football](https://en.wikipedia.org/wiki/Association_football) matches during the season 2017-18 in five European top-level leagues: English Premier League, Italian Serie A, Spanish La Liga, German Bundesliga, and French Ligue 1.
# MAGIC
# MAGIC #### Background information
# MAGIC
# MAGIC In the considered leagues, a season is played in a double round-robin format where each team plays against all other teams twice. Once as a home team in their own stadium and once as an away team in the other team's stadium. A season usually starts in August and ends in May.
# MAGIC
# MAGIC Each league match consists of two halves of 45 minutes each. Each half runs continuously, meaning that the clock is not stopped when the ball is out of play. The referee of the match may add some additional time to each half based on game stoppages. \[[https://en.wikipedia.org/wiki/Association_football#90-minute_ordinary_time](https://en.wikipedia.org/wiki/Association_football#90-minute_ordinary_time)\]
# MAGIC
# MAGIC The team that scores more goals than their opponent wins the match.
# MAGIC
# MAGIC **Columns in the data**
# MAGIC
# MAGIC Each row in the given data represents an event in a specific match. An event can be, for example, a pass, a foul, a shot, or a save attempt.
# MAGIC
# MAGIC Simple explanations for the available columns. Not all of these will be needed in this assignment.
# MAGIC
# MAGIC | column name | column type | description |
# MAGIC | ----------- | ----------- | ----------- |
# MAGIC | competition | string | The name of the competition |
# MAGIC | season | string | The season the match was played |
# MAGIC | matchId | integer | A unique id for the match |
# MAGIC | eventId | integer | A unique id for the event |
# MAGIC | homeTeam | string | The name of the home team |
# MAGIC | awayTeam | string | The name of the away team |
# MAGIC | event | string | The main category for the event |
# MAGIC | subEvent | string | The subcategory for the event |
# MAGIC | eventTeam | string | The name of the team that initiated the event |
# MAGIC | eventPlayerId | integer | The id for the player who initiated the event |
# MAGIC | eventPeriod | string | `1H` for events in the first half, `2H` for events in the second half |
# MAGIC | eventTime | double | The event time in seconds counted from the start of the half |
# MAGIC | tags | array of strings | The descriptions of the tags associated with the event |
# MAGIC | startPosition | struct | The event start position given in `x` and `y` coordinates in range \[0,100\] |
# MAGIC | enPosition | struct | The event end position given in `x` and `y` coordinates in range \[0,100\] |
# MAGIC
# MAGIC The used event categories can be seen from `assignment/football/metadata/eventid2name.csv`.<br>
# MAGIC And all available tag descriptions from `assignment/football/metadata/tags2name.csv`.<br>
# MAGIC You don't need to access these files in the assignment, but they can provide context for the following basic tasks that will use the event data.
# MAGIC
# MAGIC #### The task
# MAGIC
# MAGIC In this task you should load the data with all the rows into a data frame. This data frame object will then be used in the following basic tasks 3-8.

# COMMAND ----------

eventDF: DataFrame = spark.read  \
  .option("header", "true") \
  .option("sep", ",") \
  .option("inferSchema", "true") \
  .parquet("abfss://shared@tunics320f2024gen2.dfs.core.windows.net/assignment/football/events.parquet")
eventDF.printSchema()

eventDF = eventDF.select("eventId", "matchId", "competition", "season", "homeTeam", "awayTeam", "event" ,"eventTeam", "eventTime", "eventPeriod", "tags")
eventDF.cache()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 3 - Calculate match results
# MAGIC
# MAGIC Create a match data frame for all the matches included in the event data frame created in basic task 2.
# MAGIC
# MAGIC The resulting data frame should contain one row for each match and include the following columns:
# MAGIC
# MAGIC | column name   | column type | description |
# MAGIC | ------------- | ----------- | ----------- |
# MAGIC | matchId       | integer     | A unique id for the match |
# MAGIC | competition   | string      | The name of the competition |
# MAGIC | season        | string      | The season the match was played |
# MAGIC | homeTeam      | string      | The name of the home team |
# MAGIC | awayTeam      | string      | The name of the away team |
# MAGIC | homeTeamGoals | integer     | The number of goals scored by the home team |
# MAGIC | awayTeamGoals | integer     | The number of goals scored by the away team |
# MAGIC
# MAGIC The number of goals scored for each team should be determined by the available event data.<br>
# MAGIC There are two events related to each goal:
# MAGIC
# MAGIC - One event for the player that scored the goal. This includes possible own goals.
# MAGIC - One event for the goalkeeper that tried to stop the goal.
# MAGIC
# MAGIC You need to choose which types of events you are counting.<br>
# MAGIC If you count both of the event types mentioned above, you will get double the amount of actual goals.

# COMMAND ----------

homeGoalsDF = eventDF.filter(
    (array_contains(col("tags"), "Accurate") & array_contains(col("tags"), "Goal") & (col("eventTeam") == col("homeTeam"))) |
    (array_contains(col("tags"), "Own goal") & (col("eventTeam") == col("awayTeam")))
).groupBy("matchId").agg(count("eventId").alias("homeTeamGoals"))

awayGoalsDF = eventDF.filter(
    (array_contains(col("tags"), "Accurate") & array_contains(col("tags"), "Goal") & (col("eventTeam") == col("awayTeam"))) |
    (array_contains(col("tags"), "Own goal") & (col("eventTeam") == col("homeTeam")))
).groupBy("matchId").agg(count("eventId").alias("awayTeamGoals"))

matchDF = eventDF.select("matchId", "competition", "season", "homeTeam", "awayTeam").distinct() \
    .join(homeGoalsDF, "matchId", "left") \
    .join(awayGoalsDF, "matchId", "left") \
    .na.fill(0, subset=["homeTeamGoals", "awayTeamGoals"])

matchDF = matchDF.withColumn("totalGoals", col("homeTeamGoals") + col("awayTeamGoals"))

matchDF.cache()

# for testing purpose

# display(matchDF.filter(col("matchId").isin(2499806, 2499920, 2500056, 2500894, 2516792, 2516861, 2565711, 2576132)).select("matchId", "competition", "season", "homeTeam", "awayTeam", "homeTeamGoals", "awayTeamGoals").orderBy("matchId"))

# total_matches = matchDF.count()
# matches_without_goals = matchDF.filter(col("totalGoals") == 0).count()
# most_goals_in_single_game = matchDF.agg(max("totalGoals")).first()[0]
# total_goals = matchDF.agg(sum("totalGoals")).first()[0]

# print(f"Total number of matches: {total_matches}")
# print(f"Matches without any goals: {matches_without_goals}")
# print(f"Most goals in total in a single game: {most_goals_in_single_game}")
# print(f"Total amount of goals: {total_goals}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 4 - Calculate team points in a season
# MAGIC
# MAGIC Create a season data frame that uses the match data frame from the basic task 3 and contains aggregated seasonal results and statistics for all the teams in all leagues. While the used dataset only includes data from a single season for each league, the code should be written such that it would work even if the data would include matches from multiple seasons for each league.
# MAGIC
# MAGIC ###### Game result determination
# MAGIC
# MAGIC - Team wins the match if they score more goals than their opponent.
# MAGIC - The match is considered a draw if both teams score equal amount of goals.
# MAGIC - Team loses the match if they score fewer goals than their opponent.
# MAGIC
# MAGIC ###### Match point determination
# MAGIC
# MAGIC - The winning team gains 3 points from the match.
# MAGIC - Both teams gain 1 point from a drawn match.
# MAGIC - The losing team does not gain any points from the match.
# MAGIC
# MAGIC The resulting data frame should contain one row for each team per league and season. It should include the following columns:
# MAGIC
# MAGIC | column name    | column type | description |
# MAGIC | -------------- | ----------- | ----------- |
# MAGIC | competition    | string      | The name of the competition |
# MAGIC | season         | string      | The season |
# MAGIC | team           | string      | The name of the team |
# MAGIC | games          | integer     | The number of games the team played in the given season |
# MAGIC | wins           | integer     | The number of wins the team had in the given season |
# MAGIC | draws          | integer     | The number of draws the team had in the given season |
# MAGIC | losses         | integer     | The number of losses the team had in the given season |
# MAGIC | goalsScored    | integer     | The total number of goals the team scored in the given season |
# MAGIC | goalsConceded  | integer     | The total number of goals scored against the team in the given season |
# MAGIC | points         | integer     | The total number of points gained by the team in the given season |

# COMMAND ----------

matchDF = matchDF.withColumn("homeTeamResult", when(col("homeTeamGoals") > col("awayTeamGoals"), "win")
                                           .when(col("homeTeamGoals") < col("awayTeamGoals"), "loss")
                                           .otherwise("draw")) \
                 .withColumn("awayTeamResult", when(col("awayTeamGoals") > col("homeTeamGoals"), "win")
                                           .when(col("awayTeamGoals") < col("homeTeamGoals"), "loss")
                                           .otherwise("draw"))

matchDF = matchDF.withColumn("homeTeamPoints", when(col("homeTeamResult") == "win", 3)
                                             .when(col("homeTeamResult") == "draw", 1)
                                             .otherwise(0)) \
                 .withColumn("awayTeamPoints", when(col("awayTeamResult") == "win", 3)
                                             .when(col("awayTeamResult") == "draw", 1)
                                             .otherwise(0))

homeStatsDF = matchDF.groupBy("competition", "season", "homeTeam").agg(
    count("matchId").alias("games"),
    sum(when(col("homeTeamResult") == "win", 1).otherwise(0)).alias("wins"),
    sum(when(col("homeTeamResult") == "draw", 1).otherwise(0)).alias("draws"),
    sum(when(col("homeTeamResult") == "loss", 1).otherwise(0)).alias("losses"),
    sum("homeTeamGoals").alias("goalsScored"),
    sum("awayTeamGoals").alias("goalsConceded"),
    sum("homeTeamPoints").alias("points")
).withColumnRenamed("homeTeam", "team")

awayStatsDF = matchDF.groupBy("competition", "season", "awayTeam").agg(
    count("matchId").alias("games"),
    sum(when(col("awayTeamResult") == "win", 1).otherwise(0)).alias("wins"),
    sum(when(col("awayTeamResult") == "draw", 1).otherwise(0)).alias("draws"),
    sum(when(col("awayTeamResult") == "loss", 1).otherwise(0)).alias("losses"),
    sum("awayTeamGoals").alias("goalsScored"),
    sum("homeTeamGoals").alias("goalsConceded"),
    sum("awayTeamPoints").alias("points")
).withColumnRenamed("awayTeam", "team")

seasonDF = homeStatsDF.unionByName(awayStatsDF).groupBy("competition", "season", "team").agg(
    sum("games").alias("games"),
    sum("wins").alias("wins"),
    sum("draws").alias("draws"),
    sum("losses").alias("losses"),
    sum("goalsScored").alias("goalsScored"),
    sum("goalsConceded").alias("goalsConceded"),
    sum("points").alias("points")
)

seasonDF.cache()

# for testing purpose

# display(seasonDF.filter(col("team").isin("Lille", "Getafe", "Torino", "Arsenal", "Schalke 04", "Burnley")))

# total_rows = seasonDF.count()
# teams_above_70_points = seasonDF.filter(col("points") > 70).count()
# lowest_points = seasonDF.agg(min("points")).first()[0]
# total_points = seasonDF.agg(sum("points")).first()[0]
# total_goals_scored = seasonDF.agg(sum("goalsScored")).first()[0]
# total_goals_conceded = seasonDF.agg(sum("goalsConceded")).first()[0]

# print(f"Total number of rows: {total_rows}")
# print(f"Teams with more than 70 points in a season: {teams_above_70_points}")
# print(f"Lowest amount points in a season: {lowest_points}")
# print(f"Total amount of points: {total_points}")
# print(f"Total amount of goals scored: {total_goals_scored}")
# print(f"Total amount of goals conceded: {total_goals_conceded}")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 5 - English Premier League table
# MAGIC
# MAGIC Using the season data frame from basic task 4 calculate the final league table for `English Premier League` in season `2017-2018`.
# MAGIC
# MAGIC The result should be given as data frame which is ordered by the team's classification for the season.
# MAGIC
# MAGIC A team is classified higher than the other team if one of the following is true:
# MAGIC
# MAGIC - The team has a higher number of total points than the other team
# MAGIC - The team has an equal number of points, but have a better goal difference than the other team
# MAGIC - The team has an equal number of points and goal difference, but have more goals scored in total than the other team
# MAGIC
# MAGIC Goal difference is the difference between the number of goals scored for and against the team.
# MAGIC
# MAGIC The resulting data frame should contain one row for each team.<br>
# MAGIC It should include the following columns (several columns renamed trying to match the [league table in Wikipedia](https://en.wikipedia.org/wiki/2017%E2%80%9318_Premier_League#League_table)):
# MAGIC
# MAGIC | column name | column type | description |
# MAGIC | ----------- | ----------- | ----------- |
# MAGIC | Pos         | integer     | The classification of the team |
# MAGIC | Team        | string      | The name of the team |
# MAGIC | Pld         | integer     | The number of games played |
# MAGIC | W           | integer     | The number of wins |
# MAGIC | D           | integer     | The number of draws |
# MAGIC | L           | integer     | The number of losses |
# MAGIC | GF          | integer     | The total number of goals scored by the team |
# MAGIC | GA          | integer     | The total number of goals scored against the team |
# MAGIC | GD          | string      | The goal difference |
# MAGIC | Pts         | integer     | The total number of points gained by the team |
# MAGIC
# MAGIC The goal difference should be given as a string with an added `+` at the beginning if the difference is positive, similarly to the table in the linked Wikipedia article.

# COMMAND ----------

englandDF = seasonDF.filter(
    (col("competition") == "English Premier League") & 
    (col("season") == "2017-2018")
)

englandDF = englandDF.withColumn(
    "GD", 
    concat(
        when((col("goalsScored") - col("goalsConceded")) >= 0, lit("+")).otherwise(lit("")),
        (col("goalsScored") - col("goalsConceded")).cast("string")
    )
)

englandDF = englandDF.withColumn("Pos", row_number().over(
    Window.orderBy(
        col("points").desc(), 
        (col("goalsScored") - col("goalsConceded")).desc(), 
        col("goalsScored").desc()
    )
))

englandDF = englandDF.select(
    col("Pos"),
    col("team").alias("Team"),
    col("games").alias("Pld"),
    col("wins").alias("W"),
    col("draws").alias("D"),
    col("losses").alias("L"),
    col("goalsScored").alias("GF"),
    col("goalsConceded").alias("GA"),
    col("GD"),
    col("points").alias("Pts")
)

print("English Premier League table for season 2017-2018")
englandDF.show(20, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic task 6: Calculate the number of passes
# MAGIC
# MAGIC This task involves going back to the event data frame and counting the number of passes each team made in each match. A pass is considered successful if it is marked as `Accurate`.
# MAGIC
# MAGIC Using the event data frame from basic task 2, calculate the total number of passes as well as the total number of successful passes for each team in each match.<br>
# MAGIC The resulting data frame should contain one row for each team in each match, i.e., two rows for each match. It should include the following columns:
# MAGIC
# MAGIC | column name | column type | description |
# MAGIC | ----------- | ----------- | ----------- |
# MAGIC | matchId     | integer     | A unique id for the match |
# MAGIC | competition | string      | The name of the competition |
# MAGIC | season      | string      | The season |
# MAGIC | team        | string      | The name of the team |
# MAGIC | totalPasses | integer     | The total number of passes the team attempted in the match |
# MAGIC | successfulPasses | integer | The total number of successful passes made by the team in the match |
# MAGIC
# MAGIC You can assume that each team had at least one pass attempt in each match they played.

# COMMAND ----------

passEventsDF = eventDF.filter(col("event") == "Pass")

matchPassDF = passEventsDF.groupBy("matchId","eventTeam", "competition", "season").agg(
     count(when(array_contains(col("tags"), "Accurate"), True)).alias("successfulPasses"),
     count("eventId").alias("totalPasses")
).withColumnRenamed("eventTeam", "team")

matchPassDF.cache()

# for testing purpose
# totalRows = matchPassDF.count()

# moreThan700TotalPasses = matchPassDF.filter(col("totalPasses") > 700).count()

# moreThan600SuccessfulPasses = matchPassDF.filter(col("successfulPasses") > 600).count()

# print(f"Total number of rows: {totalRows}")
# print(f"Team-match pairs with more than 700 total passes: {moreThan700TotalPasses}")
# print(f"Team-match pairs with more than 600 successful passes: {moreThan600SuccessfulPasses}")

# display(matchPassDF)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 7: Teams with the worst passes
# MAGIC
# MAGIC Using the match pass data frame from basic task 6 find the teams with the lowest average ratio for successful passes over the season `2017-2018` for each league.
# MAGIC
# MAGIC The ratio for successful passes over a single match is the number of successful passes divided by the number of total passes.<br>
# MAGIC The average ratio over the season is the average of the single match ratios.
# MAGIC
# MAGIC Give the result as a data frame that has one row for each league-team pair with the following columns:
# MAGIC
# MAGIC | column name | column type | description |
# MAGIC | ----------- | ----------- | ----------- |
# MAGIC | competition | string      | The name of the competition |
# MAGIC | team        | string      | The name of the team |
# MAGIC | passSuccessRatio | double | The average ratio for successful passes over the season given as percentages rounded to two decimals |
# MAGIC
# MAGIC Order the data frame so that the team with the lowest ratio for passes is given first.

# COMMAND ----------

seasonPassDF = matchPassDF.filter(col("season") == "2017-2018")

seasonPassDF = seasonPassDF.withColumn("passSuccessRatio", col("successfulPasses") / col("totalPasses"))

avgPassSuccessRatioDF = seasonPassDF.groupBy("competition", "team").agg(
    round(avg("passSuccessRatio") * 100, 2).alias("passSuccessRatio")
)

windowSpec = Window.partitionBy("competition").orderBy(col("passSuccessRatio").asc())

rankedDF = avgPassSuccessRatioDF.withColumn("rank", row_number().over(windowSpec))

lowestPassSuccessRatioDF = rankedDF.filter(col("rank") == 1).drop("rank")

sortedLowestPassSuccessRatioDF = lowestPassSuccessRatioDF.orderBy(col("passSuccessRatio").asc())

print("The teams with the lowest ratios for successful passes for each league in season 2017-2018:")
sortedLowestPassSuccessRatioDF.show(5, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic task 8: The best teams
# MAGIC
# MAGIC For this task the best teams are determined by having the highest point average per match.
# MAGIC
# MAGIC Using the data frames created in the previous tasks find the two best teams from each league in season `2017-2018` with their full statistics.
# MAGIC
# MAGIC Give the result as a data frame with the following columns:
# MAGIC
# MAGIC | column name | column type | description |
# MAGIC | ----------- | ----------- | ----------- |
# MAGIC | Team        | string      | The name of the team |
# MAGIC | League      | string      | The name of the league |
# MAGIC | Pos         | integer     | The classification of the team within their league |
# MAGIC | Pld         | integer     | The number of games played |
# MAGIC | W           | integer     | The number of wins |
# MAGIC | D           | integer     | The number of draws |
# MAGIC | L           | integer     | The number of losses |
# MAGIC | GF          | integer     | The total number of goals scored by the team |
# MAGIC | GA          | integer     | The total number of goals scored against the team |
# MAGIC | GD          | string      | The goal difference |
# MAGIC | Pts         | integer     | The total number of points gained by the team |
# MAGIC | Avg         | double      | The average points per match gained by the team |
# MAGIC | PassRatio   | double      | The average ratio for successful passes over the season given as percentages rounded to two decimals |
# MAGIC
# MAGIC Order the data frame so that the team with the highest point average per match is given first.

# COMMAND ----------

seasonStatsDF = seasonDF.filter(col("season") == "2017-2018")

seasonStatsDF = seasonStatsDF.join(broadcast(avgPassSuccessRatioDF), on=["competition", "team"], how="inner")

seasonStatsDF = seasonStatsDF.withColumn("Avg", round(col("points") / col("games"), 2))

windowSpec = Window.partitionBy("competition").orderBy(col("points").desc())

rankedStatsDF = seasonStatsDF.withColumn("Pos", row_number().over(windowSpec))

top2TeamsDF = rankedStatsDF.filter(col("Pos") <= 2)

bestDF = top2TeamsDF.select(
    col("team").alias("Team"),
    col("competition"),
    col("Pos"),
    col("games").alias("Pld"),
    col("wins").alias("W"),
    col("draws").alias("D"),
    col("losses").alias("L"),
    col("goalsScored").alias("GF"),
    col("goalsConceded").alias("GA"),
    concat(lit("+"), col("points")).alias("Pts"),
    col("Avg"),
    col("passSuccessRatio").alias("PassRatio")
).orderBy(col("Avg").desc())

bestDF.show(10, False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Basic Task 9: General information
# MAGIC
# MAGIC Answer **briefly** to the following questions.
# MAGIC
# MAGIC Remember that using AI and collaborating with other students outside your group is allowed as long as the usage and collaboration is documented.<br>
# MAGIC However, every member of the group should have some contribution to the assignment work.
# MAGIC
# MAGIC - Who were your group members and their contributions to the work?
# MAGIC     - Solo groups can ignore this question.
# MAGIC
# MAGIC - Did you use AI tools while doing the assignment?
# MAGIC     - Which ones and how did they help?
# MAGIC
# MAGIC - Did you work with students outside your assignment group?
# MAGIC     - Who or which group? (only extensive collaboration need to reported)

# COMMAND ----------

# MAGIC %md
# MAGIC Our group members are:
# MAGIC - Khanh Pham (minhkhanh.pham@tuni.fi)
# MAGIC - Chi Mai (chi.mai@tuni.fi)
# MAGIC - Dat Minh Lam (datminh.lam@tuni.fi)
# MAGIC
# MAGIC For the basic tasks, we sat in a room and did all the tasks together. It took us around 4 hours to complete them.
# MAGIC
# MAGIC While doing the assignment, we used Databricks AI assistant to help us diagnose the errors and search for syntax.
# MAGIC
# MAGIC We did not work with any students outside our group.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced tasks
# MAGIC
# MAGIC The implementation of the basic tasks is compulsory for every group.
# MAGIC
# MAGIC Doing the following advanced tasks you can gain course points which can help in getting a better grade from the course.<br>
# MAGIC Partial solutions can give partial points.
# MAGIC
# MAGIC The advanced task 1 will be considered in the grading for every group based on their solutions for the basic tasks.
# MAGIC
# MAGIC The advanced tasks 2, 3, and 4 are separate tasks. The solutions used in these other advanced tasks do not affect the grading of advanced task 1. Instead, a good use of optimized methods can positively influence the grading of each specific task, while very non-optimized solutions can have a negative effect on the task grade.
# MAGIC
# MAGIC While you can attempt all three tasks (advanced tasks 2-4), only two of them will be graded and contribute towards the course grade.<br>
# MAGIC Mark in the following cell which tasks you want to be graded and which should be ignored.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### If you did the advanced tasks 2-4, mark here which of the two should be considered in grading:
# MAGIC
# MAGIC - Advanced task 2 should be graded: Yes
# MAGIC - Advanced task 3 should be graded: No
# MAGIC - Advanced task 4 should be graded: Yes

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced Task 1 - Optimized and correct solutions to the basic tasks (2 points)
# MAGIC
# MAGIC Use the tools Spark offers effectively and avoid unnecessary operations in the code for the basic tasks.
# MAGIC
# MAGIC A couple of things to consider (**not** even close to a complete list):
# MAGIC
# MAGIC - Consider using explicit schemas when dealing with CSV data sources.
# MAGIC - Consider only including those columns from a data source that are actually needed.
# MAGIC - Filter unnecessary rows whenever possible to get smaller datasets.
# MAGIC - Avoid collect or similar expensive operations for large datasets.
# MAGIC - Consider using explicit caching if some data frame is used repeatedly.
# MAGIC - Avoid unnecessary shuffling (for example sorting) operations.
# MAGIC - Avoid unnecessary actions (count, etc.) that are not needed for the task.
# MAGIC
# MAGIC In addition to the effectiveness of your solutions, the correctness of the solution logic will be taken into account when determining the grade for this advanced task 1.
# MAGIC "A close enough" solution with some logic fails might be enough to have an accepted group assignment, but those failings might lower the score for this task.
# MAGIC
# MAGIC It is okay to have your own test code that would fall into category of "ineffective usage" or "unnecessary operations" while doing the assignment tasks. However, for the final Moodle submission you should comment out or delete such code (and test that you have not broken anything when doing the final modifications).
# MAGIC
# MAGIC Note, that you should not do the basic tasks again for this additional task, but instead modify your basic task code with more efficient versions.
# MAGIC
# MAGIC You can create a text cell below this one and describe what optimizations you have done. This might help the grader to better recognize how skilled your work with the basic tasks has been.

# COMMAND ----------

# MAGIC %md
# MAGIC The optimizations focus on the following improvements:
# MAGIC
# MAGIC - Used more concise filtering and transformation logic to reduce redundancy and improve readability.
# MAGIC - Applied column selection (select) at the earliest stages to avoid carrying unnecessary data.
# MAGIC - Leveraged Spark's Window functionality and caching strategically to improve computation speed, particularly for ranking and aggregation tasks.
# MAGIC - Refactored column computations like goal differences and pass ratios to avoid repetitive expressions and improve clarity.
# MAGIC - Frequent use of .cache() in intermediate steps ensures that Spark does not recompute expensive transformations multiple times.
# MAGIC - Reorganized and documented transformations to make the logic clearer and easier to understand.
# MAGIC - Used consistent naming conventions for variables and data frames, which simplifies debugging.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced Task 2 - Further tasks with football data (2 points)
# MAGIC
# MAGIC This advanced task continues with football event data from the basic tasks. In addition, there are two further related datasets that are used in this task.
# MAGIC
# MAGIC A Parquet file at folder `assignment/football/matches.parquet` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2024-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2024gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) contains information about which players were involved on each match including information on the substitutions made during the match.
# MAGIC
# MAGIC Another Parquet file at folder `assignment/football/players.parquet` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2024-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2024gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) contains information about the player names, default roles when playing, and their birth areas.
# MAGIC
# MAGIC #### Columns in the additional data
# MAGIC
# MAGIC The match dataset (`assignment/football/matches.parquet`) has one row for each match and each row has the following columns:
# MAGIC
# MAGIC | column name  | column type | description |
# MAGIC | ------------ | ----------- | ----------- |
# MAGIC | matchId      | integer     | A unique id for the match |
# MAGIC | competition  | string      | The name of the league |
# MAGIC | season       | string      | The season the match was played |
# MAGIC | roundId      | integer     | A unique id for the round in the competition |
# MAGIC | gameWeek     | integer     | The gameWeek of the match |
# MAGIC | date         | date        | The date the match was played |
# MAGIC | status       | string      | The status of the match, `Played` if the match has been played |
# MAGIC | homeTeamData | struct      | The home team data, see the table below for the attributes in the struct |
# MAGIC | awayTeamData | struct      | The away team data, see the table below for the attributes in the struct |
# MAGIC | referees     | struct      | The referees for the match |
# MAGIC
# MAGIC Both team data columns have the following inner structure:
# MAGIC
# MAGIC | column name  | column type | description |
# MAGIC | ------------ | ----------- | ----------- |
# MAGIC | team         | string      | The name of the team |
# MAGIC | coachId      | integer     | A unique id for the coach of the team |
# MAGIC | lineup       | array of integers | A list of the player ids who start the match on the field for the team |
# MAGIC | bench        | array of integers | A list of the player ids who start the match on the bench, i.e., the reserve players for the team |
# MAGIC | substitution1 | struct     | The first substitution the team made in the match, see the table below for the attributes in the struct |
# MAGIC | substitution2 | struct     | The second substitution the team made in the match, see the table below for the attributes in the struct |
# MAGIC | substitution3 | struct     | The third substitution the team made in the match, see the table below for the attributes in the struct |
# MAGIC
# MAGIC Each substitution structs have the following inner structure:
# MAGIC | column name  | column type | description |
# MAGIC | ------------ | ----------- | ----------- |
# MAGIC | playerIn     | integer     | The id for the player who was substituted from the bench into the field, i.e., this player started playing after this substitution |
# MAGIC | playerOut    | integer     | The id for the player who was substituted from the field to the bench, i.e., this player stopped playing after this substitution |
# MAGIC | minute       | integer     | The minute from the start of the match the substitution was made.<br>Values of 45 or less indicate that the substitution was made in the first half of the match,<br>and values larger than 45 indicate that the substitution was made on the second half of the match. |
# MAGIC
# MAGIC The player dataset (`assignment/football/players.parquet`) has the following columns:
# MAGIC
# MAGIC | column name  | column type | description |
# MAGIC | ------------ | ----------- | ----------- |
# MAGIC | playerId     | integer     | A unique id for the player |
# MAGIC | firstName    | string      | The first name of the player |
# MAGIC | lastName     | string      | The last name of the player |
# MAGIC | birthArea    | string      | The birth area (nation or similar) of the player |
# MAGIC | role         | string      | The main role of the player, either `Goalkeeper`, `Defender`, `Midfielder`, or `Forward` |
# MAGIC | foot         | string      | The stronger foot of the player |
# MAGIC
# MAGIC #### Background information
# MAGIC
# MAGIC In a football match both teams have 11 players on the playing field or pitch at the start of the match. Each team also have some number of reserve players on the bench at the start of the match. The teams can make up to three substitution during the match where they switch one of the players on the field to a reserve player. (Currently, more substitutions are allowed, but at the time when the data is from, three substitutions were the maximum.) Any player starting the match as a reserve and who is not substituted to the field during the match does not play any minutes and are not considered involved in the match.
# MAGIC
# MAGIC For this task the length of each match should be estimated with the following procedure:
# MAGIC
# MAGIC - Only the additional time added to the second half of the match should be considered. I.e., the length of the first half is always considered to be 45 minutes.
# MAGIC - The length of the second half is to be considered as the last event of the half rounded upwards towards the nearest minute.
# MAGIC     - I.e., if the last event of the second half happens at 2845 seconds (=47.4 minutes) from the start of the half, the length of the half should be considered as 48 minutes. And thus, the full length of the entire match as 93 minutes.
# MAGIC
# MAGIC A personal plus-minus statistics for each player can be calculated using the following information:
# MAGIC
# MAGIC - If a goal was scored by the player's team when the player was on the field, `add 1`
# MAGIC - If a goal was scored by the opponent's team when the player was on the field, `subtract 1`
# MAGIC - If a goal was scored when the player was a reserve on the bench, `no change`
# MAGIC - For any event that is not a goal, or is in a match that the player was not involved in, `no change`
# MAGIC - Any substitutions is considered to be done at the start of the given minute.
# MAGIC     - I.e., if the player is substituted from the bench to the field at minute 80 (minute 35 on the second half), they were considered to be on the pitch from second 2100.0 on the 2nd half of the match.
# MAGIC - If a goal was scored in the additional time of the first half of the match, i.e., the goal event period is `1H` and event time is larger than 2700 seconds, some extra considerations should be taken into account:
# MAGIC     - If a player is substituted into the field at the beginning of the second half, `no change`
# MAGIC     - If a player is substituted off the field at the beginning of the second half, either `add 1` or `subtract 1` depending on team that scored the goal
# MAGIC     - Any player who is substituted into the field at minute 45 or later is only playing on the second half of the match.
# MAGIC     - Any player who is substituted off the field at minute 45 or later is considered to be playing the entire first half including the additional time.
# MAGIC
# MAGIC ### Tasks
# MAGIC
# MAGIC The target of the task is to use the football event data and the additional datasets to determine the following:
# MAGIC
# MAGIC - The players with the most total minutes played in season 2017-2018 for each player role
# MAGIC     - I.e., the player in Goalkeeper role who has played the longest time across all included leagues. And the same for the other player roles (Defender, Midfielder, and Forward)
# MAGIC     - Give the result as a data frame that has the following columns:
# MAGIC         - `role`: the player role
# MAGIC         - `player`: the full name of the player, i.e., the first name combined with the last name
# MAGIC         - `birthArea`: the birth area of the player
# MAGIC         - `minutes`: the total minutes the player played during season 2017-2018
# MAGIC - The players with higher than `+65` for the total plus-minus statistics in season 2017-2018
# MAGIC     - Give the result as a data frame that has the following columns:
# MAGIC         - `player`: the full name of the player, i.e., the first name combined with the last name
# MAGIC         - `birthArea`: the birth area of the player
# MAGIC         - `role`: the player role
# MAGIC         - `plusMinus`: the total plus-minus statistics for the player during season 2017-2018
# MAGIC
# MAGIC It is advisable to work towards the target results using several intermediate steps.

# COMMAND ----------

# Read the data
matchesDF = spark.read.parquet("abfss://shared@tunics320f2024gen2.dfs.core.windows.net/assignment/football/matches.parquet")
playersDF = spark.read.parquet("abfss://shared@tunics320f2024gen2.dfs.core.windows.net/assignment/football/players.parquet")

# Filter for second half events and calculate the match length
secondHalfEventsDF = eventDF.filter(col("eventPeriod") == "2H")
lastEventTimeDF = secondHalfEventsDF.groupBy("matchId").agg(max("eventTime").alias("lastEventTime"))
matchLengthsDF = lastEventTimeDF.withColumn("matchLength", 45 + ceil(col("lastEventTime") / 60))

# Join matchLengthsDF with matchesDF to include matchLength column
matchesWithLengthDF = matchesDF.join(matchLengthsDF, "matchId", "left")

# Explode the lineup arrays to get individual player records
def get_players_df(team_col):
    return matchesWithLengthDF.select(
        col("matchId"),
        col("competition"),
        col("season"),
        col(f"{team_col}.team").alias("playerTeam"),
        col(f"{team_col}.lineup").alias("lineup"),
        col(f"{team_col}.bench").alias("bench"),
        col(f"{team_col}.substitution1"),
        col(f"{team_col}.substitution2"),
        col(f"{team_col}.substitution3"),
        col("matchLength")
    ).withColumn("playerId", explode(col("lineup")))

homePlayersDF = get_players_df("homeTeamData")
awayPlayersDF = get_players_df("awayTeamData")

# Combine home and away players
allPlayersDF = homePlayersDF.union(awayPlayersDF)

# Calculate start and end minutes for each player
allPlayersDF = allPlayersDF.withColumn("startMinute", lit(0)).withColumn("endMinute", col("matchLength"))

# Adjust end minutes based on substitutions
for i in range(1, 4):
    allPlayersDF = allPlayersDF.withColumn(
        "endMinute",
        when(col(f"substitution{i}.playerOut") == col("playerId"), col(f"substitution{i}.minute")).otherwise(col("endMinute"))
    )

# Calculate minutes played for starting players
allPlayersDF = allPlayersDF.withColumn("minutes", col("endMinute") - col("startMinute"))
# Calculate minutes played for substituted-in players
def get_substituted_in_df(team_col):
    return matchesWithLengthDF.select(
        col("matchId"),
        col("competition"),
        col("season"),
        col(f"{team_col}.team").alias("playerTeam"),
        col(f"{team_col}.substitution1.playerIn").alias("playerId"),
        col(f"{team_col}.substitution1.minute").alias("startMinute"),
        col("matchLength")
    ).union(
        matchesWithLengthDF.select(
            col("matchId"),
            col("competition"),
            col("season"),
            col(f"{team_col}.team").alias("playerTeam"),
            col(f"{team_col}.substitution2.playerIn").alias("playerId"),
            col(f"{team_col}.substitution2.minute").alias("startMinute"),
            col("matchLength")
        )
    ).union(
        matchesWithLengthDF.select(
            col("matchId"),
            col("competition"),
            col("season"),
            col(f"{team_col}.team").alias("playerTeam"),
            col(f"{team_col}.substitution3.playerIn").alias("playerId"),
            col(f"{team_col}.substitution3.minute").alias("startMinute"),
            col("matchLength")
        )
    )

homeSubstitutedInDF = get_substituted_in_df("homeTeamData")
awaySubstitutedInDF = get_substituted_in_df("awayTeamData")

substitutedInDF = homeSubstitutedInDF.union(awaySubstitutedInDF)

substitutedInDF = substitutedInDF.withColumn("endMinute", col("matchLength"))
substitutedInDF = substitutedInDF.withColumn("minutes", col("endMinute") - col("startMinute"))

# Ensure both DataFrames have the same columns before union
substitutedInDF = substitutedInDF.select("matchId", "playerId", "competition", "season", "playerTeam", "startMinute", "endMinute", "minutes")

# Combine the minutes played by starting players and substituted-in players
allPlayersDF = allPlayersDF.select("matchId", "playerId", "competition", "season", "playerTeam", "startMinute", "endMinute", "minutes").union(substitutedInDF)

# Select relevant columns
playersTimeOnPitchDF = allPlayersDF.select(
    "matchId", "playerId", "competition", "season", "playerTeam", "startMinute", "endMinute", "minutes"
)

# Calculate total minutes played for each player
totalMinutesDF = playersTimeOnPitchDF.groupBy("playerId").agg(sum("minutes").alias("minutes"))

# Join with players data to get player details
playerMinutesDF = totalMinutesDF.join(playersDF, "playerId").select(
    col("role"),
    concat_ws(" ", col("firstName"), col("lastName")).alias("player"),
    col("birthArea"),
    col("minutes")
)

# Get the player with the most minutes played for each role
windowSpec = Window.partitionBy("role").orderBy(col("minutes").desc())

mostMinutesDF = playerMinutesDF.withColumn("rank", row_number().over(windowSpec)).filter(col("rank") == 1).drop("rank")

# Sort by minutes in descending order
mostMinutesDF = mostMinutesDF.orderBy(col("minutes").desc())

# Display the result
mostMinutesDF.show(truncate=False)

# COMMAND ----------

from pyspark.sql.functions import array_contains, when, col, lit, expr, concat

goalEventsDF = eventDF.filter(
    array_contains(col("tags"), "Goal") & array_contains(col("tags"), "Accurate") |
    array_contains(col("tags"), "Own goal") 
).select(
    "eventId", "matchId", "eventTime", "eventPeriod", "eventTeam", "homeTeam", "awayTeam", "tags"
).withColumn(
    "goalTeam",
    when(
        array_contains(col("tags"), "Own goal"),
        # If "Own goal", flip to the opposing team
        when(col("eventTeam") == col("homeTeam"), col("awayTeam")).otherwise(col("homeTeam"))
    ).otherwise(
        # Otherwise, retain the event team as the goalTeam
        col("eventTeam")
    )
)
goalEventsDF = goalEventsDF.withColumn("eventTime", (col("eventTime") / 60).cast("double"))
goalImpactDF = goalEventsDF.join(playersTimeOnPitchDF, "matchId").filter(
    (
        (col("eventPeriod") == "1H") &
        (col("eventTime") >= col("startMinute")) &
        (col("eventTime") <= col("endMinute"))
    ) |
    (
        (col("eventPeriod") == "2H") &
        ((col("eventTime") + 45) >= col("startMinute")) &
        ((col("eventTime") + 45) <= col("endMinute"))
    )
)
goalImpactDF = goalImpactDF.withColumn(
    "playerPlusMinus",
    when(col("goalTeam") == col("playerTeam"), 1).otherwise(-1)
)
# Aggregate total plus-minus per player for season 2017-2018
plusMinusDF = goalImpactDF.groupBy("playerId", "season").agg(
    sum("playerPlusMinus").alias("plusMinus")
).filter(col("season") == "2017-2018")

# Join with playersDF to get player details
topPlayersDF = plusMinusDF.join(playersDF, "playerId").filter(
    col("plusMinus") > 65
).select(
    concat_ws(" ", col("firstName"), col("lastName")).alias("player"),
    col("birthArea"),
    col("role"),
    col("plusMinus")
).orderBy(col("plusMinus").desc())

topPlayersDF.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced Task 3 - Image data and pixel colors (2 points)
# MAGIC
# MAGIC This advanced task involves loading in PNG image data and complementing JSON metadata into Spark data structure. And then determining the colors of the pixels in the images, and finding the answers to several color related questions.
# MAGIC
# MAGIC The folder `assignment/openmoji/color` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2024-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2024gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) contains collection of PNG images from [OpenMoji](https://openmoji.org/) project.
# MAGIC
# MAGIC The JSON Lines formatted file `assignment/openmoji/openmoji.jsonl` contains metadata about the image collection. Only a portion of the images are included as source data for this task, so the metadata file contains also information about images not considered in this task.
# MAGIC
# MAGIC #### Data description and helper functions
# MAGIC
# MAGIC The image data considered in this task can be loaded into a Spark data frame using the `image` format: [https://spark.apache.org/docs/3.5.0/ml-datasource.html](https://spark.apache.org/docs/3.5.0/ml-datasource.html). The resulting data frame contains a single column which includes information about the filename, image size as well as the binary data representing the image itself. The Spark documentation page contains more detailed information about the structure of the column.
# MAGIC
# MAGIC Instead of using the images as source data for machine learning tasks, the binary image data is accessed directly in this task.<br>
# MAGIC You are given two helper functions to help in dealing with the binary data:
# MAGIC
# MAGIC - Function `toPixels` takes in the binary image data and the number channels used to represent each pixel.
# MAGIC     - In the case of the images used in this task, the number of channels match the number bytes used for each pixel.
# MAGIC     - As output the function returns an array of strings where each string is hexadecimal representation of a single pixel in the image.
# MAGIC - Function `toColorName` takes in a single pixel represented as hexadecimal string.
# MAGIC     - As output the function returns a string with the name of the basic color that most closely represents the pixel.
# MAGIC     - The function uses somewhat naive algorithm to determine the name of the color, and does not always give correct results.
# MAGIC     - Many of the pixels in this task have a lot of transparent pixels. Any such pixel is marked as the color `None` by the function.
# MAGIC
# MAGIC With the help of the given functions it is possible to transform the binary image data to an array of color names without using additional libraries or knowing much about image processing.
# MAGIC
# MAGIC The metadata file given in JSON Lines format can be loaded into a Spark data frame using the `json` format: [https://spark.apache.org/docs/3.5.0/sql-data-sources-json.html](https://spark.apache.org/docs/3.5.0/sql-data-sources-json.html). The attributes used in the JSON data are not described here, but are left for you to explore. The original regular JSON formatted file can be found at [https://github.com/hfg-gmuend/openmoji/blob/master/data/openmoji.json](https://github.com/hfg-gmuend/openmoji/blob/master/data/openmoji.json).
# MAGIC
# MAGIC ### Tasks
# MAGIC
# MAGIC The target of the task is to combine the image data with the JSON data, determine the image pixel colors, and the find the answers to the following questions:
# MAGIC
# MAGIC - Which four images have the most colored non-transparent pixels?
# MAGIC - Which five images have the lowest ratio of colored vs. transparent pixels?
# MAGIC - What are the three most common colors in the Finnish flag image (annotation: `flag: Finland`)?
# MAGIC     - And how many percentages of the colored pixels does each color have?
# MAGIC - How many images have their most common three colors as, `Blue`-`Yellow`-`Black`, in that order?
# MAGIC - Which five images have the most red pixels among the image group `activities`?
# MAGIC     - And how many red pixels do each of these images have?
# MAGIC
# MAGIC It might be advisable to test your work-in-progress code with a limited number of images before using the full image set.<br>
# MAGIC You are free to choose your own approach to the task: user defined functions with data frames, RDDs/Datasets, or combination of both.
# MAGIC
# MAGIC Note that the currently the Python helper functions do not exactly match the Scala versions, and thus the answers to the questions might not quite match the given example results in the example output notebook.

# COMMAND ----------

# separates binary image data to an array of hex strings that represent the pixels
# assumes 8-bit representation for each pixel (0x00 - 0xff)
# with `channels` attribute representing how many bytes is used for each pixel
def toPixels(data: bytes, channels: int) -> List[str]:
    return [
        "".join([
            f"{data[index+byte]:02X}"
            for byte in range(0, channels)
        ])
        for index in range(0, len(data), channels)
    ]

# COMMAND ----------

# naive implementation of picking the name of the pixel color based on the input hex representation of the pixel
# only works for OpenCV type CV_8U (mode=24) compatible input
def toColorName(hexString: str) -> str:
    # mapping of RGB values to basic color names
    colors: Dict[Tuple[int, int, int], str] = {
        (0, 0, 0):     "Black",  (0, 0, 128):     "Blue",   (0, 0, 255):     "Blue",
        (0, 128, 0):   "Green",  (0, 128, 128):   "Green",  (0, 128, 255):   "Blue",
        (0, 255, 0):   "Green",  (0, 255, 128):   "Green",  (0, 255, 255):   "Blue",
        (128, 0, 0):   "Red",    (128, 0, 128):   "Purple", (128, 0, 255):   "Purple",
        (128, 128, 0): "Green",  (128, 128, 128): "Gray",   (128, 128, 255): "Purple",
        (128, 255, 0): "Green",  (128, 255, 128): "Green",  (128, 255, 255): "Blue",
        (255, 0, 0):   "Red",    (255, 0, 128):   "Pink",   (255, 0, 255):   "Purple",
        (255, 128, 0): "Orange", (255, 128, 128): "Orange", (255, 128, 255): "Pink",
        (255, 255, 0): "Yellow", (255, 255, 128): "Yellow", (255, 255, 255): "White"
    }

    # helper function to round values of 0-255 to the nearest of 0, 128, or 255
    def roundColorValue(value: int) -> int:
        if value < 85:
            return 0
        if value < 170:
            return 128
        return 255

    validString: bool = re.match(r"[0-9a-fA-F]{8}", hexString) is not None
    if validString:
        # for OpenCV type CV_8U (mode=24) the expected order of bytes is BGRA
        blue: int = roundColorValue(int(hexString[0:2], 16))
        green: int = roundColorValue(int(hexString[2:4], 16))
        red: int = roundColorValue(int(hexString[4:6], 16))
        alpha: int = int(hexString[6:8], 16)

        if alpha < 128:
            return "None"  # any pixel with less than 50% opacity is considered as color "None"
        return colors[(red, green, blue)]

    return "None"  # any input that is not in valid format is considered as color "None"

# COMMAND ----------

# The annotations for the four images with the most colored non-transparent pixels
mostColoredPixels: List[str] = ???

print("The annotations for the four images with the most colored non-transparent pixels:")
for image in mostColoredPixels:
    print(f"- {image}")
print("============================================================")


# The annotations for the five images having the lowest ratio of colored vs. transparent pixels
leastColoredPixels: List[str] = ???

print("The annotations for the five images having the lowest ratio of colored vs. transparent pixels:")
for image in leastColoredPixels:
    print(f"- {image}")

# COMMAND ----------

# The three most common colors in the Finnish flag image:
finnishFlagColors: List[str] = ???

# The percentages of the colored pixels for each common color in the Finnish flag image:
finnishColorShares: List[float] = ???

print("The colors and their percentage shares in the image for the Finnish flag:")
for color, share in zip(finnishFlagColors, finnishColorShares):
    print(f"- color: {color}, share: {share}")
print("============================================================")


# The number of images that have their most common three colors as, Blue-Yellow-Black, in that exact order:
blueYellowBlackCount: int = ???

print(f"The number of images that have, Blue-Yellow-Black, as the most common colors: {blueYellowBlackCount}")

# COMMAND ----------

# The annotations for the five images with the most red pixels among the image group activities:
redImageNames: List[str] = ???

# The number of red pixels in the five images with the most red pixels among the image group activities:
redPixelAmounts: List[int] = ???

print("The annotations and red pixel counts for the five images with the most red pixels among the image group 'activities':")
for color, pixel_count in zip(redImageNames, redPixelAmounts):
    print(f"- {color} (red pixels: {pixel_count})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Advanced Task 4 - Machine learning tasks (2 points)
# MAGIC
# MAGIC This advanced task involves experimenting with the classifiers provided by the Spark machine learning library. Time series data collected in the [ProCem](https://www.senecc.fi/projects/procem-2) research project is used as the training and test data. Similar data in a slightly different format was used in the first tasks of weekly exercise 3.
# MAGIC
# MAGIC The folder `assignment/energy/procem_13m.parquet` in the [Shared container](https://portal.azure.com/#view/Microsoft_Azure_Storage/ContainerMenuBlade/~/overview/storageAccountId/%2Fsubscriptions%2Fe0c78478-e7f8-429c-a25f-015eae9f54bb%2FresourceGroups%2Ftuni-cs320-f2024-rg%2Fproviders%2FMicrosoft.Storage%2FstorageAccounts%2Ftunics320f2024gen2/path/shared/etag/%220x8DBB0695B02FFFE%22/defaultEncryptionScope/%24account-encryption-key/denyEncryptionScopeOverride~/false/defaultId//publicAccessVal/None) contains the time series data in Parquet format.
# MAGIC
# MAGIC #### Data description
# MAGIC
# MAGIC The dataset contains time series data from a period of 13 months (from the beginning of May 2023 to the end of May 2024). Each row contains the average of the measured values for a single minute. The following columns are included in the data:
# MAGIC
# MAGIC | column name        | column type   | description |
# MAGIC | ------------------ | ------------- | ----------- |
# MAGIC | time               | long          | The UNIX timestamp in second precision |
# MAGIC | temperature        | double        | The temperature measured by the weather station on top of Sähkötalo (`°C`) |
# MAGIC | humidity           | double        | The humidity measured by the weather station on top of Sähkötalo (`%`) |
# MAGIC | wind_speed         | double        | The wind speed measured by the weather station on top of Sähkötalo (`m/s`) |
# MAGIC | power_tenants      | double        | The total combined electricity power used by the tenants on Kampusareena (`W`) |
# MAGIC | power_maintenance  | double        | The total combined electricity power used by the building maintenance systems on Kampusareena (`W`) |
# MAGIC | power_solar_panels | double        | The total electricity power produced by the solar panels on Kampusareena (`W`) |
# MAGIC | electricity_price  | double        | The market price for electricity in Finland (`€/MWh`) |
# MAGIC
# MAGIC There are some missing values that need to be removed before using the data for training or testing. However, only the minimal amount of rows should be removed for each test case.
# MAGIC
# MAGIC ### Tasks
# MAGIC
# MAGIC - The main task is to train and test a machine learning model with [Random forest classifier](https://spark.apache.org/docs/3.5.0/ml-classification-regression.html#random-forests) in six different cases:
# MAGIC     - Predict the month (1-12) using the three weather measurements (temperature, humidity, and wind speed) as input
# MAGIC     - Predict the month (1-12) using the three power measurements (tenants, maintenance, and solar panels) as input
# MAGIC     - Predict the month (1-12) using all seven measurements (weather values, power values, and price) as input
# MAGIC     - Predict the hour of the day (0-23) using the three weather measurements (temperature, humidity, and wind speed) as input
# MAGIC     - Predict the hour of the day (0-23) using the three power measurements (tenants, maintenance, and solar panels) as input
# MAGIC     - Predict the hour of the day (0-23) using all seven measurements (weather values, power values, and price) as input
# MAGIC - For each of the six case you are asked to:
# MAGIC     1. Clean the source dataset from rows with missing values.
# MAGIC     2. Split the dataset into training and test parts.
# MAGIC     3. Train the ML model using a Random forest classifier with case-specific input and prediction.
# MAGIC     4. Evaluate the accuracy of the model with Spark built-in multiclass classification evaluator.
# MAGIC     5. Further evaluate the accuracy of the model with a custom build evaluator which should do the following:
# MAGIC         - calculate the percentage of correct predictions
# MAGIC             - this should correspond to the accuracy value from the built-in accuracy evaluator
# MAGIC         - calculate the percentage of predictions that were at most one away from the correct predictions taking into account the cyclic nature of the month and hour values:
# MAGIC             - if the correct month value was `5`, then acceptable predictions would be `4`, `5`, or `6`
# MAGIC             - if the correct month value was `1`, then acceptable predictions would be `12`, `1`, or `2`
# MAGIC             - if the correct month value was `12`, then acceptable predictions would be `11`, `12`, or `1`
# MAGIC         - calculate the percentage of predictions that were at most two away from the correct predictions taking into account the cyclic nature of the month and hour values:
# MAGIC             - if the correct month value was `5`, then acceptable predictions would be from `3` to `7`
# MAGIC             - if the correct month value was `1`, then acceptable predictions would be from `11` to `12` and from `1` to `3`
# MAGIC             - if the correct month value was `12`, then acceptable predictions would be from `10` to `12` and from `1` to `2`
# MAGIC         - calculate the average probability the model predicts for the correct value
# MAGIC             - the probabilities for a single prediction can be found from the `probability` column after the predictions have been made with the model
# MAGIC - As the final part of this advanced task, you are asked to do the same experiments (training+evaluation) with two further cases of your own choosing:
# MAGIC     - you can decide on the input columns yourself
# MAGIC     - you can decide the predicted attribute yourself
# MAGIC     - you can try some other classifier other than the random forest one if you want
# MAGIC
# MAGIC In all cases you are free to choose the training parameters as you wish.<br>
# MAGIC Note that it is advisable that while you are building your task code to only use a portion of the full 13-month dataset in the initial experiments.

# COMMAND ----------

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.classification import RandomForestClassifier, DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import col, from_unixtime, hour, month, dayofweek, lit
from pyspark.sql.types import BooleanType, DoubleType
from pyspark.sql import Row
import numpy as np

# COMMAND ----------

# Read data from a parquet file
df = spark.read.parquet("abfss://shared@tunics320f2024gen2.dfs.core.windows.net/assignment/energy/procem_13m.parquet")

# Extract month and hour from the timestamp
df = df.withColumn("month", month(from_unixtime(col("time"))))
df = df.withColumn("hour", hour(from_unixtime(col("time"))))

# Remove rows with missing values
df = df.dropna()
# Select relevant columns for different predictions
# 1
data_month_weather = df.select("temperature", "humidity", "wind_speed", "month")
# 2
data_month_power = df.select("power_tenants", "power_maintenance", "power_solar_panels", "month")
# 3
data_month_all = df.select("temperature", "humidity", "wind_speed", "power_tenants", "power_maintenance", "power_solar_panels", "electricity_price", "month")
# 4
data_hour_weather = df.select("temperature", "humidity", "wind_speed", "hour")
# 5
data_hour_power = df.select("power_tenants", "power_maintenance", "power_solar_panels", "hour")
# 6
data_hour_all = df.select("temperature", "humidity", "wind_speed", "power_tenants", "power_maintenance", "power_solar_panels", "electricity_price", "hour")

# Assemble features for different predictions
assembler_weather = VectorAssembler(inputCols=["temperature", "humidity", "wind_speed"], outputCol="features")
assembler_power = VectorAssembler(inputCols=["power_tenants", "power_maintenance", "power_solar_panels"], outputCol="features")
assembler_all = VectorAssembler(inputCols=["temperature", "humidity", "wind_speed", "power_tenants", "power_maintenance", "power_solar_panels", "electricity_price"], outputCol="features")

data_month_weather = assembler_weather.transform(data_month_weather).select("features", "month")
data_month_power = assembler_power.transform(data_month_power).select("features", "month")
data_month_all = assembler_all.transform(data_month_all).select("features", "month")
data_hour_weather = assembler_weather.transform(data_hour_weather).select("features", "hour")
data_hour_power = assembler_power.transform(data_hour_power).select("features", "hour")
data_hour_all = assembler_all.transform(data_hour_all).select("features", "hour")

# Split data into training and test sets
train_data_month_weather, test_data_month_weather = data_month_weather.randomSplit([0.8, 0.2], seed=42)
train_data_month_power, test_data_month_power = data_month_power.randomSplit([0.8, 0.2], seed=42)
train_data_month_all, test_data_month_all = data_month_all.randomSplit([0.8, 0.2], seed=42)
train_data_hour_weather, test_data_hour_weather = data_hour_weather.randomSplit([0.8, 0.2], seed=42)
train_data_hour_power, test_data_hour_power = data_hour_power.randomSplit([0.8, 0.2], seed=42)
train_data_hour_all, test_data_hour_all = data_hour_all.randomSplit([0.8, 0.2], seed=42)

# Define the model
rf_month = RandomForestClassifier(labelCol="month", featuresCol="features", numTrees=100)
rf_hour = RandomForestClassifier(labelCol="hour", featuresCol="features", numTrees=100)

# Train the models
model_month_weather = rf_month.fit(train_data_month_weather)
model_month_power = rf_month.fit(train_data_month_power)
model_month_all = rf_month.fit(train_data_month_all)
model_hour_weather = rf_hour.fit(train_data_hour_weather)
model_hour_power = rf_hour.fit(train_data_hour_power)
model_hour_all = rf_hour.fit(train_data_hour_all)

# Make predictions
predictions_month_weather = model_month_weather.transform(test_data_month_weather)
predictions_month_power = model_month_power.transform(test_data_month_power)
predictions_month_all = model_month_all.transform(test_data_month_all)
predictions_hour_weather = model_hour_weather.transform(test_data_hour_weather)
predictions_hour_power = model_hour_power.transform(test_data_hour_power)
predictions_hour_all = model_hour_all.transform(test_data_hour_all)

# Evaluate the models
evaluator_month = MulticlassClassificationEvaluator(labelCol="month", predictionCol="prediction", metricName="accuracy")
evaluator_hour = MulticlassClassificationEvaluator(labelCol="hour", predictionCol="prediction", metricName="accuracy")

accuracy_month_weather = evaluator_month.evaluate(predictions_month_weather)
accuracy_month_power = evaluator_month.evaluate(predictions_month_power)
accuracy_month_all = evaluator_month.evaluate(predictions_month_all)
accuracy_hour_weather = evaluator_hour.evaluate(predictions_hour_weather)
accuracy_hour_power = evaluator_hour.evaluate(predictions_hour_power)
accuracy_hour_all = evaluator_hour.evaluate(predictions_hour_all)

# Display results
print(f"Accuracy on test data (month prediction using weather measurements): {accuracy_month_weather}")
print(f"Accuracy on test data (month prediction using power measurements): {accuracy_month_power}")
print(f"Accuracy on test data (month prediction using all measurements): {accuracy_month_all}")
print(f"Accuracy on test data (hour prediction using weather measurements): {accuracy_hour_weather}")
print(f"Accuracy on test data (hour prediction using power measurements): {accuracy_hour_power}")
print(f"Accuracy on test data (hour prediction using all measurements): {accuracy_hour_all}")

# COMMAND ----------

# Define UDFs to check if predictions are within 1 or 2 units away, considering cyclic nature
def within_range(label, prediction, total_no_value, range_value):
    return (abs(label - prediction) <= 1) or (abs(label - prediction) >= total_no_value - range_value)

def correct_probability(probability, label):
    return float(probability[label])

within_range_udf = udf(lambda label, prediction, total_no_value, range_value: within_range(label, prediction, total_no_value, range_value), BooleanType())
correct_probability_udf = udf(correct_probability, DoubleType())

def custom_evaluate(predictions, label_col, total_no_value):
    # Calculate the percentage of correct predictions
    correct_predictions = predictions.filter(col(label_col) == col("prediction")).count()
    total_predictions = predictions.count()
    accuracy = correct_predictions / total_predictions

    # Add columns to the predictions DataFrame to check if predictions are within range and calculate the percentages of predictions within a range
    within_range_percentages = []
    for range_value in [1, 2]:
        col_name = f"within_{range_value}_away"
        predictions = predictions.withColumn(
            col_name, within_range_udf(col(label_col), col("prediction"), lit(total_no_value), lit(range_value))
        )
        within_range_count = predictions.filter(col(col_name)).count()
        within_range_percentage = within_range_count / total_predictions
        within_range_percentages.append(within_range_percentage)

    # Calculate the average probability the model predicts for the correct value
    predictions = predictions.withColumn("correct_probability", correct_probability_udf(col("probability"), col(label_col)))
    average_correct_probability = predictions.select("correct_probability").agg({"correct_probability": "avg"}).collect()[0][0]

    return accuracy, within_range_percentages[0], within_range_percentages[1], average_correct_probability

# COMMAND ----------

evaluation_configs = [
    # Month predictions
    ("RandomForest", "month", 12, predictions_month_weather, "temperat,humidity,wind_spe"),
    ("RandomForest", "month", 12, predictions_month_power, "power_te,power_ma,power_so"),
    ("RandomForest", "month", 12, predictions_month_all, "temperat,humidity,wind_spe,power_te,power_ma,power_so,electric"),
    
    # Hour predictions
    ("RandomForest", "hour", 24, predictions_hour_weather, "temperat,humidity,wind_spe"),
    ("RandomForest", "hour", 24, predictions_hour_power, "power_te,power_ma,power_so"),
    ("RandomForest", "hour", 24, predictions_hour_all, "temperat,humidity,wind_spe,power_te,power_ma,power_so,electric"),
]

# Evaluate models using a loop
results = [
    Row(
        classifier=classifier,
        input=features,
        label=label,
        correct=float(accuracy),
        within_one=float(within_one),
        within_two=float(within_two),
        avg_prob=float(avg_prob),
    )
    for classifier, label, total_no_value, predictions, features in evaluation_configs
    for accuracy, within_one, within_two, avg_prob in [custom_evaluate(predictions, label, total_no_value)]
]

# Create a DataFrame from the results
accuracy_df = spark.createDataFrame(results)

# Display the DataFrame
display(accuracy_df)

# COMMAND ----------

custom_results = []
# Add day of the week column to the DataFrame
df = df.withColumn("day_of_week", dayofweek(from_unixtime(col("time"))))

# Select features and label
data_weather_day_of_week = df.select("temperature", "humidity", "wind_speed", "day_of_week")
data_power_day_of_week = df.select("power_tenants", "power_maintenance", "power_solar_panels", "electricity_price", "day_of_week")

assembler_power_and_price = VectorAssembler(inputCols=["power_tenants", "power_maintenance", "power_solar_panels", "electricity_price"], outputCol="features")

data_weather_day_of_week = assembler_weather.transform(data_weather_day_of_week).select("features", "day_of_week")
data_power_day_of_week = assembler_power_and_price.transform(data_power_day_of_week).select("features", "day_of_week")


# Split the data into training and test sets
train_data_day_of_week_weather, test_data_day_of_week_weather = data_weather_day_of_week.randomSplit([0.8, 0.2], seed=42)
train_data_day_of_week_power_and_price, test_data_day_of_week_power_and_price = data_power_day_of_week.randomSplit([0.8, 0.2], seed=42)

# Initialize classifiers
rf_classifier_day_of_week = RandomForestClassifier(labelCol="day_of_week", featuresCol="features", numTrees=100)
dt_classifier_day_of_week = DecisionTreeClassifier(labelCol="day_of_week", featuresCol="features")

# Train the models
rf_model_day_of_week_weather = rf_classifier_day_of_week.fit(train_data_day_of_week_weather)
dt_model_day_of_week_weather = dt_classifier_day_of_week.fit(train_data_day_of_week_weather)
rf_model_day_of_week_power_and_price = rf_classifier_day_of_week.fit(train_data_day_of_week_weather)
dt_model_day_of_week_power_and_price = dt_classifier_day_of_week.fit(train_data_day_of_week_weather)

# Make predictions
rf_predictions_day_of_week_weather = rf_model_day_of_week_weather.transform(test_data_day_of_week_weather)
dt_predictions_day_of_week_weather = dt_model_day_of_week_weather.transform(test_data_day_of_week_weather)
rf_predictions_day_of_week_power_and_price = rf_model_day_of_week_power_and_price.transform(test_data_day_of_week_power_and_price)
dt_predictions_day_of_week_power_and_price = dt_model_day_of_week_power_and_price.transform(test_data_day_of_week_power_and_price)

# Custom evaluate the models
accuracy, within_one, within_two, avg_prob = custom_evaluate(rf_predictions_day_of_week_weather, "day_of_week", 7)
custom_results.append(Row(classifier="RandomForest", input="temperat,humidity,wind_spe", label="day_of_week", correct=float(accuracy), within_one=float(within_one), within_two=float(within_two), avg_prob=float(avg_prob)))

accuracy, within_one, within_two, avg_prob = custom_evaluate(dt_predictions_day_of_week_weather, "day_of_week", 7)
custom_results.append(Row(classifier="DecisionTree", input="temperat,humidity,wind_spe", label="day_of_week", correct=float(accuracy), within_one=float(within_one), within_two=float(within_two), avg_prob=float(avg_prob)))

accuracy, within_one, within_two, avg_prob = custom_evaluate(rf_predictions_day_of_week_power_and_price, "day_of_week", 7)
custom_results.append(Row(classifier="RandomForest", input="power_tenants,power_maintenance,power_solar_panels,electricity_price", label="day_of_week", correct=float(accuracy), within_one=float(within_one), within_two=float(within_two), avg_prob=float(avg_prob)))

accuracy, within_one, within_two, avg_prob = custom_evaluate(dt_predictions_day_of_week_power_and_price, "day_of_week", 7)
custom_results.append(Row(classifier="DecisionTree", input="power_tenants,power_maintenance,power_solar_panels,electricity_price", label="day_of_week", correct=float(accuracy), within_one=float(within_one), within_two=float(within_two), avg_prob=float(avg_prob)))

# Evaluate the model using built-in evaluator
evaluator = MulticlassClassificationEvaluator(labelCol="day_of_week", predictionCol="prediction", metricName="accuracy")

rf_accuracy_weather = evaluator.evaluate(rf_predictions_day_of_week_weather)

dt_accuracy_weather = evaluator.evaluate(dt_predictions_day_of_week_weather)

rf_accuracy_power_and_price = evaluator.evaluate(rf_predictions_day_of_week_power_and_price)

dt_accuracy_power_and_price = evaluator.evaluate(dt_predictions_day_of_week_power_and_price)

print("RandomForest Accuracy: " + str(rf_accuracy_weather) + " " + str(rf_accuracy_power_and_price))
print("DecisionTree Accuracy: " + str(dt_accuracy_weather) + " " + str(dt_accuracy_power_and_price))

# Create a DataFrame from the results
accuracy_df = spark.createDataFrame(custom_results)

# Display the DataFrame
display(accuracy_df)