# Big-Data-Analysis-using-SPARK
Solving small problems using **Spark** platform. All proposed implementations minimize data shuffled across servers by combining intermediate results.
![](Readme_img/img1.png)

## Data Description
#### Conncetivity_Score Data:
Three CSV files of size 105 MB, each line has a pair of **Twitter** user names such as **'user1 user2'** that indicates that **user1 is following user2**
#### Income Date:
Three CSV files of size 171 MB, each line has a city name and income of a person in this city (in thousands rounded to the closest integer) such as 'city10 41'. The names have been removed for privacy reasons and all incomes that are greater than 100 is reported as 100. Therefore, the possible income range is from 0 to 100 (integer only).
#### Directedgraph Data:
Three CSV files of size 691 KB, directed graph with the (positive) weighted edges. The graph is represented in an adjacency list format. In each line of the input file, the file entry is a node in the graph. The subsequent enteries in the line represent the edges and the edges lengths (weights). Each pair of values is essentially the end point of the directed edge and the lenght of the edge.

## Algorithm Implementation
![](Readme_img/img2.jpg)
### Twitter Connectivity Score 1
Designing a method to give each user on **Twitter** a connectivity score, each user's score (N*M) will be simply calculated by multiplying the number of people the user is following (N-outbound links in graph representation) and the number of people that are following the user (M-inbound links in graph representation).

### Twitter Connectivity Score 2
Updating score calculation in **Twitter Connectivity Score 1** if two users are following each other, following and follower counts are increased by only 0.5 instead of 1 for both users.

### Mode of Income per City
Calculating and reporting the mode of the incomes for each city (the income that
occurs most frequently for each city). If there are more than one mode for a city, select the smallest income as mode.

### Shortest Path Algorithm 
Designing Shortest Path algorithm on MapReduce, which finds all nodes within the distance of 10 (ten) to given node
