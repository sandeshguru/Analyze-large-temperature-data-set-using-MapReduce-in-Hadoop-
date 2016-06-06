This Programming assignment is implemented on Amazon EC2 Server using Hadoop Map Reducer. The csv file contains the weather data for last 25 years. This is supplied as the input to the mapper function. The mapper function computes the average for every month in a year using the max temperature and min temperature. The key value is passed to the reducer as key being the year and value being temperature averages. The reducer computes the average per year and creates an output file and puts into that.

Usage in Ec2:
1.	Hadoop fs –mkdir /user/Ubuntu/avgtemp
2.	Hadoop fs –put input.csv /user/Ubuntu/avgtemp
3.	Hadoop jar Assignment.jar /user/Ubuntu/avgtemp /user/Ubuntu/output 1 2 <Seasonally or yearly>
4.	Hadoop fs –cat /user/Ubuntu/part-00000
