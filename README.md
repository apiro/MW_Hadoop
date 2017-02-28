# Project Hadoop for the course Middleware Technologies for Distributed Systems, year 2016-17

(Course Page)[http://corsi.dei.polimi.it/distsys/]

## Project description

The goal of this project is to infer qualitative data regarding the car accidents in New York City. Start by downloading the data set from (here) [http://home.deib.polimi.it/guinea/Materiale/Middleware/index.html].

Each row in the data set contains the following data: DATE, TIME, BOROUGH, ZIP CODE, LATITUDE, LONGITUDE, LOCATION, ON STREET NAME, CROSS STREET NAME, OFF STREET NAME, NUMBER OF PERSONS INJURED, NUMBER OF PERSONS KILLED, NUMBER OF PEDESTRIANS INJURED, NUMBER OF PEDESTRIANS KILLED, NUMBER OF CYCLIST INJURED, NUMBER OF CYCLIST KILLED, NUMBER OF MOTORIST INJURED, NUMBER OF MOTORIST KILLED, CONTRIBUTING FACTOR VEHICLE 1, CONTRIBUTING FACTOR VEHICLE 2, CONTRIBUTING FACTOR VEHICLE 3, CONTRIBUTING FACTOR VEHICLE 4, CONTRIBUTING FACTOR VEHICLE 5, UNIQUE KEY, VEHICLE TYPE CODE 1, VEHICLE TYPE CODE 2, VEHICLE TYPE CODE 3, VEHICLE TYPE CODE 4, VEHICLE TYPE CODE 5

By CONTRIBUTING FACTOR we intend the reason why the accident took place in the first place.

Using Hadoop in a fully distributed cluster (use your own physical/virtual machines) provide the following information:

- number of lethal accidents per week throughout the entire dataset
- number of accidents and percentage of number of deaths per contributing factor in the dataset (e.g., let's take drinking and driving -> I want to know how many accidents were due to this cause and what percentage of these accidents were also lethal)
- number of accidents and average number of lethal accidents per week per borough (e.g., let's take burough number 1 -> I want to know how many accidents there were in burough 1 each week, as well as the average number of lethal accidents that the burough had per week)
Optional: Provide charts for the information.