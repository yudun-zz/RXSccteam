#RXS Cloud Computing Project

-----------

This request is just checking whether I could contribute to this repo normally.

If anyone got a bunch of code to contribute, please briefly describe it here for another team members to understand

Yeah it works normally. Let's do this guys.

--Xiaotong


## File Description
`CCTeam.java`
The main entry of our vertx front end. 
It defines all the routing logic and initialize the mysql client as well.

I tried three different strategy:
1.    Set up a new connection whenever a request comes and close it after it ends (RPS=3918.5/m4.large, 1 min)
2.    Use a single connection for all requests **(We use this one in the end!!!)** (RPS=4473.7/m4.large, 1 min)
3.    Keep a pool of connection and request them in a Round Robin manner (RPS=276.2/3 connections, m4.large, 1 min)

We choose the second strategy eventually, so you can ignore `Q2Request.java` and `MyConnection.java`

`ConfigSingleton.java`
Define all the necessary configuration in our project.
It serves as a singleton for all other modules to use.

`Q1.java`
The logic for responsing ti q1

`Q2.java`
The logic for responsing to q2
It used the **single connection** set up in the CCTeam.java to select desired info from our mysql

`Q2tester.java`
A tester comparing the result from the buggy and stupid refrence server with ours

`Q2Request.java`
A wrapper for our request to be put into the connection queue in our "multi_connection" strategy.
**You can ignore this as we don't use the "multi_connection" strategy**

`MyConnection.java`
A wrapper for each Connection we set up at the beginning of our "multi_connection" strategy.
**You can ignore this as we don't use the "multi_connection" strategy**

--  Shimin Wang