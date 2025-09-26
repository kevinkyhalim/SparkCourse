# Advanced Spark Concepts



- Broadcast Variable:
    - Broadcasts objects to the executors so that they are always there whenever needed!
    - sc.broadcast() to ship off the object
    - .value() to get the object back

- Joining

- Bread-First-Search Implementation in Spark
    1. Convert each line as a node with conections, color and distance
    i.e. 1 2 34 4 5 6 to (1, (2, 34, 4, 5, 6), 9999, WHITE)
    9999 here means the distance
    2. Go through, looking for gray nodes to expand
    3. Color nodes that we are done with black
    4. Update distances as we go

- Accumulator
    - Allows many executors to increment a shared variable

- Item based collaborative filtering
    - Caching: useful when performing more than one action on a dataframe
        - .cache()  : cache to memory
        - .persist() : cache to disk