# Intellias test task (oleksandr-tsilynko)
## - Brief -
> ### The project includes primarily 2 classes for storing data (`DataRow` is represented as a class for storing a single row with a key, and `JoinedDataRow` as the result of joining data from DataRow by a key), as well as 3 different implementations of data joining possibilities: Inner, Left, Right (similar to SQL).
> ### Stream API tools are involved. Unit tests are present (100% coverage).
## - Time and space complexity -
### 3 ways of handling the problem:
### 1. Inner join
> ### Initially have 2 collections to join. For our needs we create the resulting collection, with an appropriate extension to the collection (in this case, choose Set if we have at least one, otherwise choose List). We iterate completely through the left-hand collection (O(n)), for each item we look for a matching key from the right-hand collection (O(n) for each in the worst case). The result is: time complexity O(n^2) in the worst case, and memory allocation for one collection
### 2. Left join
> ### Same here, allocating memory to one collection (the output is the collection that the left one was), iterating through the left collection and trying to find the keys in the right collection, but with the difference that every item in the left collection will remain as it was (excluding null values). We have O(n^2) in the worst case, one resulting collection.
### 3. Right join
> ### Symmetrical to the left join.

## ▶️Project launch
- ### Copy project link
- ### Create new Intellij IDEA project from Version Control System
- ### Run with `mvn test`