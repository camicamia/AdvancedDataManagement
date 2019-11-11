## Advanced Data Management Assignments

These exercises are part of assignments of Advanced Data Management.

They are done in ***Hadoop*** (*Java*), ***Pig*** and ***Spark*** (both *Python*).

Starting from a list of key&value tuples like this

```
car 4

pen 2

the 5

table 2
```

... and so on, where the number represents occurrences.

The output will be as the following:

**Exercise A**: the key should be the number of occurrences and the value a list of words with that number of occurrences in the input

```
2 table pen

4 car

5 the
```



**Exercise B**: the key should be the number of occurrences and the value the amount of the words associated to it.

```
2 2

4 1

5 1
```

**Exercise C**: exercises A and B using a Combiner.

**Exercise E**: taking the output A as an input, the output should be the pair with the maximum key.

 `5 the ` 

**Exercise F**: taking the output A as an input, the output should be the keys with maximum and minimum frequencies.

 ```
5 the
2 table pen
 ```



**Exercise G**: taking the output B as an input, the output should be the average frequency of the words.

`3.25`

**Exercise H**: taking the output A and the output B as two inputs, the output should be a join where the new key will be the output B value and the value will be the output A value.

 ```

2 table pen

1 car

1 the 
 ```



