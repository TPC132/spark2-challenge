# App Execution

The execution requires the use of JVM options:
```bash
export MAVEN_OPTS="--add-exports java.base/sun.nio.ch=ALL-UNNAMED"
```

## Naming Files
From what I have searched, when writing a dataframe to a file using spark, it is not possible to choose the name of it. 
Therefore, I attributed the desired name to the directory. 

