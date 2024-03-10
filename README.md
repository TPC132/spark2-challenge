# App Execution

The execution requires the use of JVM options:
```bash
export MAVEN_OPTS="--add-exports java.base/sun.nio.ch=ALL-UNNAMED"
```
After JVM Options, here is the execution command
```bash
mvn compile exec:java -Dexec.mainClass="com.challenge.App"
```

## Naming Files
From what I have searched, when writing a dataframe to a file using spark, it is not possible to choose the name of it. 
Therefore, I attributed the desired name to the directory. 

