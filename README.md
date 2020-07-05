## [CS-186](https://cs186berkeley.net/) 

By this course, you will learn how to use `Docker`, `Maven` and `Git`  

#### Command
##### start image
```bash
docker start -ai cs186
```

##### run test for projN
```bash
mvn clean test -D proj=N
```

#### Note
```java
TestRecoveryManager.testAnalysisCheckpoints2() {
    iter.next(); // add by developer at line 1445
}
```