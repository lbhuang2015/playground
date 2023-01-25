# 1. Accelerate maven

- Skip testing code

  ```shell
  -Dmaven.test.skip=true；
  ```

- compile by multiple threads

  ```shell
   -Dmaven.compile.fork=true
  ```

- For 3.x or later

Use parameter `-T 1C`

- Example：

```shell
mvn clean package -T 1C -Dmaven.test.skip=true -Dmaven.compile.fork=true
```

- remove Lombok

  if possible remove Lombok

- Others

```shell
MAVEN_OPTS= -XX:+TieredCompilation -XX:TieredStopAtLevel=1
mvn -T 1C install -pl $moduleName -am --offline
#-pl $moduleName -am
#-pl - makes Maven build only specified modules and not the whole project.
#-am - find module's dependencies and built them.
#offline build (using local repo)
```

