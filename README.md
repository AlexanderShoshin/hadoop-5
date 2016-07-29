## Description

This application will be launch a web interface in application master container.
You can specify memory, cores and container count for test task execution via web interface.


## Testing environment

Programm was tested on HDP 2.4 sandbox.

## How to deploy

1. Make jars by running command from the project root:
```
mvn clean package
```
2. Copy yarn-container/target/yarn-container-1.0.0.jar, yarn-web/target/yarn-webAM-1.0.0.jar, yarn-client/target/yarn-client-1.0.0.jar to machine with hadoop installed.
4. Launch application by running command:
```
yarn jar yarn-client-1.0.0.jar -amJar yarn-web-1.0.0.jar -conJar yarn-container-1.0.0.jar
```
5. Web interface will be available on http://<cluster-host>:1212.
6. Try to launch test task on claster specifying availiable memory, cores, containers via web interface.