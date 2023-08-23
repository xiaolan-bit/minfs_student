@echo off
cd ..
cd dataServer
start "ds2" cmd /c "mvn spring-boot:run -D spring-boot.run.profiles=ds2"
start "ds3" cmd /c "mvn spring-boot:run -D spring-boot.run.profiles=ds3"
start "ds4" cmd /c "mvn spring-boot:run -D spring-boot.run.profiles=ds4"
cd ..
cd metaServer
start "slave" cmd /c "mvn spring-boot:run -D spring-boot.run.profiles=ms2"