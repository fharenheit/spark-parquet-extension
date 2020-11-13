# Spark Parquet Extension

Spark에서 Parquet 파일을 생성하기 위한 추가 확장 모듈 프로젝트

## Spark Shell

Spark Shell에서 해당 모듈을 실행하기 위해서 다음과 같이 JAR 옵션으로 `.jar` 파일을 지정합니다.

```
spark-shell --jars spark-parquet-extension.jar,jedis-3.3.0.jar,commons-lang3-3.5.jar
```

## Redis

### macOS

다음의 커맨드로 Redis를 설치합니다.

```
brew install redis
```

다음과 같이 환경설정 파일을 추가합니다.

```
vi /usr/local/etc/redis.conf
```

```
brew services start redis
brew services stop redis
brew services restart redis
```

## Reference

* Redis Client : https://www.baeldung.com/jedis-java-redis-client-library