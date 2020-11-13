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

다음과 같이 환경설정 파일을 찾아서 변경하고자 하는 설정값을 변경하고 저장합니다.

```
vi /usr/local/etc/redis.conf
```

서비스로 동작시키는 경우 다음과 같이 실행할 수 있습니다.

```
brew services start redis
brew services stop redis
brew services restart redis
```

서비스가 아닌 스탠드얼론 foreground로 동작시키는 경우 다음과 같이 실행합니다.

```
# redis-server                                                                                                                                                                  14:58:24 
  29263:C 13 Nov 2020 14:58:26.706 # oO0OoO0OoO0Oo Redis is starting oO0OoO0OoO0Oo
  29263:C 13 Nov 2020 14:58:26.707 # Redis version=6.0.9, bits=64, commit=00000000, modified=0, pid=29263, just started
  29263:C 13 Nov 2020 14:58:26.707 # Warning: no config file specified, using the default config. In order to specify a config file use redis-server /path/to/redis.conf
  29263:M 13 Nov 2020 14:58:26.708 * Increased maximum number of open files to 10032 (it was originally set to 256).
                  _._                                                  
             _.-``__ ''-._                                             
        _.-``    `.  `_.  ''-._           Redis 6.0.9 (00000000/0) 64 bit
    .-`` .-```.  ```\/    _.,_ ''-._                                   
   (    '      ,       .-`  | `,    )     Running in standalone mode
   |`-._`-...-` __...-.``-._|'` _.-'|     Port: 6379
   |    `-._   `._    /     _.-'    |     PID: 29263
    `-._    `-._  `-./  _.-'    _.-'                                   
   |`-._`-._    `-.__.-'    _.-'_.-'|                                  
   |    `-._`-._        _.-'_.-'    |           http://redis.io        
    `-._    `-._`-.__.-'_.-'    _.-'                                   
   |`-._`-._    `-.__.-'    _.-'_.-'|                                  
   |    `-._`-._        _.-'_.-'    |                                  
    `-._    `-._`-.__.-'_.-'    _.-'                                   
        `-._    `-.__.-'    _.-'                                       
            `-._        _.-'                                           
                `-.__.-'                                               
  
  29263:M 13 Nov 2020 14:58:26.712 # Server initialized
  29263:M 13 Nov 2020 14:58:26.712 * Ready to accept connections
```

## Reference

* Redis Client : https://www.baeldung.com/jedis-java-redis-client-library