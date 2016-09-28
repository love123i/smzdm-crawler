# 什么值得买信息抓取

# 需要环境:
```sh
# python2.7
pip install fabric
pip install beautifulsoup4
pip install html5lib
pip install requests    # require https://www.microsoft.com/en-us/download/confirmation.aspx?id=44266
pip install pymongo     # http://www.mongodb.org/downloads
```

## win32 下启动mongo
```
x64 https://fastdl.mongodb.org/win32/mongodb-win32-i386-3.2.9-signed.msi?_ga=1.91945178.1993393702.1474981843
```
#### 方法一
```
mongod -dbpath "d:\mongodb\db" --logpath "d:\mongodb\log\mongodb.log" --journal --storageEngine=mmapv1
```

#### 方法二 服务
1. 安装服务
mongod -dbpath "d:\mongodb\db" --logpath "d:\mongodb\log\mongodb.log" --journal --storageEngine=mmapv1 --install --serviceName "MongoDB"
2. 启动服务
net start MongoDB