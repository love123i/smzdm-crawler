# 什么值得买信息抓取

# 需要环境:
```sh
# python2.7
pip install beautifulsoup4
pip install html5lib
pip install requests    # require https://www.microsoft.com/en-us/download/confirmation.aspx?id=44266
pip install pymongo     # http://www.mongodb.org/downloads
```

## Linux 基础环境配置
```
# 请谨慎操作, 注意python版本
#更新内核
yum update -y
#安装开发工具
yum groupinstall Base "Development Tools" -y
yum -y install python-setuptools
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