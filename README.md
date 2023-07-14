# 修改

## 主要修改内容
1. 异步监听配置文件的变更, 实时加载, 如果修改配置文件出错，会还原回原来的配置
2. 异步通知 **spacemesh-storage-server** 服务拉取最新的 **plot** 文件

## 配置文件
```json
{
  "db_path": "/etc/spacemesh-storage-proxy.db",
  "localplot": false,
  "host": "127.0.0.1",
  "port": 10089,
  "file_server_port": 10099,
  "storage_hosts": [
    "127.0.0.1"
  ]
}
```

## service 文件
```
cat << EOF > /etc/systemd/system/spacemesh-storage-proxy.service
[Unit]
Description=Spacemesh Plotter
After=lotus-mount-disk.service

[Service]
ExecStart=/usr/local/bin/spacemesh-storage-proxy --config /etc/spacemesh-storage-proxy.conf
Restart=always
RestartSec=10
MemoryAccounting=true
MemoryHigh=infinity
MemoryMax=infinity
Nice=-20
LimitNICE=-20
LimitNOFILE=1048576:1048576
LimitCORE=infinity
LimitNPROC=819200:1048576
IOWeight=9999
CPUWeight=1000
LimitCORE=1024
Delegate=yes
User=root

[Install]
WantedBy=multi-user.target
```

----

## 部署

部署相关的文件

+ spacemesh-storage-proxy
+ spacemesh-storage-proxy.conf
+ spacemesh-storage-proxy.service

| 文件                       | 部署路径            | 说明         |
| :------------------------- | :------------------ | :----------- |
| spacemesh-storage-proxy         | /usr/local/bin      |              |
| spacemesh-storage-proxy.conf    | /etc                | 配置文件     |
| spacemesh-storage-proxy.service | /etc/systemd/system | service 文件 |

**注**
**spacemesh-storage-proxy** 服务要设置自启动
```
  systemctl enable spacemesh-storage-proxy
```
