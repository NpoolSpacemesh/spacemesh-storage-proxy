# 修改

## 主要修改内容
1. 异步监听配置文件的变更, 实时加载, 如果修改配置文件出错，会还原回原来的配置
2. 异步通知 **chia-storage-server** 服务拉取最新的 **plot** 文件

## 配置文件
```json
{
  "db_path": "/etc/chia-storage-proxy.db",
  "localplot": false,
  "host": "127.0.0.1",
  "port": 10089,
  "file_server_port": 10099,
  "storage_hosts": [
    "127.0.0.1"
  ]
}
```