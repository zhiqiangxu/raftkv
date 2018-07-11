# raftkv, distributed key value system based on raft, writen in go


-------------------

## Try
```
#启动3个server
go run app/server/main.go --addr 192.168.68.237:8081 --raddr 192.168.68.237:8082
go run app/server/main.go --addr 192.168.68.237:8083 --raddr 192.168.68.237:8084 --join 192.168.68.237:8081
go run app/server/main.go --addr 192.168.68.237:8085 --raddr 192.168.68.237:8086 --join 192.168.68.237:8081


#往一个node写
go run app/client/agent.go set 192.168.68.237:8081 key value2
#从另一个node读
go run app/client/agent.go get 192.168.68.237:8083 key
```
