# raftkv, distributed key value system based on raft, writen in go


-------------------

## Try
```
go run app/server/main.go --addr 0.0.0.0:8081 --raddr 192.168.68.237:8082
go run app/server/main.go --addr 0.0.0.0:8083 --raddr 192.168.68.237:8084
go run app/server/main.go --addr 0.0.0.0:8085 --raddr 192.168.68.237:8086


go run app/client/agent.go set localhost:8083 key value2
go run app/client/agent.go get localhost:8083 key
```
