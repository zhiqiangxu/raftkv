package main

import (
	"qpush/modules/logger"
	"raftkv/server"
	"raftkv/server/impl"

	"github.com/spf13/cobra"
)

var (
	join     string
	apiAddr  string
	raftAddr string
)

func main() {

	var rootCmd = &cobra.Command{
		Use:   "raftkv [public address] [internal address]",
		Short: "listen and server at specified address",
		Run: func(cobraCmd *cobra.Command, args []string) {
			config := server.Config{
				APIAddr: apiAddr, RaftAddr: raftAddr,
				RaftDir: "/var/lib/raftkv/" + raftAddr, Join: join}

			var server server.Server
			server = impl.NewServer(config)
			err := server.ListenAndServe()
			if err != nil {
				logger.Error(err)
			}
		}}

	cobra.OnInitialize(initConfig)
	rootCmd.PersistentFlags().StringVar(&join, "join", "", "join cluster")
	rootCmd.PersistentFlags().StringVar(&apiAddr, "addr", "0.0.0.0:8081", "api addr")
	rootCmd.PersistentFlags().StringVar(&raftAddr, "raddr", "172.20.10.4:8082", "raft addr")
	rootCmd.Execute()

}

func initConfig() {

}
