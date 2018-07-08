package cmd

import (
	"fmt"
	"qpush/modules/logger"
	"raftkv/client"
	cimpl "raftkv/client/impl"
	"raftkv/modules/gob"
	"raftkv/server"

	"github.com/spf13/cobra"
)

var getCmd = &cobra.Command{
	Use:   "get [address] [key]",
	Short: "connect to [address] and get value for [key]",
	Args:  cobra.MinimumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		logger.Info("test1")
		var agent client.Agent = &cimpl.Agent{}
		conn, err := agent.Dial(args[0])
		if err != nil {
			panic(err)
		}
		logger.Info("test2")
		bytes, err := conn.Get([]byte(args[1]))
		if err != nil {
			panic(err)
		}

		resp := server.GetResp{}
		gob.FromBytes(bytes, &resp)
		fmt.Println("get", string(resp.Resp))
	}}

func init() {
	rootCmd.AddCommand(getCmd)
}
