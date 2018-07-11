package cmd

import (
	"fmt"
	"qpush/modules/logger"
	"raftkv/client"
	cimpl "raftkv/client/impl"

	"github.com/spf13/cobra"
)

var joinCmd = &cobra.Command{
	Use:   "get [api address] [node raft addr] [api addr]",
	Short: "connect to [api address] and join a node",
	Args:  cobra.MinimumNArgs(3),
	Run: func(cmd *cobra.Command, args []string) {
		logger.Info("test1")
		var agent client.Agent = &cimpl.Agent{}
		conn, err := agent.Dial(args[0])
		if err != nil {
			panic(err)
		}
		logger.Info("test2")
		err = conn.Join(args[1], args[2])
		if err != nil {
			panic(err)
		}

		fmt.Println("join ok")
	}}

func init() {
	rootCmd.AddCommand(joinCmd)
}
