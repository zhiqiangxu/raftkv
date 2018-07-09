package cmd

import (
	"fmt"
	"qpush/modules/logger"
	"raftkv/client"
	cimpl "raftkv/client/impl"

	"github.com/spf13/cobra"
)

var dumpCmd = &cobra.Command{
	Use:   "dump [address]",
	Short: "connect to [address] and dump all values",
	Args:  cobra.MinimumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		logger.Info("test1")
		var agent client.Agent = &cimpl.Agent{}
		conn, err := agent.Dial(args[0])
		if err != nil {
			panic(err)
		}
		logger.Info("test2")
		m, err := conn.Dump()
		if err != nil {
			panic(err)
		}

		fmt.Println("get", m)
	}}

func init() {
	rootCmd.AddCommand(dumpCmd)
}
