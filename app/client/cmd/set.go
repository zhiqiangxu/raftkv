package cmd

import (
	"fmt"
	"qpush/modules/logger"
	"raftkv/client"
	cimpl "raftkv/client/impl"

	"github.com/spf13/cobra"
)

var setCmd = &cobra.Command{
	Use:   "set [address] [key] [value]",
	Short: "connect to [address] and set [key] [value]",
	Args:  cobra.MinimumNArgs(3),
	Run: func(cmd *cobra.Command, args []string) {
		logger.Info("test1")
		var agent client.Agent = &cimpl.Agent{}
		conn, err := agent.Dial(args[0])
		if err != nil {
			panic(err)
		}
		logger.Info("test2")
		err = conn.Set([]byte(args[1]), []byte(args[2]))
		if err != nil {
			panic(err)
		}
		fmt.Println("set ok")
	}}

func init() {
	rootCmd.AddCommand(setCmd)
}
