package cmd

import (
	"fmt"
	"raftkv/modules/raftconf"

	"github.com/spf13/cobra"
)

var confCmd = &cobra.Command{
	Use:   "conf [address] [key]",
	Short: "connect to [address] and get value for [key]",
	Args:  cobra.MinimumNArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		raftconf.Init([]string{args[0]})
		value := raftconf.Get("key")

		fmt.Println("value is ", value)
	}}

func init() {
	rootCmd.AddCommand(confCmd)
}
