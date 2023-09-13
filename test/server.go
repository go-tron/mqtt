package main

import (
	"bufio"
	"errors"
	"fmt"
	"github.com/go-tron/logger"
	"github.com/go-tron/mqtt"
	"os"
	"strings"
	"time"
)

func pubHandler(topic string, msg string) error {
	fmt.Println("sub", topic, msg)
	time.Sleep(time.Second * 100)
	return errors.New("asd")
}

func main() {

	client := mqtt.New(&mqtt.Config{
		Addr:         "mqtt.eioos.com:1813",
		UserName:     "server",
		Password:     "s28wfCMn##Y!znu6",
		ClientId:     "server1",
		CleanSession: false,
		ClientLogger: logger.NewZap("mqtt-client", "info"),
	})

	client.Subscribe("gateway/+/read", pubHandler, mqtt.SubWithoutLog())
	client.Subscribe("gateway/+/info", pubHandler, mqtt.SubWithoutLog())

	if err := client.Publish("server/AA:BB:CC:DD:EE:F1/read", "test"); err != nil {
		fmt.Println(err)
		return
	}

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		arr := strings.Split(scanner.Text(), "@")
		if len(arr) != 2 {
			fmt.Println("message format mismatch")
			continue
		}
		if err := client.Publish(arr[0], arr[1]); err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println("pub", arr[0], arr[1])
	}
}
