package main

import (
	"bufio"
	"github.com/go-tron/mqtt"
	"fmt"
	"os"
	"strings"
)

func subHandler(topic string, msg string) error {
	fmt.Println("sub", topic, msg)
	return nil
}

func main() {

	clientId := "342"
	client := mqtt.New(&mqtt.Config{
		Addr:         "127.0.0.1:1883",
		UserName:     "pos",
		Password:     "M9o!ejN@1fm#oH#M",
		ClientId:     clientId,
		CleanSession: true,
	})

	client.Subscribe("pingres/"+clientId, subHandler, mqtt.SubWithoutLog())

	scanner := bufio.NewScanner(os.Stdin)
	for scanner.Scan() {
		if scanner.Text() == "disconnect" {
			client.Disconnect()
			continue
		}
		arr := strings.Split(scanner.Text(), "@")
		if len(arr) != 2 {
			fmt.Println("message format mismatch")
			continue
		}
		if err := client.Publish(arr[0]+"/"+clientId, arr[1]); err != nil {
			fmt.Println(err)
			continue
		}
		fmt.Println("pub", arr[0]+"/"+clientId, arr[1])
	}

}
