package mqtt

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/go-tron/config"
	"github.com/go-tron/logger"
	"net/url"
	"reflect"
)

type Config struct {
	Addr          string
	UserName      string
	Password      string
	ClientId      string
	CleanSession  bool
	Qos           uint
	Debug         bool
	SubHandler    Handler
	LogHandler    LogHandler
	ClientLogger  logger.Logger
	MessageLogger logger.Logger
}

type LogHandler = func(t string, topic string, message string, err error)

type Option func(*Config)

func WithSubHandler(handler Handler) Option {
	return func(c *Config) {
		c.SubHandler = handler
	}
}
func WithLogHandler(handler LogHandler) Option {
	return func(c *Config) {
		c.LogHandler = handler
	}
}

func NewWithConfig(c *config.Config, opts ...Option) *Client {
	//return nil
	config := &Config{
		Addr:          c.GetString("mqtt.addr"),
		UserName:      c.GetString("mqtt.userName"),
		Password:      c.GetString("mqtt.password"),
		ClientId:      c.GetString("cluster.nodeName"),
		CleanSession:  c.GetBool("mqtt.cleanSession"),
		Qos:           c.GetUint("mqtt.qos"),
		Debug:         c.GetBool("mqtt.debug"),
		ClientLogger:  logger.NewZapWithConfig(c, "mqtt-client", "info"),
		MessageLogger: logger.NewZapWithConfig(c, "mqtt-message", "info"),
	}

	for _, apply := range opts {
		apply(config)
	}
	return New(config)
}

func New(config *Config) *Client {

	if config == nil {
		panic("config 必须设置")
	}
	if config.UserName == "" {
		panic("UserName 必须设置")
	}
	if config.Password == "" {
		panic("Password 必须设置")
	}
	if config.ClientId == "" {
		panic("ClientId 必须设置")
	}
	//if config.ClientLogger == nil {
	//	panic("ClientLogger 必须设置")
	//}

	if config.ClientLogger != nil {
		mqtt.ERROR = &ErrorLogger{config.ClientLogger}
		mqtt.CRITICAL = &ErrorLogger{config.ClientLogger}
		mqtt.WARN = &InfoLogger{config.ClientLogger}
		if config.Debug {
			mqtt.DEBUG = &InfoLogger{config.ClientLogger}
		}
	}

	opts := mqtt.NewClientOptions().AddBroker("tcp://" + config.Addr)
	opts.SetUsername(config.UserName)
	opts.SetPassword(config.Password)
	opts.SetClientID(config.ClientId)
	opts.SetCleanSession(config.CleanSession)
	//默认为true 收到消息逐条执行Handler 可能造成消息阻塞
	opts.SetOrderMatters(false)
	opts.SetOnConnectHandler(func(client mqtt.Client) {
		if config.ClientLogger != nil {
			config.ClientLogger.Info("OnConnect",
				config.ClientLogger.Field("type", "handler"))
		}
	})
	opts.SetReconnectingHandler(func(mqtt.Client, *mqtt.ClientOptions) {
		if config.ClientLogger != nil {
			config.ClientLogger.Info("Reconnecting",
				config.ClientLogger.Field("type", "handler"))
		}
	})
	opts.SetConnectionAttemptHandler(func(broker *url.URL, tlsCfg *tls.Config) *tls.Config {
		defer func() {
			if config.ClientLogger != nil {
				config.ClientLogger.Info("ConnectionAttempt",
					config.ClientLogger.Field("type", "handler"))
			}
		}()
		return tlsCfg
	})
	opts.SetConnectionLostHandler(func(mqtt.Client, error) {
		if config.ClientLogger != nil {
			config.ClientLogger.Info("ConnectionLost",
				config.ClientLogger.Field("type", "handler"))
		}
	})

	opts.SetDefaultPublishHandler(func(client mqtt.Client, msg mqtt.Message) {
		if config.MessageLogger != nil {
			config.MessageLogger.Error(string(msg.Payload()),
				config.MessageLogger.Field("type", "sub"),
				config.MessageLogger.Field("topic", msg.Topic()),
				config.MessageLogger.Field("error", "no match handler"),
			)
		}
	})

	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		panic(token.Error())
	}

	fmt.Println("mqtt connect " + config.Addr)
	return &Client{
		Config: config,
		Client: client,
	}
}

type Handler = func(topic string, msg string) error

type Client struct {
	*Config
	mqtt.Client
}

func (c *Client) MessageFormat(data interface{}) ([]byte, error) {
	var message []byte
	v := reflect.ValueOf(data)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}

	if v.Type() == reflect.TypeOf(message) {
		message = data.([]byte)
	} else {
		if reflect.TypeOf(data).Kind() == reflect.String {
			message = []byte(data.(string))
		} else {
			var err error
			message, err = json.Marshal(data)
			if err != nil {
				return nil, err
			}
		}
	}
	return message, nil
}

type PubConfig struct {
	Log bool
	Qos byte
}
type PubOption func(*PubConfig)

func PubWithQos(val byte) PubOption {
	return func(c *PubConfig) {
		c.Qos = val
	}
}
func PubWithoutLog() PubOption {
	return func(c *PubConfig) {
		c.Log = false
	}
}

func (c *Client) Publish(topic string, data interface{}, opts ...PubOption) error {

	config := &PubConfig{
		Log: true,
		Qos: byte(c.Qos),
	}
	for _, apply := range opts {
		apply(config)
	}

	message, err := c.MessageFormat(data)
	defer func() {
		if !config.Log {
			return
		}
		if c.LogHandler != nil {
			c.LogHandler("pub", topic, string(message), err)
		}
		if c.MessageLogger != nil {
			c.MessageLogger.Info(string(message),
				c.MessageLogger.Field("type", "pub"),
				c.MessageLogger.Field("topic", topic),
				c.MessageLogger.Field("error", err),
			)
		}

	}()
	if err != nil {
		return err
	}

	token := c.Client.Publish(topic, config.Qos, false, message)
	if token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

type SubConfig struct {
	Log bool
	Qos byte
}
type SubOption func(*SubConfig)

func SubWithQos(val byte) SubOption {
	return func(c *SubConfig) {
		c.Qos = val
	}
}
func SubWithoutLog() SubOption {
	return func(c *SubConfig) {
		c.Log = false
	}
}

func (c *Client) Subscribe(topic string, handler Handler, opts ...SubOption) error {

	config := &SubConfig{
		Log: true,
		Qos: byte(c.Qos),
	}
	for _, apply := range opts {
		apply(config)
	}

	token := c.Client.Subscribe(topic, config.Qos, func(client mqtt.Client, msg mqtt.Message) {
		var (
			message = string(msg.Payload())
			topic   = msg.Topic()
			err     error
		)
		defer func() {
			if !config.Log {
				return
			}
			if c.LogHandler != nil {
				c.LogHandler("sub", topic, message, err)
			}

			if c.MessageLogger != nil {
				c.MessageLogger.Info(message,
					c.MessageLogger.Field("type", "sub"),
					c.MessageLogger.Field("topic", topic),
					c.MessageLogger.Field("error", err),
				)
			}
		}()

		if c.SubHandler != nil {
			if err = c.SubHandler(topic, message); err != nil {
				return
			}
		}
		err = handler(topic, message)
	})

	if token.Wait() && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func (c *Client) Disconnect() {
	c.Client.Disconnect(250)
}
