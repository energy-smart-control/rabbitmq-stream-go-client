package stream

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"net/url"
	"sync"
	"time"
)

type TuneState struct {
	requestedMaxFrameSize int
	requestedHeartbeat    int
}

type ClientProperties struct {
	items map[string]string
}

type PublishErrorListener func(publisherId uint8, publishingId int64, code uint16, errorMessage string)
type metadataListener func(ch <-chan string)

type Client struct {
	socket               socket
	destructor           *sync.Once
	clientProperties     ClientProperties
	tuneState            TuneState
	coordinator          *Coordinator
	PublishErrorListener PublishErrorListener
	broker               Broker
	metadataListener     metadataListener
}

func NewClient() *Client {
	return &Client{
		coordinator: NewCoordinator(),
		broker:      newBrokerDefault(),
		destructor:  &sync.Once{},
	}
}

func (c *Client) connect() error {
	c.broker.GetUri()
	u, err := url.Parse(c.broker.GetUri())
	if err != nil {
		return err
	}
	host, port := u.Hostname(), u.Port()

	c.tuneState.requestedHeartbeat = 60
	c.tuneState.requestedMaxFrameSize = 1048576
	c.clientProperties.items = make(map[string]string)
	resolver, err := net.ResolveTCPAddr("tcp", net.JoinHostPort(host, port))
	if err != nil {
		logDebug("%s", err)
		return err
	}
	connection, err2 := net.DialTCP("tcp", nil, resolver)
	if err2 != nil {
		logDebug("%s", err2)
		return err2
	}
	err2 = connection.SetReadBuffer(defaultReadSocketBuffer)
	if err2 != nil {
		logDebug("%s", err2)
		return err2
	}
	err2 = connection.SetWriteBuffer(defaultReadSocketBuffer)
	if err2 != nil {
		logDebug("%s", err2)
		return err2
	}

	c.socket = socket{connection: connection, mutex: &sync.Mutex{},
		writer:     bufio.NewWriter(connection),
		destructor: &sync.Once{},
	}
	c.socket.SetConnect(true)

	go c.handleResponse()
	err2 = c.peerProperties()

	if err2 != nil {
		logDebug("%s", err2)
		return err2
	}
	pwd, _ := u.User.Password()
	err2 = c.authenticate(u.User.Username(), pwd)
	if err2 != nil {
		logDebug("User:%s, %s", u.User.Username(), err2)
		return err2
	}
	vhost := "/"
	if len(u.Path) > 1 {
		vhost, _ = url.QueryUnescape(u.Path[1:])
	}
	err2 = c.open(vhost)
	if err2 != nil {
		logDebug("%s", err2)
		return err2
	}
	c.heartBeat()
	logDebug("User %s, connected to: %s, vhost:%s", u.User.Username(),
		net.JoinHostPort(host, port),
		vhost)
	return nil
}

func (c *Client) peerProperties() error {
	clientPropertiesSize := 4 // size of the map, always there

	c.clientProperties.items["connection_name"] = "rabbitmq-StreamOptions-locator"
	c.clientProperties.items["product"] = "RabbitMQ Stream"
	c.clientProperties.items["copyright"] = "Copyright (c) 2021 VMware, Inc. or its affiliates."
	c.clientProperties.items["information"] = "Licensed under the MPL 2.0. See https://www.rabbitmq.com/"
	c.clientProperties.items["version"] = ClientVersion
	c.clientProperties.items["platform"] = "Golang"
	for key, element := range c.clientProperties.items {
		clientPropertiesSize = clientPropertiesSize + 2 + len(key) + 2 + len(element)
	}

	length := 2 + 2 + 4 + clientPropertiesSize
	resp := c.coordinator.NewResponse(commandPeerProperties)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandPeerProperties,
		correlationId)
	writeInt(b, len(c.clientProperties.items))

	for key, element := range c.clientProperties.items {
		writeString(b, key)
		writeString(b, element)
	}

	return c.handleWrite(b.Bytes(), resp)
}

func (c *Client) authenticate(user string, password string) error {

	saslMechanisms, err := c.getSaslMechanisms()
	if err != nil {
		return err
	}
	saslMechanism := ""
	for i := 0; i < len(saslMechanisms); i++ {
		if saslMechanisms[i] == "PLAIN" {
			saslMechanism = "PLAIN"
		}
	}
	response := unicodeNull + user + unicodeNull + password
	saslResponse := []byte(response)
	return c.sendSaslAuthenticate(saslMechanism, saslResponse)
}

func (c *Client) getSaslMechanisms() ([]string, error) {
	length := 2 + 2 + 4
	resp := c.coordinator.NewResponse(commandSaslHandshake)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandSaslHandshake,
		correlationId)

	errWrite := c.socket.writeAndFlush(b.Bytes())
	data := <-resp.data
	err := c.coordinator.RemoveResponseById(correlationId)
	if err != nil {
		return nil, err
	}
	if errWrite != nil {
		return nil, errWrite
	}
	return data.([]string), nil

}

func (c *Client) sendSaslAuthenticate(saslMechanism string, challengeResponse []byte) error {
	length := 2 + 2 + 4 + 2 + len(saslMechanism) + 4 + len(challengeResponse)
	resp := c.coordinator.NewResponse(commandSaslAuthenticate)
	respTune := c.coordinator.NewResponseWitName("tune")
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandSaslAuthenticate,
		correlationId)

	writeString(b, saslMechanism)
	writeInt(b, len(challengeResponse))
	b.Write(challengeResponse)
	err := c.handleWrite(b.Bytes(), resp)
	if err != nil {
		return err
	}
	// double read for TUNE
	tuneData := <-respTune.data
	err = c.coordinator.RemoveResponseByName("tune")
	if err != nil {
		return err
	}

	return c.socket.writeAndFlush(tuneData.([]byte))
}

func (c *Client) open(virtualHost string) error {
	length := 2 + 2 + 4 + 2 + len(virtualHost)
	resp := c.coordinator.NewResponse(commandOpen, virtualHost)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandOpen,
		correlationId)
	writeString(b, virtualHost)
	return c.handleWrite(b.Bytes(), resp)
}

func (c *Client) DeleteStream(streamName string) error {
	length := 2 + 2 + 4 + 2 + len(streamName)
	resp := c.coordinator.NewResponse(commandDeleteStream, streamName)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandDeleteStream,
		correlationId)

	writeString(b, streamName)

	return c.handleWrite(b.Bytes(), resp)
}

func (c *Client) heartBeat() {

	ticker := time.NewTicker(60 * time.Second)
	resp := c.coordinator.NewResponseWitName("heartbeat")
	go func() {
		for {
			select {
			case code := <-resp.code:
				if code.id == closeChannel {
					_ = c.coordinator.RemoveResponseByName("heartbeat")
				}
				return
			case <-ticker.C:
				c.sendHeartbeat()
			}
		}
	}()
}

func (c *Client) sendHeartbeat() {
	length := 4
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandHeartbeat)
	_ = c.socket.writeAndFlush(b.Bytes())
}

func (c *Client) closeHartBeat() {
	c.destructor.Do(func() {
		r, err := c.coordinator.GetResponseByName("heartbeat")
		if err != nil {
			logWarn("error removing heartbeat: %s", err)
		} else {
			r.code <- Code{id: closeChannel}
		}
	})

}

func (c *Client) Close() error {
	for _, p := range c.coordinator.producers {
		err := c.coordinator.RemoveProducerById(p.(*Producer).ID)
		if err != nil {
			logWarn("error removing producer: %s", err)
		}
	}
	for _, cs := range c.coordinator.consumers {
		err := c.coordinator.RemoveProducerById(cs.(*Consumer).ID)
		if err != nil {
			logWarn("error removing consumer: %s", err)
		}
	}
	var err error
	if c.socket.isOpen() {
		c.closeHartBeat()
		res := c.coordinator.NewResponse(commandClose)
		length := 2 + 2 + 4 + 2
		var b = bytes.NewBuffer(make([]byte, 0, length))
		writeProtocolHeader(b, length, int16(uShortEncodeResponseCode(commandClose)), res.correlationid)
		writeUShort(b, responseCodeOk)

		err = c.socket.writeAndFlush(b.Bytes())
		if err != nil {
			logWarn("error during send client close %s", err)
		}
		_ = c.coordinator.RemoveResponseById(res.correlationid)
	}

	c.socket.shutdown(nil)
	return err
}

func (c *Client) DeclarePublisher(streamName string) (*Producer, error) {
	producer, err := c.coordinator.NewProducer(&ProducerOptions{
		client:     c,
		streamName: streamName,
	})
	if err != nil {
		return nil, err
	}
	publisherReferenceSize := 0
	length := 2 + 2 + 4 + 1 + 2 + publisherReferenceSize + 2 + len(streamName)
	resp := c.coordinator.NewResponse(commandDeclarePublisher, streamName)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandDeclarePublisher,
		correlationId)

	writeByte(b, producer.ID)
	writeShort(b, int16(publisherReferenceSize))
	writeString(b, streamName)
	res := c.handleWrite(b.Bytes(), resp)
	return producer, res
}

func (c *Client) metaData(streams ...string) *StreamsMetadata {

	length := 2 + 2 + 4 + 4 // API code, version, correlation ID, size of array
	for _, stream := range streams {
		length += 2
		length += len(stream)

	}
	resp := c.coordinator.NewResponse(commandMetadata)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandMetadata,
		correlationId)

	writeInt(b, len(streams))
	for _, stream := range streams {
		writeString(b, stream)
	}

	err := c.handleWrite(b.Bytes(), resp)
	if err != nil {
		return nil
	}

	data := <-resp.data
	return data.(*StreamsMetadata)
}

func (c *Client) BrokerLeader(stream string) (*Broker, error) {
	streamsMetadata := c.metaData(stream)
	if streamsMetadata == nil {
		return nil, fmt.Errorf("leader error for stream for stream: %s", stream)
	}

	streamMetadata := streamsMetadata.Get(stream)
	if streamMetadata.responseCode != responseCodeOk {
		return nil, fmt.Errorf("leader error for stream: %s, error:%s", stream, lookErrorCode(streamMetadata.responseCode))
	}
	return streamMetadata.Leader, nil
}

func (c *Client) DeclareStream(streamName string, options *StreamOptions) error {
	if streamName == "" {
		return fmt.Errorf("stream name can't be empty")
	}

	resp := c.coordinator.NewResponse(commandCreateStream, streamName)
	length := 2 + 2 + 4 + 2 + len(streamName) + 4
	correlationId := resp.correlationid
	if options == nil {
		options = NewStreamOptions()
	}

	args, err := options.buildParameters()
	if err != nil {
		_ = c.coordinator.RemoveResponseById(resp.correlationid)
		return err
	}
	for key, element := range args {
		length = length + 2 + len(key) + 2 + len(element)
	}
	var b = bytes.NewBuffer(make([]byte, 0, length))
	writeProtocolHeader(b, length, commandCreateStream,
		correlationId)
	writeString(b, streamName)
	writeInt(b, len(args))

	for key, element := range args {
		writeString(b, key)
		writeString(b, element)
	}

	return c.handleWrite(b.Bytes(), resp)

}

func (c *Client) DeclareSubscriber(streamName string, messagesHandler MessagesHandler, options *ConsumerOptions) (*Consumer, error) {
	options.client = c
	options.streamName = streamName
	consumer := c.coordinator.NewConsumer(messagesHandler, options)
	length := 2 + 2 + 4 + 1 + 2 + len(streamName) + 2 + 2
	if options.Offset.isOffset() ||
		options.Offset.isTimestamp() {
		length += 8
	}

	if options.Offset.isLastConsumed() {
		lastOffset, err := consumer.QueryOffset()
		if err != nil {
			_ = c.coordinator.RemoveConsumerById(consumer.ID)
			return nil, err
		}
		options.Offset.offset = lastOffset
		// here we change the type since typeLastConsumed is not part of the protocol
		options.Offset.typeOfs = typeOffset
	}
	resp := c.coordinator.NewResponse(commandSubscribe, streamName)
	correlationId := resp.correlationid
	var b = bytes.NewBuffer(make([]byte, 0, length+4))
	writeProtocolHeader(b, length, commandSubscribe,
		correlationId)
	writeByte(b, consumer.ID)

	writeString(b, streamName)

	writeShort(b, options.Offset.typeOfs)

	if options.Offset.isOffset() ||
		options.Offset.isTimestamp() {
		writeLong(b, options.Offset.offset)
	}
	writeShort(b, 10)

	res := c.handleWrite(b.Bytes(), resp)

	go func() {
		for {
			select {
			case code := <-consumer.response.code:
				if code.id == closeChannel {

					return
				}

			case data := <-consumer.response.data:
				consumer.setOffset(data.(int64))

			case messages := <-consumer.response.messages:
				for _, message := range messages {
					consumer.messagesHandler(ConsumerContext{Consumer: consumer}, message)
				}
			}
		}
	}()
	return consumer, res
}