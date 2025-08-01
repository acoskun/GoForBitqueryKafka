package main

import (
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"
	"golang.org/x/sync/errgroup"
)

type Consumer interface {
	waitMessages(ctx context.Context, listener Listener)
	close()
}

type SimpleConsumer struct {
	consumer sarama.Consumer
}

type ConsumerConfig struct {
	Topic       string
	Partitioned bool
}

type PartitionedConsumer struct {
	consumers []sarama.Consumer
}

// XDGSCRAMClient wraps the xdg-go/scram client to implement sarama.SCRAMClient
type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	HashGeneratorFcn scram.HashGeneratorFcn
}

func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

var SHA512 = scram.SHA512

func newConsumer(config *Config) (Consumer, error) {
	// First try ConsumerGroup since Python uses group.id
	fmt.Println("🔄 Trying ConsumerGroup approach first...")
	consumerGroup, err := newConsumerGroupConsumer(config)
	if err != nil {
		fmt.Printf("⚠️  ConsumerGroup failed: %v\n", err)
		fmt.Println("🔄 Falling back to Simple Consumer...")

		// Check if partitioned mode is enabled
		partitioned := false
		if p, ok := config.Consumer["partitioned"].(bool); ok {
			partitioned = p
		}

		if partitioned {
			return newPartitionedConsumer(config)
		}
		return newSimpleConsumer(config)
	}
	return consumerGroup, nil
}

func newSimpleConsumer(config *Config) (*SimpleConsumer, error) {
	// Disable Sarama debug logging to reduce noise
	// sarama.Logger = log.New(os.Stdout, "[SARAMA] ", log.LstdFlags)

	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Version = sarama.V1_1_0_0     // Try middle version
	saramaConfig.ClientID = "GoKafkaTransfers" // Set proper ClientID

	// Configure network settings to match Python client
	saramaConfig.Net.DialTimeout = 30 * time.Second
	saramaConfig.Net.ReadTimeout = 30 * time.Second
	saramaConfig.Net.WriteTimeout = 30 * time.Second
	saramaConfig.Net.KeepAlive = 30 * time.Second

	// Disable ApiVersions request which causes issues with Bitquery
	saramaConfig.ApiVersionsRequest = false

	// Configure consumer settings for backfill support
	offsetReset := "latest" // default
	if reset, ok := config.Consumer["auto.offset.reset"].(string); ok {
		offsetReset = reset
	}

	if offsetReset == "earliest" {
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest // Backfill from beginning
		fmt.Println("🔄 BACKFILL MODE: Starting from earliest available messages")
	} else {
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest // Only new messages
		fmt.Println("📡 LIVE MODE: Starting from latest messages only")
	}

	saramaConfig.Consumer.Group.Session.Timeout = 30 * time.Second // session.timeout.ms: 30000
	saramaConfig.Consumer.Group.Heartbeat.Interval = 3 * time.Second
	saramaConfig.Consumer.MaxProcessingTime = 30 * time.Second

	// NO TLS for SASL_PLAINTEXT
	saramaConfig.Net.TLS.Enable = false

	// Debug: Print all config values
	fmt.Printf("🔧 Debug - Config Kafka map: %+v\n", config.Kafka)

	// Enable SCRAM-SHA-512 authentication
	if username, ok := config.Kafka["sasl.username"].(string); ok {
		fmt.Printf("Configuring SASL authentication for user: %s\n", username)
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		saramaConfig.Net.SASL.User = username
		saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
		}
	} else {
		fmt.Println("❌ No username found in config")
	}

	if password, ok := config.Kafka["sasl.password"].(string); ok {
		saramaConfig.Net.SASL.Password = password
		fmt.Printf("Password configured: %s\n", strings.Repeat("*", len(password)))
	} else {
		fmt.Println("❌ No password found in config")
	}

	// Configure group ID if provided
	if groupID, ok := config.Kafka["group.id"].(string); ok {
		saramaConfig.ClientID = groupID
		fmt.Printf("Group ID configured: %s\n", groupID)
	}

	if serversStr, ok := config.Kafka["bootstrap.servers"].(string); ok {
		// Parse comma-separated server list
		servers := strings.Split(serversStr, ",")
		for i, server := range servers {
			servers[i] = strings.TrimSpace(server)
		}

		fmt.Printf("Connecting to Kafka servers: %v (SASL_PLAINTEXT)\n", servers)
		fmt.Printf("SASL Config: Enable=%v, Mechanism=%v, User=%v\n",
			saramaConfig.Net.SASL.Enable, saramaConfig.Net.SASL.Mechanism, saramaConfig.Net.SASL.User)
		fmt.Printf("TLS Config: Enable=%v\n", saramaConfig.Net.TLS.Enable)
		fmt.Printf("Consumer Config: InitialOffset=%v, SessionTimeout=%v\n",
			saramaConfig.Consumer.Offsets.Initial, saramaConfig.Consumer.Group.Session.Timeout)

		// Test network connectivity first
		fmt.Println("🔌 Testing network connectivity...")
		for _, server := range servers {
			conn, err := net.DialTimeout("tcp", server, 5*time.Second)
			if err != nil {
				fmt.Printf("❌ Cannot connect to %s: %v\n", server, err)
			} else {
				fmt.Printf("✅ Network connection to %s successful\n", server)
				conn.Close()
			}
		}

		consumer, err := sarama.NewConsumer(servers, saramaConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create consumer: %v", err)
		}

		fmt.Println("✅ Consumer created successfully")
		return &SimpleConsumer{consumer: consumer}, nil
	}
	return nil, fmt.Errorf("bootstrap.servers not found in config")
}

func (consumer *SimpleConsumer) close() {
	consumer.consumer.Close()
}

func (consumer *SimpleConsumer) waitMessages(ctx context.Context, listener Listener) {
	topicName := "solana.transfers"
	fmt.Printf("🔍 Attempting to subscribe to topic: %s\n", topicName)

	// First, get topic metadata to check if topic exists
	topics, err := consumer.consumer.Topics()
	if err != nil {
		fmt.Printf("❌ Error getting topics: %v\n", err)
		return
	}

	fmt.Printf("📋 Available topics (%d): %v\n", len(topics), topics)

	// Check if our topic exists
	topicExists := false
	for _, topic := range topics {
		if topic == topicName {
			topicExists = true
			break
		}
	}

	if !topicExists {
		fmt.Printf("❌ Topic '%s' does not exist!\n", topicName)
		return
	}

	fmt.Printf("✅ Topic '%s' found\n", topicName)

	// Get partitions for the topic
	partitions, err := consumer.consumer.Partitions(topicName)
	if err != nil {
		fmt.Printf("❌ Error getting partitions for topic %s: %v\n", topicName, err)
		return
	}

	fmt.Printf("📊 Topic '%s' has %d partitions: %v\n", topicName, len(partitions), partitions)

	if len(partitions) == 0 {
		fmt.Printf("❌ No partitions found for topic '%s'\n", topicName)
		return
	}

	// Try to consume from the first partition
	partition := partitions[0]
	fmt.Printf("🎯 Attempting to consume from partition %d\n", partition)

	partitionConsumer, err := consumer.consumer.ConsumePartition(topicName, partition, sarama.OffsetNewest)
	if err != nil {
		fmt.Printf("❌ Error creating partition consumer for partition %d: %v\n", partition, err)
		return
	}
	defer partitionConsumer.Close()

	fmt.Printf("✅ Successfully subscribed to topic '%s', partition %d\n", topicName, partition)
	fmt.Println("⏳ Waiting for messages...")

	// Add a timeout to check if we're receiving anything
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	messageCount := 0

	for {
		select {
		case <-ctx.Done():
			fmt.Println("🛑 Context cancelled, exiting consumer loop")
			return
		case <-ticker.C:
			fmt.Printf("⏰ Still waiting... Messages received so far: %d\n", messageCount)
		case msg := <-partitionConsumer.Messages():
			messageCount++
			fmt.Printf("📨 Received message #%d from partition %d, offset %d\n",
				messageCount, msg.Partition, msg.Offset)
			listener.enqueue(&Message{Value: msg.Value, Key: msg.Key, TopicPartition: TopicPartition{Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset}})
		case err := <-partitionConsumer.Errors():
			fmt.Printf("❌ Consumer error: %v\n", err)
		}
	}
}

func newPartitionedConsumer(config *Config) (Consumer, error) {
	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Version = sarama.V2_8_0_0 // Set Kafka version

	// Enable TLS with certificates
	// tlsConfig, err := loadTLSConfig() // Removed loadTLSConfig
	// if err != nil {
	// 	return nil, fmt.Errorf("failed to load TLS config: %v", err)
	// }

	// saramaConfig.Net.TLS.Enable = true
	// saramaConfig.Net.TLS.Config = tlsConfig

	// Enable SCRAM-SHA-512 authentication
	if username, ok := config.Kafka["sasl.username"].(string); ok {
		fmt.Printf("Configuring SASL authentication for user: %s\n", username)
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		saramaConfig.Net.SASL.User = username
		saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
		}
	}
	if password, ok := config.Kafka["sasl.password"].(string); ok {
		saramaConfig.Net.SASL.Password = password
		fmt.Printf("Password configured: %s\n", strings.Repeat("*", len(password)))
	}

	if serversStr, ok := config.Kafka["bootstrap.servers"].(string); ok {
		// Parse comma-separated server list
		servers := strings.Split(serversStr, ",")
		for i, server := range servers {
			servers[i] = strings.TrimSpace(server)
		}

		fmt.Printf("Connecting to Kafka servers: %v\n", servers)

		consumer, err := sarama.NewConsumer(servers, saramaConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create consumer: %v", err)
		}
		return &PartitionedConsumer{consumers: []sarama.Consumer{consumer}}, nil
	}
	return nil, fmt.Errorf("bootstrap.servers not found in config")
}

func (consumer *PartitionedConsumer) close() {
	for _, c := range consumer.consumers {
		c.Close()
	}
}

func (consumer *PartitionedConsumer) waitMessages(ctx context.Context, listener Listener) {
	var wg errgroup.Group
	for _, c := range consumer.consumers {
		c := c
		wg.Go(func() error {
			partitionConsumer, err := c.ConsumePartition("solana.transfers", 0, sarama.OffsetNewest)
			if err != nil {
				return err
			}
			defer partitionConsumer.Close()

			for {
				select {
				case <-ctx.Done():
					return nil
				case msg := <-partitionConsumer.Messages():
					listener.enqueue(&Message{Value: msg.Value, Key: msg.Key, TopicPartition: TopicPartition{Topic: msg.Topic, Partition: msg.Partition, Offset: msg.Offset}})
				case err := <-partitionConsumer.Errors():
					fmt.Println("Error:", err)
				}
			}
		})
	}
	wg.Wait()
}

// ConsumerGroup consumer implementation
type ConsumerGroupConsumer struct {
	consumerGroup sarama.ConsumerGroup
}

func newConsumerGroupConsumer(config *Config) (*ConsumerGroupConsumer, error) {
	// Disable Sarama debug logging to reduce noise
	// sarama.Logger = log.New(os.Stdout, "[SARAMA] ", log.LstdFlags)

	saramaConfig := sarama.NewConfig()
	saramaConfig.Consumer.Return.Errors = true
	saramaConfig.Version = sarama.V1_1_0_0     // Try middle version
	saramaConfig.ClientID = "GoKafkaTransfers" // Set proper ClientID

	// Configure network settings to match Python client
	saramaConfig.Net.DialTimeout = 30 * time.Second
	saramaConfig.Net.ReadTimeout = 30 * time.Second
	saramaConfig.Net.WriteTimeout = 30 * time.Second
	saramaConfig.Net.KeepAlive = 30 * time.Second

	// Disable ApiVersions request which causes issues with Bitquery
	saramaConfig.ApiVersionsRequest = false

	// Configure consumer settings for backfill support
	offsetReset := "latest" // default
	if reset, ok := config.Consumer["auto.offset.reset"].(string); ok {
		offsetReset = reset
	}

	if offsetReset == "earliest" {
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest // Backfill from beginning
		fmt.Println("🔄 BACKFILL MODE: Starting from earliest available messages")
	} else {
		saramaConfig.Consumer.Offsets.Initial = sarama.OffsetNewest // Only new messages
		fmt.Println("📡 LIVE MODE: Starting from latest messages only")
	}

	saramaConfig.Consumer.Group.Session.Timeout = 30 * time.Second // session.timeout.ms: 30000
	saramaConfig.Consumer.Group.Heartbeat.Interval = 3 * time.Second
	saramaConfig.Consumer.MaxProcessingTime = 30 * time.Second

	// NO TLS for SASL_PLAINTEXT
	saramaConfig.Net.TLS.Enable = false

	// SASL configuration
	if username, ok := config.Kafka["sasl.username"].(string); ok {
		saramaConfig.Net.SASL.Enable = true
		saramaConfig.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		saramaConfig.Net.SASL.User = username
		saramaConfig.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
		}
	}

	if password, ok := config.Kafka["sasl.password"].(string); ok {
		saramaConfig.Net.SASL.Password = password
	}

	var groupID string
	if gid, ok := config.Kafka["group.id"].(string); ok {
		groupID = gid
	} else {
		return nil, fmt.Errorf("group.id is required for ConsumerGroup")
	}

	if serversStr, ok := config.Kafka["bootstrap.servers"].(string); ok {
		servers := strings.Split(serversStr, ",")
		for i, server := range servers {
			servers[i] = strings.TrimSpace(server)
		}

		fmt.Printf("Creating ConsumerGroup with groupID: %s (SASL_PLAINTEXT)\n", groupID)

		consumerGroup, err := sarama.NewConsumerGroup(servers, groupID, saramaConfig)
		if err != nil {
			return nil, fmt.Errorf("failed to create consumer group: %v", err)
		}

		fmt.Println("✅ ConsumerGroup created successfully")
		return &ConsumerGroupConsumer{consumerGroup: consumerGroup}, nil
	}
	return nil, fmt.Errorf("bootstrap.servers not found in config")
}

// ConsumerGroup Handler with callback-based commit
type ConsumerGroupHandler struct {
	listener Listener
	session  sarama.ConsumerGroupSession
}

func (h *ConsumerGroupHandler) Setup(session sarama.ConsumerGroupSession) error {
	h.session = session
	return nil
}

func (h *ConsumerGroupHandler) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	fmt.Printf("📥 Started consuming partition %d\n", claim.Partition())

	for {
		select {
		case message := <-claim.Messages():
			if message == nil {
				return nil
			}

			// Convert to our Message format with commit callback
			msg := &Message{
				Value: message.Value,
				Key:   message.Key,
				TopicPartition: TopicPartition{
					Topic:     message.Topic,
					Partition: message.Partition,
					Offset:    message.Offset,
				},
				// Add commit callback
				commitCallback: func() {
					session.MarkMessage(message, "")
					fmt.Printf("✅ Message committed: partition=%d, offset=%d\n", message.Partition, message.Offset)
				},
			}

			// Send to listener for processing
			h.listener.enqueue(msg)

		case <-session.Context().Done():
			return nil
		}
	}
}

func (c *ConsumerGroupConsumer) waitMessages(ctx context.Context, listener Listener) {
	handler := &ConsumerGroupHandler{listener: listener}

	// Get topic from config
	topics := []string{"solana.transfers"} // hardcoded for now

	fmt.Printf("🔄 Starting to consume from topics: %v\n", topics)

	for {
		select {
		case <-ctx.Done():
			fmt.Println("❌ Context cancelled, stopping consumer group")
			return
		default:
			// Consume will keep retrying until the context is cancelled
			err := c.consumerGroup.Consume(ctx, topics, handler)
			if err != nil {
				fmt.Printf("❌ Error consuming messages: %v\n", err)
				time.Sleep(time.Second)
			}
		}
	}
}

func (c *ConsumerGroupConsumer) close() {
	if c.consumerGroup != nil {
		c.consumerGroup.Close()
	}
}

// Message wrapper to maintain compatibility
type Message struct {
	Value          []byte
	Key            []byte
	TopicPartition TopicPartition
	commitCallback func()
}

type TopicPartition struct {
	Topic     string
	Partition int32
	Offset    int64
}
