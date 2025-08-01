package main

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"golang.org/x/sync/errgroup"
)

type ProcessorConfig struct {
	Buffer  int
	Workers int
}

type Listener interface {
	enqueue(message *Message)
}

type dedupCache struct {
	cache *expirable.LRU[string, bool]
}

type processFn func(context.Context, *Message, int, *dedupCache) error

type Processor struct {
	queue     chan (*Message)
	wg        errgroup.Group
	config    ProcessorConfig
	processFn processFn
	stat      *Statistics
	dedup     *dedupCache
}

func newProcessor(config *Config) (*Processor, error) {
	processor := &Processor{
		queue:  make(chan *Message, config.Processor.Buffer),
		config: config.Processor,
		stat:   newStatistics(),
		dedup: &dedupCache{
			cache: expirable.NewLRU[string, bool](100000, nil,
				time.Second*time.Duration(240)),
		},
	}

	var processFn processFn
	topic := "solana.transfers.proto" // default
	if t, ok := config.Consumer["topic"].(string); ok {
		topic = t
	}

	switch topic {
	case "solana.dextrades.proto":
		processFn = processor.dexTradesMessageHandler
	case "solana.transactions.proto":
		processFn = processor.transactionsMessageHandler
	case "solana.tokens.proto":
		processFn = processor.tokensMessageHandler
	case "solana.transfers.proto":
		processFn = processor.transfersMessageHandler
	default:
		processFn = processor.jsonMessageHandler
	}

	processor.processFn = processFn
	return processor, nil
}

func (processor *Processor) enqueue(message *Message) {
	processor.queue <- message
}

func (processor *Processor) start(ctx context.Context) {
	counter := 0
	for i := 0; i < processor.config.Workers; i++ {
		processor.wg.Go(func() error {
			i := i
			fmt.Println("Starting worker ", i)
			for {
				select {
				case <-ctx.Done():
					fmt.Println("Done, exiting processor loop worker ", i)
					return nil
				case message := <-processor.queue:
					err := processor.processFn(ctx, message, i, processor.dedup)
					if err != nil {
						fmt.Println("Error processing message", err)
					}
					counter++
					processor.stat.increment()
					if counter%100 == 0 {
						processor.stat.report()
					}
				}
			}
		})
	}
}

func (processor *Processor) close() {
	fmt.Println("Shutting down processor...")
	processor.wg.Wait()
	fmt.Println("Processor stopped")
	processor.stat.report()
}

func (dedup *dedupCache) isDuplicated(slot uint64, index uint32) bool {
	key := fmt.Sprintf("%d-%d", slot, index)

	if dedup.cache.Contains(key) {
		return true
	}

	dedup.cache.Add(key, true)
	return false
}

// Message handlers for different topics
func (processor *Processor) dexTradesMessageHandler(ctx context.Context, message *Message, workerID int, dedup *dedupCache) error {
	fmt.Printf("Worker %d: Processing DEX trade message from partition %d at offset %d\n",
		workerID, message.TopicPartition.Partition, message.TopicPartition.Offset)
	// TODO: Implement protobuf parsing for DEX trades
	return nil
}

func (processor *Processor) transactionsMessageHandler(ctx context.Context, message *Message, workerID int, dedup *dedupCache) error {
	fmt.Printf("Worker %d: Processing transaction message from partition %d at offset %d\n",
		workerID, message.TopicPartition.Partition, message.TopicPartition.Offset)
	// TODO: Implement protobuf parsing for transactions
	return nil
}

func (processor *Processor) tokensMessageHandler(ctx context.Context, message *Message, workerID int, dedup *dedupCache) error {
	fmt.Printf("Worker %d: Processing token message from partition %d at offset %d\n",
		workerID, message.TopicPartition.Partition, message.TopicPartition.Offset)
	// TODO: Implement protobuf parsing for tokens
	return nil
}

func (processor *Processor) transfersMessageHandler(ctx context.Context, message *Message, workerID int, dedup *dedupCache) error {
	fmt.Printf("Worker %d: Processing transfer message from partition %d at offset %d\n",
		workerID, message.TopicPartition.Partition, message.TopicPartition.Offset)

	// Check for duplicates using slot and offset as unique identifiers
	if dedup.isDuplicated(uint64(message.TopicPartition.Offset), uint32(message.TopicPartition.Partition)) {
		fmt.Printf("Worker %d: Skipping duplicate message\n", workerID)
		return nil
	}

	// Decode the message value as UTF-8 and print to console
	decodedMessage := string(message.Value)
	fmt.Printf("Worker %d: Transfer message (UTF-8 decoded):\n%s\n", workerID, decodedMessage)

	// Print a separator for better readability
	fmt.Printf("Worker %d: "+strings.Repeat("-", 80)+"\n", workerID)

	// ✅ MESSAGE SUCCESSFULLY PROCESSED - NOW COMMIT TO KAFKA
	if message.commitCallback != nil {
		message.commitCallback()
	}

	return nil
}

func (processor *Processor) jsonMessageHandler(ctx context.Context, message *Message, workerID int, dedup *dedupCache) error {
	fmt.Printf("Worker %d: Processing JSON message from partition %d at offset %d\n",
		workerID, message.TopicPartition.Partition, message.TopicPartition.Offset)

	// Check for duplicates using slot and offset as unique identifiers
	if dedup.isDuplicated(uint64(message.TopicPartition.Offset), uint32(message.TopicPartition.Partition)) {
		fmt.Printf("Worker %d: Skipping duplicate message\n", workerID)
		return nil
	}

	// Decode the message value as UTF-8 and print to console
	decodedMessage := string(message.Value)
	fmt.Printf("Worker %d: Solana Transfer (UTF-8 decoded):\n%s\n", workerID, decodedMessage)

	// Print a separator for better readability
	fmt.Printf("Worker %d: "+strings.Repeat("-", 80)+"\n", workerID)

	// ✅ MESSAGE SUCCESSFULLY PROCESSED - NOW COMMIT TO KAFKA
	if message.commitCallback != nil {
		message.commitCallback()
	}

	return nil
}
