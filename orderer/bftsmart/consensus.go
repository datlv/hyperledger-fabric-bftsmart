/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

                 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package bftsmart

import (
	"fmt"
	"sync"
	"time"

	"github.com/hyperledger/fabric/orderer/multichain"
	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/op/go-logging"

	"encoding/binary"
	"io"
	"net"

	//"github.com/hyperledger/fabric/orderer/common/filter" JCS: not used anymore
	"github.com/hyperledger/fabric/protos/utils"
)

var logger = logging.MustGetLogger("orderer/solo")
var poolsize uint = 0
var poolindex uint = 0
var recvport uint = 0
var sendport uint = 0


//measurements
var interval = int64(10000)
var envelopeMeasurementStartTime = int64(-1)
var countEnvelopes = int64(0)

type consenter struct{}

type chain struct {
	support      multichain.ConsenterSupport
	batchTimeout time.Duration
	sendChan     chan *cb.Block
	exitChan     chan struct{}
	sendProxy    net.Conn //JCS: my code, to send data to proxy
	recvProxy    net.Conn //JCS: my code, to receive data to proxy
	sendPool     []net.Conn
	mutex        []*sync.Mutex
}

// New creates a new consenter for the solo consensus scheme.
// The solo consensus scheme is very simple, and allows only one consenter for a given chain (this process).
// It accepts messages being delivered via Enqueue, orders them, and then uses the blockcutter to form the messages
// into blocks before writing to the given ledger
func New(size uint, send uint, recv uint) multichain.Consenter {
	poolsize = size
	recvport = recv
	sendport = send
	return &consenter{}
}

func (solo *consenter) HandleChain(support multichain.ConsenterSupport, metadata *cb.Metadata) (multichain.Chain, error) {
	return newChain(support), nil
}

func newChain(support multichain.ConsenterSupport) *chain {
	return &chain{
		batchTimeout: support.SharedConfig().BatchTimeout(),
		support:      support,
		sendChan:     make(chan *cb.Block),
		exitChan:     make(chan struct{}),
		sendPool:     make([]net.Conn, poolsize),
		mutex:        make([]*sync.Mutex, poolsize),
	}
}

func (ch *chain) Start() {

	//JCS: my code, to create a connections to the java proxy

	//addr := fmt.Sprintf("localhost:%d", sendport)
	//conn, err := net.Dial("tcp", addr)
	conn, err := net.Dial("unix", "/tmp/bft.sock")

	if err != nil {
		logger.Debugf("Could not connect to proxy!")
		return
	} else {
		logger.Debugf("Connected to proxy!")
	}

	ch.sendProxy = conn

	addr := fmt.Sprintf("localhost:%d", recvport)
	conn, err = net.Dial("tcp", addr)

	if err != nil {
		logger.Debugf("Could not connect to proxy!")
		return
	} else {
		logger.Debugf("Connected to proxy!")
	}

	ch.recvProxy = conn

	//JCS: Sending pool size
	_, err = ch.sendUint32(uint32(poolsize))

	if err != nil {
		logger.Debugf("Error while sending pool size:", err)
		return
	}

	//JCS: Sending batch configuration
	_, err = ch.sendUint32(ch.support.SharedConfig().BatchSize().PreferredMaxBytes)

	if err != nil {
		logger.Debugf("Error while sending PreferredMaxBytes:", err)
		return
	}

	_, err = ch.sendUint32(ch.support.SharedConfig().BatchSize().MaxMessageCount)

	if err != nil {
		logger.Debugf("Error while sending MaxMessageCount:", err)
		return
	}
	_, err = ch.sendUint64(uint64(time.Duration.Nanoseconds(ch.batchTimeout)))

	if err != nil {
		logger.Debugf("Error while sending BatchTimeout:", err)
		return
	}

	lastBlock := ch.support.GetLastBlock()
	header := lastBlock.Header

	//JCS: debug messages
	/*fmt.Println("Showing genesis header number:", header.Number)
	fmt.Print("Showing genesis header previous hash:")
	printBytes(header.PreviousHash)
	fmt.Print("Showing genesis header data hash:")
	printBytes(header.DataHash)

	fmt.Println("Showing genesis header asn1 for number:", header.Number)
	printBytes(header.Bytes())
	fmt.Print("Showing genesis data with length: ", len(lastBlock.Data.Data))

	for i := 0; i < len(lastBlock.Data.Data); i++ {
		printBytes(lastBlock.Data.Data[i])

	}*/

	//sending genesis block
	//fmt.Println("Showing genesis data asn1 for number:", header.Number)
	//printBytes(lastBlock.Data.Bytes())
	ch.sendHeaderToBFTProxy(header)

	//create connection pool
	for i := uint(0); i < poolsize; i++ {
		//addr, _ := net.ResolveTCPAddr("tcp", fmt.Sprintf("localhost:%d", sendport))
		//conn, err := net.DialTCP("tcp", nil, addr)
		conn, err := net.Dial("unix", "/tmp/bft.sock")

		if err != nil {
			logger.Debugf("Could not create connection: %v\n", i)
			return
		} else {
			logger.Debugf("Created connection: %v\n", i)
			//conn.SetNoDelay(true)
			ch.sendPool[i] = conn
			ch.mutex[i] = &sync.Mutex{}
		}
	}

	//JCS: starting loops
	go ch.connLoop() //JCS: my own loop

	go ch.appendToChain()
}

func (ch *chain) Halt() {

	select {
	case <-ch.exitChan:
		// Allow multiple halts without panic
	default:
		close(ch.exitChan)
	}
}

func (ch *chain) sendLength(length int, conn net.Conn) (int, error) {

	var buf [8]byte

	binary.BigEndian.PutUint64(buf[:], uint64(length))

	return conn.Write(buf[:])
}

func (ch *chain) sendUint64(length uint64) (int, error) {

	var buf [8]byte

	binary.BigEndian.PutUint64(buf[:], uint64(length))

	return ch.sendProxy.Write(buf[:])
}

func (ch *chain) sendUint32(length uint32) (int, error) {

	var buf [4]byte

	binary.BigEndian.PutUint32(buf[:], uint32(length))

	return ch.sendProxy.Write(buf[:])
}

func (ch *chain) sendEnvToBFTProxy(env *cb.Envelope, index uint) (int, error) {

	ch.mutex[index].Lock()
	bytes, err := utils.Marshal(env)

	if err != nil {
		return -1, err
	}

	status, err := ch.sendLength(len(bytes), ch.sendPool[index])

	if err != nil {
		return status, err
	}

	i, err := ch.sendPool[index].Write(bytes)

	ch.mutex[index].Unlock()

	return i, err
}

func (ch *chain) sendHeaderToBFTProxy(header *cb.BlockHeader) (int, error) {
	bytes, err := utils.Marshal(header)

	if err != nil {
		return -1, err
	}

	status, err := ch.sendLength(len(bytes), ch.sendProxy)

	if err != nil {
		return status, err
	}

	return ch.sendProxy.Write(bytes)
}

func (ch *chain) recvLength() (int64, error) {

	var size int64
	err := binary.Read(ch.recvProxy, binary.BigEndian, &size)
	return size, err
}

func (ch *chain) recvBytes() ([]byte, error) {

	size, err := ch.recvLength()

	if err != nil {
		return nil, err
	}

	buf := make([]byte, size)

	_, err = io.ReadFull(ch.recvProxy, buf)

	if err != nil {
		return nil, err
	}

	return buf, nil
}

func (ch *chain) recvEnvFromBFTProxy() (*cb.Envelope, error) {

	size, err := ch.recvLength()

	if err != nil {
		return nil, err
	}

	buf := make([]byte, size)

	_, err = io.ReadFull(ch.recvProxy, buf)

	if err != nil {
		return nil, err
	}

	env, err := utils.UnmarshalEnvelope(buf)

	if err != nil {
		return nil, err
	}

	return env, nil
}

/*func printBytes(bytes []byte) {
	fmt.Print("[")
	for _, b := range bytes {
		fmt.Printf("%d, ", int8(b))
	}
	fmt.Println("]")
}*/

// Enqueue accepts a message and returns true on acceptance, or false on shutdown
func (ch *chain) Enqueue(env *cb.Envelope) bool {

	////JCS: new code that contacts the java proxy

	poolindex = (poolindex + 1) % poolsize

	_, err := ch.support.Filters().Apply(env) //JCS: filter before submitting

	if err != nil {
		logger.Debugf("Discarding message: %s", err)
		return true
	}

	_, err = ch.sendEnvToBFTProxy(env, poolindex)

	if err != nil {
		logger.Debugf("[send] Error while sending envelope to BFT proxy: %v\n", err)
		return false
	}


	if envelopeMeasurementStartTime == -1 {
		envelopeMeasurementStartTime = time.Now().UnixNano()
	}
	countEnvelopes++
	if countEnvelopes%interval == 0 {

		tp := float64(interval*1000000000) / float64(time.Now().UnixNano()-envelopeMeasurementStartTime)
		fmt.Printf("Throughput = %v envelopes/sec\n", tp)
		envelopeMeasurementStartTime = time.Now().UnixNano()

	}

	//JCS: I want the orderer to wait for reception on the main loop
	select {

	case <-ch.exitChan:
		return false
	default: //JCS: avoid blocking
		return true
	}

	//return true
}

func (ch *chain) connLoop() {

	for {

		//JCS receive a marshalled block
		bytes, err := ch.recvBytes()
		if err != nil {
			logger.Debugf("[recv] Error while receiving block from BFT proxy: %v\n", err)
			continue
		}

		block, err := utils.GetBlockFromBlockBytes(bytes)
		if err != nil {
			logger.Debugf("[recv] Error while unmarshaling block from BFT proxy: %v\n", err)
			continue
		}

		ch.sendChan <- block

	}
}

func (ch *chain) appendToChain() {
	//var timer <-chan time.Time //JCS: original timer to flush the blockcutter

	for {

		select {

		//JCS: I want the orderer to wait for reception from he java proxy
		case block := <-ch.sendChan:

			//JCS: deal with committers
			for _, msg := range block.Data.Data {

				env, err := utils.UnmarshalEnvelope(msg)

				if err != nil {
					logger.Panicf("Block contains data which is not an envelope structure: %s", err)
				}

				committer, err := ch.support.Filters().Apply(env)
				if err != nil || committer == nil {
					logger.Panicf("Block contains envelopes that should had been rejected during enqueueing: %s", err)
				} else {
					committer.Commit()
				}

			}

			//JCS: orginal code that uses blockcutter
			/*batches, committers, ok := ch.support.BlockCutter().Ordered(msg)
			if ok && len(batches) == 0 && timer == nil {
				timer = time.After(ch.batchTimeout)
				continue
			}
			if len(batches) > 0 {
				timer = nil
			}*/

			//JCS: original code that took an ordered envelope, created a block and appended it to the chain
			/*batches = append(batches, []*cb.Envelope{msg})
			committers = append(committers, committer)
			for i, batch := range batches {
			block := ch.support.CreateNextBlock(batch)
			ch.support.WriteBlock(block, committers, nil)*/

			err := ch.support.AppendBlock(block)
			if err != nil {
				logger.Panicf("Could not append block: %s", err)
			}

			/*fmt.Printf("G BlockHeader bytes #%d: ", block.Header.Number) // JCS: see what the bytes are and compare to proxy
			printBytes(block.Header.Bytes())
			fmt.Printf("G BlockData hash #%d: ", block.Header.Number) // JCS: see what the bytes are and compare to proxy
			printBytes(block.Data.Hash())*/

			//JCS:original code that needed a timer to flush the blockcutter
			/*case <-timer: //JCS: this is no longer triggered, but I'll keep it for reference
			fmt.Println("Testing case timer")

			//clear the timer
			timer = nil

			batch, committers := ch.support.BlockCutter().Cut()
			if len(batch) == 0 {
				logger.Warningf("Batch timer expired with no pending requests, this might indicate a bug")
				continue
			}
			logger.Debugf("Batch timer expired, creating block")
			block := ch.support.CreateNextBlock(batch)
			ch.support.WriteBlock(block, committers, nil)
			fmt.Println("Tested case timer")*/

		case <-ch.exitChan:
			logger.Debugf("Exiting...")
			return
		}
	}
}

