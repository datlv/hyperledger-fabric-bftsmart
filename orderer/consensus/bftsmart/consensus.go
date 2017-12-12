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

	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/op/go-logging"

	"encoding/binary"
	"io"
	"net"

	localconfig "github.com/hyperledger/fabric/orderer/common/localconfig"
	"github.com/hyperledger/fabric/orderer/consensus" //JCS: not used anymore
	"github.com/hyperledger/fabric/protos/utils"
)

var logger = logging.MustGetLogger("orderer/bftsmart")
var poolsize uint = 0
var poolindex uint = 0
var recvport uint = 0
var sendProxy net.Conn //JCS: my code, to send data to proxy
var sendPool []net.Conn
var mutex []*sync.Mutex
var batchTimeout time.Duration

//measurements
var interval = int64(10000)
var envelopeMeasurementStartTime = int64(-1)
var countEnvelopes = int64(0)

type consenter struct {
	createSystemChannel bool
}

type chain struct {
	recvProxy       net.Conn //JCS: my code, to receive data to proxy
	isSystemChannel bool

	support         consensus.ConsenterSupport
	sendChanRegular chan *cb.Block
	sendChanConfig  chan *cb.Block
	exitChan        chan struct{}
}

// New creates a new consenter for the solo consensus scheme.
// The solo consensus scheme is very simple, and allows only one consenter for a given chain (this process).
// It accepts messages being delivered via Enqueue, orders them, and then uses the blockcutter to form the messages
// into blocks before writing to the given ledger
func New(config localconfig.BFTsmart) consensus.Consenter {
	poolsize = config.ConnectionPoolSize
	recvport = config.RecvPort
	return &consenter{
		createSystemChannel: true,
	}
}

func (bftsmart *consenter) HandleChain(support consensus.ConsenterSupport, metadata *cb.Metadata) (consensus.Chain, error) {
	isSysChan := bftsmart.createSystemChannel
	bftsmart.createSystemChannel = false
	return newChain(isSysChan, support), nil
}

func newChain(isSysChan bool, support consensus.ConsenterSupport) *chain {

	return &chain{
		support:         support,
		isSystemChannel: isSysChan,

		sendChanRegular: make(chan *cb.Block),
		sendChanConfig:  make(chan *cb.Block),
		exitChan:        make(chan struct{}),
	}

}

func (ch *chain) Start() {

	logger.Infof("Starting new bftsmart chain with ID '%s'\n", ch.support.ChainID())

	if ch.isSystemChannel {

		conn, err := net.Dial("unix", "/tmp/hlf-pool.sock")

		if err != nil {
			logger.Info("[SEND] Error while connecting to proxy:", err)
			return
		}

		sendProxy = conn

		sendPool = make([]net.Conn, poolsize)
		mutex = make([]*sync.Mutex, poolsize)

		//create connection pool
		for i := uint(0); i < poolsize; i++ {

			conn, err := net.Dial("unix", "/tmp/hlf-pool.sock")

			if err != nil {
				panic(fmt.Sprintf("Could not create connection %v: %d\n", i, err))
				//return
			} else {
				logger.Info("Created connection: %v\n", i)
				//conn.SetNoDelay(true)
				sendPool[i] = conn
				mutex[i] = &sync.Mutex{}
			}
		}

		batchTimeout = ch.support.SharedConfig().BatchTimeout()

		//JCS: Sending pool size
		/*_, err = ch.sendUint32(uint32(poolsize))

		if err != nil {
			logger.Debugf("Error while sending pool size:", err)
			return
		}*/

		//JCS: Sending batch configuration
		_, err = sendUint32(ch.support.SharedConfig().BatchSize().PreferredMaxBytes, sendProxy)

		if err != nil {
			logger.Info("Error while sending PreferredMaxBytes:", err)
			return
		}

		_, err = sendUint32(ch.support.SharedConfig().BatchSize().MaxMessageCount, sendProxy)

		if err != nil {
			logger.Info("Error while sending MaxMessageCount:", err)
			return
		}
		_, err = sendUint64(uint64(time.Duration.Nanoseconds(batchTimeout)), sendProxy)

		if err != nil {
			logger.Info("Error while sending BatchTimeout:", err)
			return
		}

	}

	addr := fmt.Sprintf("localhost:%d", recvport)
	conn, err := net.Dial("tcp", addr)

	if err != nil {
		logger.Info("[RECV] Error while connecting to proxy:", err)
		return
	}

	ch.recvProxy = conn

	_, err = sendString(ch.support.ChainID(), sendProxy)

	if err != nil {
		logger.Info("Error while sending chain ID:", err)
		return
	}

	lastBlock := ch.support.GetLastBlock()
	header := lastBlock.Header

	_, err = sendHeaderToBFTProxy(header)

	if err != nil {
		logger.Info("Error while sending chain ID:", err)
		return
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

func (ch *chain) WaitReady() error {
	return nil
}

// Errored only closes on exit
func (ch *chain) Errored() <-chan struct{} {
	return ch.exitChan
}

func sendLength(length int, conn net.Conn) (int, error) {

	var buf [8]byte

	binary.BigEndian.PutUint64(buf[:], uint64(length))

	return conn.Write(buf[:])
}

func sendUint64(length uint64, conn net.Conn) (int, error) {

	var buf [8]byte

	binary.BigEndian.PutUint64(buf[:], uint64(length))

	return conn.Write(buf[:])
}

func sendUint32(length uint32, conn net.Conn) (int, error) {

	var buf [4]byte

	binary.BigEndian.PutUint32(buf[:], uint32(length))

	return conn.Write(buf[:])
}

func sendBoolean(boolean bool, conn net.Conn) (int, error) {

	var buf [1]byte

	if boolean {
		buf[0] = 1
	} else {
		buf[0] = 0
	}

	status, err := sendLength(1, conn)

	if err != nil {
		return status, err
	}

	return conn.Write(buf[:])

}

func sendString(str string, conn net.Conn) (int, error) {

	status, err := sendLength(len(str), conn)

	if err != nil {
		return status, err
	}

	return conn.Write([]byte(str))

}

func sendBytes(bytes []byte, conn net.Conn) (int, error) {

	status, err := sendLength(len(bytes), conn)

	if err != nil {
		return status, err
	}

	return conn.Write(bytes)

}

func sendEnvToBFTProxy(isConfig bool, chainID string, env *cb.Envelope, index uint) (int, error) {

	mutex[index].Lock()

	//send channel id
	status, err := sendString(chainID, sendPool[index])

	//send isConfig
	status, err = sendBoolean(isConfig, sendPool[index])

	//send envelope
	bytes, err := utils.Marshal(env)
	if err != nil {
		return -1, err
	}
	status, err = sendBytes(bytes, sendPool[index])

	mutex[index].Unlock()

	return status, err
}

func sendHeaderToBFTProxy(header *cb.BlockHeader) (int, error) {
	bytes, err := utils.Marshal(header)

	if err != nil {
		return -1, err
	}

	status, err := sendLength(len(bytes), sendProxy)

	if err != nil {
		return status, err
	}

	return sendProxy.Write(bytes)
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

// Order accepts a message and returns true on acceptance, or false on shutdown
func (ch *chain) Order(env *cb.Envelope, configSeq uint64) error {

	//perform usual msg processing
	seq := ch.support.Sequence()

	if configSeq < seq {
		_, err := ch.support.ProcessNormalMsg(env)
		if err != nil {
			logger.Warningf("Discarding bad normal message: %s", err)
			return nil
		}
	}

	//if everything ok, proceed
	poolindex = (poolindex + 1) % poolsize

	_, err := sendEnvToBFTProxy(false, ch.support.ChainID(), env, poolindex)

	if err != nil {

		return err
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
		return fmt.Errorf("Exiting")
	default: //JCS: avoid blocking
		return nil
	}

	//return true
}

// Configure accepts configuration update messages for ordering (JCS: for the moment, this orderer doe not support this feature)
func (ch *chain) Configure(config *cb.Envelope, configSeq uint64) error {

	//perform usual config processing
	seq := ch.support.Sequence()
	msg := config
	if configSeq < seq {
		configMsg, _, err := ch.support.ProcessConfigMsg(config)
		if err != nil {
			logger.Warningf("Discarding bad config message: %s", err)
			return nil
		}
		msg = configMsg
	}

	//if everything ok, proceed
	poolindex = (poolindex + 1) % poolsize

	_, err := sendEnvToBFTProxy(true, ch.support.ChainID(), msg, poolindex)

	if err != nil {

		return err
	}

	select {

	case <-ch.exitChan:
		return fmt.Errorf("Exiting")
	default: //JCS: avoid blocking
		return nil
	}

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

		//JCS receive block type
		bytes, err = ch.recvBytes()
		if err != nil {
			logger.Debugf("[recv] Error while receiving block type from BFT proxy: %v\n", err)
			continue
		}

		if bytes[0] == 1 {

			logger.Infof("[recv] Received config block! \n")
			ch.sendChanConfig <- block
		} else {

			ch.sendChanRegular <- block
		}

	}
}

func (ch *chain) appendToChain() {
	//var timer <-chan time.Time //JCS: original timer to flush the blockcutter

	for {

		select {

		//JCS: I want the orderer to wait for reception from the java proxy
		case block := <-ch.sendChanRegular:

			err := ch.support.AppendBlock(block)
			if err != nil {
				logger.Panicf("Could not append regular block: %s", err)
			}

		case block := <-ch.sendChanConfig:

			ch.support.ProcessConfigBlock(block)
			err := ch.support.AppendBlock(block)
			if err != nil {
				logger.Panicf("Could not append configuration block: %s", err)
			}

		case <-ch.exitChan:
			logger.Debugf("Exiting...")
			return
		}
	}
}
