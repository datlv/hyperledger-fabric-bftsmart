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

package solo

import (
	"fmt"
	"time"

	"github.com/hyperledger/fabric/orderer/multichain"
	cb "github.com/hyperledger/fabric/protos/common"
	"github.com/op/go-logging"
)

var logger = logging.MustGetLogger("orderer/solo")

var interval = int64(10000)
var envelopeMeasurementStartTime = int64(-1)
var countEnvelopes = int64(0)

type consenter struct{}

type chain struct {
	support      multichain.ConsenterSupport
	batchTimeout time.Duration
	sendChan     chan *cb.Envelope
	exitChan     chan struct{}
}

// New creates a new consenter for the solo consensus scheme.
// The solo consensus scheme is very simple, and allows only one consenter for a given chain (this process).
// It accepts messages being delivered via Enqueue, orders them, and then uses the blockcutter to form the messages
// into blocks before writing to the given ledger
func New() multichain.Consenter {
	return &consenter{}
}

func (solo *consenter) HandleChain(support multichain.ConsenterSupport, metadata *cb.Metadata) (multichain.Chain, error) {
	return newChain(support), nil
}

func newChain(support multichain.ConsenterSupport) *chain {
	return &chain{
		batchTimeout: support.SharedConfig().BatchTimeout(),
		support:      support,
		sendChan:     make(chan *cb.Envelope),
		exitChan:     make(chan struct{}),
	}
}

func (ch *chain) Start() {
	go ch.main()
}

func (ch *chain) Halt() {
	select {
	case <-ch.exitChan:
		// Allow multiple halts without panic
	default:
		close(ch.exitChan)
	}
}

// Enqueue accepts a message and returns true on acceptance, or false on shutdown
func (ch *chain) Enqueue(env *cb.Envelope) bool {
	select {
	case ch.sendChan <- env:

                        if envelopeMeasurementStartTime == -1 {
                                envelopeMeasurementStartTime = time.Now().UnixNano()
                        }
                        countEnvelopes++
                        if countEnvelopes%interval == 0 {

                                tp := float64(interval*1000000000) / float64(time.Now().UnixNano() - envelopeMeasurementStartTime)
                                fmt.Printf("Throughput = %v envelopes/sec\n", tp)
                                envelopeMeasurementStartTime = time.Now().UnixNano()

                        }

		return true
	case <-ch.exitChan:
		return false
	}
}

func (ch *chain) main() {
	var timer <-chan time.Time

	for {
		select {
		case msg := <-ch.sendChan:

			batches, _, ok := ch.support.BlockCutter().Ordered(msg)
			if ok && len(batches) == 0 && timer == nil {
				timer = time.After(ch.batchTimeout)
				continue
			}
			for _, batch := range batches {
				block := ch.support.CreateNextBlock(batch)

				//JCS: hack to avoid signatures and get pure throughput
				//ch.support.WriteBlock(block, committers[i], nil)
				err := ch.support.AppendBlock(block)
				if err != nil {
					logger.Panicf("Could not append block: %s", err)
				}

			}
			if len(batches) > 0 {
				timer = nil
			}
		case <-timer:
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
		case <-ch.exitChan:
			logger.Debugf("Exiting")
			return
		}
	}
}
