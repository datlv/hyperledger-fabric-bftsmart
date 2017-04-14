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

package main

import (
	"flag"
	"fmt"
	"math"

	"github.com/hyperledger/fabric/common/configtx/tool/provisional"
	"github.com/hyperledger/fabric/common/crypto"    //JCS: import crypto
	"github.com/hyperledger/fabric/common/localmsp"  //JCS: import localmsp
	util "github.com/hyperledger/fabric/common/util" //JCS import utils
	mspmgmt "github.com/hyperledger/fabric/msp/mgmt" //JCS: import mgmt
	"github.com/hyperledger/fabric/orderer/localconfig"
	cb "github.com/hyperledger/fabric/protos/common"
	ab "github.com/hyperledger/fabric/protos/orderer"
	utils "github.com/hyperledger/fabric/protos/utils"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
)

var (
	blocksReceived uint64             = 0                               //JCS: block counter and checker
	N              uint32             = 4                               //JCS: number of ordering nodes
	F              uint32             = 1                               //JCS: number of faults
	Q              float32            = ((float32(N) + float32(F)) / 2) //JCS: quorum size
	signer         crypto.LocalSigner                                   //JCS: local signer
	oldest         = &ab.SeekPosition{Type: &ab.SeekPosition_Oldest{Oldest: &ab.SeekOldest{}}}
	newest         = &ab.SeekPosition{Type: &ab.SeekPosition_Newest{Newest: &ab.SeekNewest{}}}
	maxStop        = &ab.SeekPosition{Type: &ab.SeekPosition_Specified{Specified: &ab.SeekSpecified{Number: math.MaxUint64}}}
)

type deliverClient struct {
	client  ab.AtomicBroadcast_DeliverClient
	chainID string
}

func printBytes(bytes []byte) {
	fmt.Print("[")
	for _, b := range bytes {
		fmt.Printf("%d, ", int8(b))
	}
	fmt.Println("]")
}

func newDeliverClient(client ab.AtomicBroadcast_DeliverClient, chainID string) *deliverClient {
	return &deliverClient{client: client, chainID: chainID}
}

func seekHelper(chainID string, start *ab.SeekPosition, stop *ab.SeekPosition) *cb.Envelope {

	payloadSignatureHeader, err := signer.NewSignatureHeader() //JCS: sig header
	if err != nil {
		return nil
	}

	payload := &cb.Payload{ //JCS: create the payload
		Header: &cb.Header{
			ChannelHeader: utils.MarshalOrPanic(&cb.ChannelHeader{
				ChannelId: chainID,
			}),
			SignatureHeader: utils.MarshalOrPanic(payloadSignatureHeader),
		},

		Data: utils.MarshalOrPanic(&ab.SeekInfo{
			Start:    start,
			Stop:     stop,
			Behavior: ab.SeekInfo_BLOCK_UNTIL_READY,
		}),
	}
	paylBytes := utils.MarshalOrPanic(payload)

	//JCS: sign the payload
	sig, err := signer.Sign(paylBytes)
	if err != nil {
		return nil
	}

	return &cb.Envelope{ //JCS: return the envelope
		Payload:   paylBytes,
		Signature: sig,
	}

	//JCS: original code, with unsigned envelopes
	/*return &cb.Envelope{
		Payload: utils.MarshalOrPanic(&cb.Payload{
			Header: &cb.Header{
				ChannelHeader: utils.MarshalOrPanic(&cb.ChannelHeader{
					ChannelId: chainID,
				}),
				SignatureHeader: utils.MarshalOrPanic(&cb.SignatureHeader{}),
			},

			Data: utils.MarshalOrPanic(&ab.SeekInfo{
				Start:    start,
				Stop:     stop,
				Behavior: ab.SeekInfo_BLOCK_UNTIL_READY,
			}),
		}),
	}*/
}

func (r *deliverClient) seekOldest() error {
	return r.client.Send(seekHelper(r.chainID, oldest, maxStop))
}

func (r *deliverClient) seekNewest() error {
	return r.client.Send(seekHelper(r.chainID, newest, maxStop))
}

func (r *deliverClient) seekSingle(blockNumber uint64) error {
	specific := &ab.SeekPosition{Type: &ab.SeekPosition_Specified{Specified: &ab.SeekSpecified{Number: blockNumber}}}
	return r.client.Send(seekHelper(r.chainID, specific, specific))
}

func validateSignatures(meta *cb.Metadata, block *cb.Block) {

	if block.Header.Number == 0 {
		fmt.Printf("Block #0 requires no signature validation!\n")
		return
	}

	des := mspmgmt.GetIdentityDeserializer("")
	validSigs := uint32(0)

	for i, sig := range meta.Signatures {

		bytes := util.ConcatenateBytes(meta.Value, sig.SignatureHeader, block.Header.Bytes())

		sigHeader, err := utils.UnmarshalSignatureHeader(sig.SignatureHeader)
		if err != nil {
			fmt.Println("Signature header Problem: ", err)
			continue
		}
		ident, err := des.DeserializeIdentity(sigHeader.Creator)
		if err != nil {
			fmt.Println("Identity Problem: ", err)
			continue
		}

		err = mspmgmt.GetLocalMSP().Validate(ident)
		if err != nil {
			fmt.Println("Identity Problem: ", err)
			continue
		}

		fmt.Printf("Signature #%d\n: ", i)
		fmt.Println("MSPID: ", ident.GetMSPIdentifier())
		printBytes(sig.Signature)
		fmt.Println("")

		err = ident.Verify(bytes, sig.Signature)
		if err != nil {
			fmt.Println("Sig verification problem: ", err)
			continue
		}

		validSigs++

		if validSigs > F {
			fmt.Printf("Block #%d contains enough valid signatures!\n", block.Header.Number)
			return
		}

	}

	switch {
	case float32(validSigs) > Q:
		{
			fmt.Printf("Block #%d contains a quorum of valid signatures!\n", block.Header.Number)
		}
	case validSigs > F:
		{
			fmt.Printf("Block #%d contains enough valid signatures...\n", block.Header.Number)
		}
	default:
		{
			panic(fmt.Errorf("Block #%d does NOT contain enough valid signatures!\n", block.Header.Number))
		}
	}
}

func (r *deliverClient) readUntilClose() {

	for {
		msg, err := r.client.Recv()
		if err != nil {
			fmt.Println("Error receiving:", err)
			return
		}

		switch t := msg.Type.(type) {
		case *ab.DeliverResponse_Status:
			fmt.Println("Got status ", t)
			return
		case *ab.DeliverResponse_Block:

			if t.Block.GetHeader().Number != blocksReceived {
				panic(fmt.Errorf("Expected block #%d, received #%d", blocksReceived, t.Block.GetHeader().Number))

			}

			blocksReceived++
			fmt.Printf("\n\n\nReceived block #%d: \n", t.Block.GetHeader().Number) //JCS changed to print only the number of the header
			fmt.Printf("BlockHeader bytes #%d: ", t.Block.Header.Number)           // JCS: see what the bytes are and compare to proxy
			printBytes(t.Block.Header.Bytes())
			fmt.Printf("BlockData hash #%d: ", t.Block.Header.Number) // JCS: see what the bytes are and compare to proxy
			printBytes(t.Block.Data.Hash())

			if t.Block.Header.Number > 0 {

				meta, _ := utils.UnmarshalMetadata(t.Block.Metadata.Metadata[cb.BlockMetadataIndex_SIGNATURES])

				fmt.Printf("Block #%d contains %d block signatures\n", t.Block.Header.Number, len(meta.Signatures)) // JCS: see what the bytes are and compare to proxy

				validateSignatures(meta, t.Block)

				meta, _ = utils.UnmarshalMetadata(t.Block.Metadata.Metadata[cb.BlockMetadataIndex_LAST_CONFIG])

				fmt.Printf("Block #%d contains %d lastconfig signatures\n", t.Block.Header.Number, len(meta.Signatures)) // JCS: see what the bytes are and compare to proxy

				validateSignatures(meta, t.Block)

				fmt.Printf("Blocks received: %d\n", blocksReceived)
			}
		}
	}
}

func main() {
	config := config.Load()

	var chainID string
	var serverAddr string
	var seek int

	//JCS: Load local MSP
	err := mspmgmt.LoadLocalMsp(config.General.LocalMSPDir, config.General.BCCSP, config.General.LocalMSPID)
	if err != nil { // Handle errors reading the config file
		panic(err)
	}

	//JCS: create signer
	signer = localmsp.NewSigner()

	flag.StringVar(&serverAddr, "server", fmt.Sprintf("%s:%d", config.General.ListenAddress, config.General.ListenPort), "The RPC server to connect to.")
	flag.StringVar(&chainID, "chainID", provisional.TestChainID, "The chain ID to deliver from.")
	flag.IntVar(&seek, "seek", -2, "Specify the range of requested blocks."+
		"Acceptable values:"+
		"-2 (or -1) to start from oldest (or newest) and keep at it indefinitely."+
		"N >= 0 to fetch block N only.")
	flag.Parse()

	if seek < -2 {
		fmt.Println("Wrong seek value.")
		flag.PrintDefaults()
	}

	conn, err := grpc.Dial(serverAddr, grpc.WithInsecure())
	if err != nil {
		fmt.Println("Error connecting:", err)
		return
	}
	client, err := ab.NewAtomicBroadcastClient(conn).Deliver(context.TODO())
	if err != nil {
		fmt.Println("Error connecting:", err)
		return
	}

	s := newDeliverClient(client, chainID)
	switch seek {
	case -2:
		err = s.seekOldest()
	case -1:
		err = s.seekNewest()
	default:
		err = s.seekSingle(uint64(seek))
	}

	if err != nil {
		fmt.Println("Received error:", err)
	}

	s.readUntilClose()
}

