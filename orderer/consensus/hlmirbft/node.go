/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package hlmirbft

import (
	"bytes"
	"crypto"
	"fmt"
	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric-protos-go/common"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/pkg/errors"

	"github.com/fly2plan/fabric-protos-go/orderer/hlmirbft"
	"github.com/fly2plan/mirbft"
	"github.com/fly2plan/mirbft/pkg/eventlog"
	"github.com/fly2plan/mirbft/pkg/pb/msgs"
	"github.com/fly2plan/mirbft/pkg/reqstore"
	"github.com/fly2plan/mirbft/pkg/simplewal"
	"github.com/hyperledger/fabric-protos-go/orderer"
	"github.com/hyperledger/fabric/protoutil"

	"code.cloudfoundry.org/clock"
	"github.com/hyperledger/fabric/common/flogging"
)

type node struct {
	chainID string
	logger  *flogging.FabricLogger
	metrics *Metrics

	unreachableLock sync.RWMutex
	unreachable     map[uint64]struct{}

	config             *mirbft.Config
	numberOfBuckets    int32
	checkpointInterval int32
	maxEpochLength     uint64
	nodeStatuses       []*hlmirbft.NodeStatus

	WALDir      string
	ReqStoreDir string

	checkpointSeqNo uint64 //JIRA FLY2-66
	networkState    *msgs.NetworkState
	epochConfig     *msgs.EpochConfig

	LastCommittedSeqNo uint64 //JIRA FLY2-48 - proposed changes: To track the last committed sequence number

	ReqStore *reqstore.Store //JIRA FLY2-48 - Stores the request store instance of mirbft node. This for getting Request object using request #.

	rpc *Disseminator

	chain *Chain

	clock clock.Clock

	metadata *hlmirbft.BlockMetadata

	mirbft.Node
}

const (
	snapSuffix = ".snap"
)

func (n *node) start(fresh, join bool) {
	n.logger.Debugf("Starting mirbft node: #peers: %v", len(n.metadata.ConsenterIds))

	if join {
		n.logger.Info("Starting mirbft node to join an existing channel")
	} else {
		n.logger.Info("Starting mirbft node as part of a new channel")
	}

	// Checking if the configuration settings have been passed correctly.
	err := os.MkdirAll(n.ReqStoreDir, 0700)
	if err != nil {
		n.logger.Error(err, "Failed to create WAL directory")
	}
	wal, err := simplewal.Open(n.WALDir)
	if err != nil {
		n.logger.Error(err, "Failed to create WAL")
	}
	err = os.MkdirAll(n.ReqStoreDir, 0700)
	if err != nil {
		n.logger.Error(err, "Failed to create request store directory")
	}
	reqStore, err := reqstore.Open(n.ReqStoreDir)
	//FL2-48 proposed changes
	// - store the mirbft node request store instance to node
	n.ReqStore = reqStore
	if err != nil {
		n.logger.Error(err, "Failed to create request store")
	}
	node, err := mirbft.NewNode(
		n.chain.MirBFTID,
		n.config,
		&mirbft.ProcessorConfig{
			Link:         n,
			Hasher:       crypto.SHA256,
			App:          n.chain,
			WAL:          wal,
			RequestStore: reqStore,
			Interceptor:  eventlog.NewRecorder(n.chain.MirBFTID, &bytes.Buffer{}),
		},
	)
	if err != nil {
		n.logger.Error(err, "Failed to create mirbft node")
	} else {
		n.Node = *node
	}

	if fresh {
		n.checkpointSeqNo = n.chain.opts.BlockMetadata.LastCheckpointSeqNo
		initialCheckpoint := []byte("first")
		atGenesis := n.chain.support.Height() == 1
		if atGenesis {
			n.networkState = InitialNetworkState(
				n.metadata.ConsenterIds,
				n.numberOfBuckets,
				n.checkpointInterval,
				n.maxEpochLength,
				n.nodeStatuses,
			)
			n.epochConfig = &msgs.EpochConfig{
				Number:  0,
				Leaders: n.networkState.Config.Nodes,
			}
		} else {
			networkState := &msgs.NetworkState{}
			err := proto.Unmarshal(n.chain.opts.BlockMetadata.NetworkState, networkState)
			if err != nil {
				n.logger.Error(err, "Failed to unmarshal network state")
			}
			n.networkState = networkState
			previousEpochConfig := &msgs.EpochConfig{}
			err = proto.Unmarshal(n.chain.opts.BlockMetadata.EpochConfig, previousEpochConfig)
			if err != nil {
				n.logger.Error(err, "Failed to unmarshal epoch configuration")
			}
			n.epochConfig = previousEpochConfig

			lastBlockBytes, err := proto.Marshal(&common.Block{
				Header: n.chain.lastBlock.Header,
				Data:   n.chain.lastBlock.Data,
			})
			if err != nil {
				n.logger.Error(err, "Could not marshal block")
			}

			initialCheckpoint, err = proto.Marshal(&hlmirbft.SnapData{
				LastCommitedBlock: lastBlockBytes,
				NetworkState:      n.chain.opts.BlockMetadata.NetworkState,
			})
		}
		n.logger.Infof("fresh: \nseqNo %+v\nnetworkState %+v\ninitialCheckpoint %x\nepochConfig %+v", n.checkpointSeqNo, n.networkState, sha256.Sum256(initialCheckpoint), n.epochConfig)
		// TODO(harrymknight) Tick interval is fixed. Perhaps introduce TickInterval field in configuration options
		go func() {
			err := n.ProcessAsNewNode(
				n.chain.doneC,
				n.clock.NewTicker(500*time.Millisecond).C(),
				n.checkpointSeqNo,
				n.networkState,
				initialCheckpoint,
				n.epochConfig,
			)
			if err != nil {
				n.logger.Error(err, "Failed to start mirbft node")
			}
		}()
	} else {
		n.logger.Info("Restarting mirbft node")
		go func() {
			err := n.RestartProcessing(n.chain.doneC, n.clock.NewTicker(500*time.Millisecond).C())
			if err != nil {
				n.logger.Error(err, "Failed to restart mirbft node")
			}
		}()
	}
}

func InitialNetworkState(nodes []uint64, numberOfBuckets int32, checkpointInterval int32, maxEpochLength uint64, nodeStatuses []*hlmirbft.NodeStatus) *msgs.NetworkState {
	loyalties := make([]int64, len(nodeStatuses))
	timeouts := make([]uint64, len(nodeStatuses))

	for i, nodeStatus := range nodeStatuses {
		loyalties[i] = nodeStatus.Loyalty
		timeouts[i] = nodeStatus.Timeout
	}

	// TODO(harrymknight) The width of a client window is fixed.
	//  Could optimise by varying according to the checkpoint interval and batch size
	clients := make([]*msgs.NetworkState_Client, len(nodes))
	for i := range clients {
		clients[i] = &msgs.NetworkState_Client{
			Id:           uint64(i + 1),
			Width:        10000,
			LowWatermark: 0,
		}
	}

	return &msgs.NetworkState{
		Config: &msgs.NetworkState_Config{
			Nodes:              nodes,
			F:                  int32((len(nodes) - 1) / 3),
			NumberOfBuckets:    numberOfBuckets,
			CheckpointInterval: checkpointInterval,
			MaxEpochLength:     maxEpochLength,
			Loyalties:          loyalties,
			Timeouts:           timeouts,
		},
		Clients: clients,
	}
}

func (n *node) Send(dest uint64, msg *msgs.Msg) {
	err := n.chain.WaitReady()
	if err != nil {
		n.logger.Panicf("Error: %x", err)
	}
	n.unreachableLock.RLock()
	defer n.unreachableLock.RUnlock()

	msgBytes := protoutil.MarshalOrPanic(msg)
	err := n.rpc.SendConsensus(dest, &orderer.ConsensusRequest{Channel: n.chainID, Payload: msgBytes})
	if err != nil {
		n.logSendFailure(dest, err)
	} else if _, ok := n.unreachable[dest]; ok {
		n.logger.Infof("Successfully sent StepRequest to %d after failed attempt(s)", dest)
		delete(n.unreachable, dest)
	}
}

func (n *node) logSendFailure(dest uint64, err error) {
	if _, ok := n.unreachable[dest]; ok {
		n.logger.Debugf("Failed to send StepRequest to %d, because: %s", dest, err)
		return
	}

	n.logger.Errorf("Failed to send StepRequest to %d, because: %s", dest, err)
	n.unreachable[dest] = struct{}{}
}

// JIRA FLY2-58 proposed changes:readSnapFiles loads the snap file based on the sequence number and reads the contents
func (n *node) ReadSnapFiles(seqNo uint64, SnapDir string) ([]byte, error) {
	var snapBytes []byte
	fileNamePattern := fmt.Sprintf("%016x-*", seqNo)

	snapFileList, err := filepath.Glob(filepath.Join(SnapDir, fileNamePattern))
	if err != nil {
		return nil, err
	}
	numberOfFiles := len(snapFileList)
	switch {
	case numberOfFiles == 0:
		err = errors.Errorf("file not found Error : No files found for sequence number %016x", seqNo)
		snapBytes = nil
	case numberOfFiles == 1:
		snapBytes, err = ioutil.ReadFile(filepath.Join(SnapDir, snapFileList[0]))
	case numberOfFiles > 1:
		n.logger.Warnf("File Duplication : multiple files detected for sequence number %016x", seqNo)
		snapBytes, err = ioutil.ReadFile(filepath.Join(SnapDir, snapFileList[numberOfFiles-1]))
	}
	return snapBytes, err
}

// JIRA FLY2- 66 Proposed changes:Implemented the PersistSnapshot functionality to persist the snaps to local files
func (n *node) PersistSnapshot(seqNo uint64, Data []byte) error {
	if err := os.MkdirAll(n.chain.opts.SnapDir, os.ModePerm); err != nil {
		return errors.Errorf("failed to mkdir '%s' for snapshot: %s", n.chain.opts.SnapDir, err)
	}

	TimeStamp := time.Now().Unix()

	fname := fmt.Sprintf("%016x-%016x%s", seqNo, TimeStamp, snapSuffix)

	spath := filepath.Join(n.chain.opts.SnapDir, fname)

	f, err := os.OpenFile(spath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0666)
	if err != nil {
		return err
	}

	byteNumber, err := f.Write(Data)
	if err == nil && byteNumber < len(Data) {
		err = io.ErrShortWrite
		return err
	}
	if err1 := f.Close(); err == nil {
		err = err1
	}
	return err
}

// JIRA FLY2-66 proposed changes: PurgeSnapFiles takes the list of snap files in the snap directory  and removes them
func (n *node) PurgeSnapFiles(SnapDir string) error {
	snapFileList, err := filepath.Glob(filepath.Join(SnapDir, snapSuffix))
	if err != nil {
		return errors.Errorf("Cannot retrive snap files : %s", err)
	}
	err = PurgeFiles(snapFileList[:len(snapFileList)-2], SnapDir, n.logger)
	if err != nil {
		return err
	}
	return nil
}
