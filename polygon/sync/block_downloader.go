// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package sync

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"sync"
	"sync/atomic"
	"time"

	"github.com/c2h5oh/datasize"
	lru "github.com/hashicorp/golang-lru/v2"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/estimate"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/polygon/heimdall"
)

const (
	notEnoughPeersBackOffDuration = time.Minute
	minWaypointFetchTimeout       = 60 * time.Second
	maxWaypointFetchTimeout       = 5 * time.Minute
	perHeaderFetchTimeout         = 500 * time.Millisecond
	waypointBodyFetchMaxRetries   = 5
	waypointBodyFetchBatchSize    = 32

	// conservative over-estimation: 1 MB block size x 1024 blocks per waypoint
	blockDownloaderEstimatedRamPerWorker = estimate.EstimatedRamPerWorker(1 * datasize.GB)
)

type localChainReader interface {
	GetHeader(ctx context.Context, blockNum uint64) (*types.Header, error)
	GetBody(ctx context.Context, blockNum uint64, blockHash common.Hash) (*types.Body, error)
}

func NewBlockDownloader(
	logger log.Logger,
	p2pService p2pService,
	waypointReader waypointReader,
	checkpointVerifier WaypointHeadersVerifier,
	milestoneVerifier WaypointHeadersVerifier,
	blocksVerifier BlocksVerifier,
	store Store,
	blockLimit uint,
	opts ...BlockDownloaderOption,
) *BlockDownloader {
	bd := &BlockDownloader{
		logger:             logger,
		p2pService:         p2pService,
		waypointReader:     waypointReader,
		checkpointVerifier: checkpointVerifier,
		milestoneVerifier:  milestoneVerifier,
		blocksVerifier:     blocksVerifier,
		store:              store,
		retryBackOff:       notEnoughPeersBackOffDuration,
		maxWorkers:         blockDownloaderEstimatedRamPerWorker.WorkersByRAMOnly(),
		blockLimit:         blockLimit,
	}

	for _, opt := range opts {
		opt(bd)
	}

	return bd
}

type BlockDownloader struct {
	logger             log.Logger
	p2pService         p2pService
	waypointReader     waypointReader
	checkpointVerifier WaypointHeadersVerifier
	milestoneVerifier  WaypointHeadersVerifier
	blocksVerifier     BlocksVerifier
	store              Store
	localChainReader   localChainReader
	retryBackOff       time.Duration
	maxWorkers         int
	blockLimit         uint
}

func (d *BlockDownloader) DownloadBlocksUsingCheckpoints(ctx context.Context, start uint64, end *uint64) (*types.Header, error) {
	checkpoints, err := d.waypointReader.CheckpointsFromBlock(ctx, start)
	if err != nil {
		return nil, err
	}

	if len(checkpoints) == 0 {
		return nil, nil
	}

	firstCheckpoint := checkpoints[0]
	if firstCheckpointStart := firstCheckpoint.StartBlock().Uint64(); firstCheckpointStart > start {
		return nil, fmt.Errorf(
			"unexpected first checkpoint with id %d has start %d which is greater than download start %d",
			firstCheckpoint.Id,
			firstCheckpointStart,
			start,
		)
	}

	// validate that there are no gaps
	for i := 1; i < len(checkpoints); i++ {
		prev, curr := checkpoints[i-1], checkpoints[i]
		if curr.RawId() != prev.RawId()+1 {
			return nil, fmt.Errorf("unexpected checkpoint gap between %d and %d", prev.RawId(), curr.RawId())
		}
	}

	return d.downloadBlocksUsingWaypoints(ctx, start, heimdall.AsWaypoints(checkpoints), d.checkpointVerifier, end)
}

func (d *BlockDownloader) DownloadBlocksUsingMilestones(ctx context.Context, start uint64, end *uint64) (*types.Header, error) {
	milestones, err := d.waypointReader.MilestonesFromBlock(ctx, start)
	if err != nil {
		return nil, err
	}

	if len(milestones) == 0 {
		return nil, nil
	}

	if firstMilestoneStart := milestones[0].StartBlock().Uint64(); start < firstMilestoneStart {
		gap := firstMilestoneStart - start
		if gap > maxMilestoneStartOverrideGap {
			d.logger.Warn(
				syncLogPrefix("gap before first milestone is too large to override start, use checkpoint sync"),
				"start", start,
				"firstMilestoneStart", firstMilestoneStart,
				"gap", gap,
				"maxGap", maxMilestoneStartOverrideGap,
			)
			return nil, nil
		}

		// Note this can happen (rarely, but it has happened) on initial sync if there is
		// a gap between the last downloaded checkpoint EndBlock and the StartBlock of the oldest
		// milestone that we have scrapped. We fill the gap by overriding the StartBlock of the milestone.
		// We are safe to do so because the RootHash of the milestone is in fact the last block of the milestone
		// range meaning that we can fetch an extended block range without failing the root hash check.
		d.logger.Warn(
			syncLogPrefix("gap between start and first milestone, overriding milestone start"),
			"start", start,
			"firstMilestoneStart", firstMilestoneStart,
			"gap", gap,
		)

		milestones[0].Fields.StartBlock = new(big.Int).SetUint64(start)
	}

	// we may have gaps in milestones due to their nature, luckily we have a way to handle that
	// as mentioned earlier we can override the start without breaking the RootHash validity due
	// to how it is calculated for milestones
	for i := 1; i < len(milestones); i++ {
		prev, curr := milestones[i-1], milestones[i]
		if prev.EndBlock().Uint64()+1 != curr.StartBlock().Uint64() {
			d.logger.Warn(
				syncLogPrefix("gap between milestones, overriding milestone start"),
				"currId", curr.Id,
				"prevId", prev.Id,
				"prevEndBlock", prev.EndBlock(),
				"currStartBlock", curr.StartBlock(),
			)

			curr.Fields.StartBlock = new(big.Int).SetUint64(prev.EndBlock().Uint64() + 1)
		}
	}

	return d.downloadBlocksUsingWaypoints(ctx, start, heimdall.AsWaypoints(milestones), d.milestoneVerifier, end)
}

func (d *BlockDownloader) downloadBlocksUsingWaypoints(
	ctx context.Context,
	start uint64,
	waypoints heimdall.Waypoints,
	verifier WaypointHeadersVerifier,
	end *uint64,
) (*types.Header, error) {
	if len(waypoints) == 0 {
		return nil, nil
	}

	waypoints = d.limitWaypoints(waypoints)
	waypoints = limitWaypointsEndBlock(waypoints, end)
	for len(waypoints) > 0 && waypoints[0].EndBlock().Uint64() < start {
		waypoints = waypoints[1:]
	}

	if len(waypoints) == 0 {
		return nil, nil
	}

	initialInfoLogArgs := []interface{}{
		"start", start,
		"waypointsLen", len(waypoints),
		"waypointsStart", waypoints[0].StartBlock().Uint64(),
		"waypointsEnd", waypoints[len(waypoints)-1].EndBlock().Uint64(),
		"kind", reflect.TypeOf(waypoints[0]),
		"blockLimit", d.blockLimit,
	}
	if end != nil {
		initialInfoLogArgs = append(initialInfoLogArgs, "end", *end)
	}
	d.logger.Info(syncLogPrefix("downloading blocks using waypoints"), initialInfoLogArgs...)

	// waypoint rootHash->[blocks part of waypoint]
	waypointBlocksMemo, err := lru.New[common.Hash, []*types.Block](d.p2pService.MaxPeers())
	if err != nil {
		return nil, err
	}

	progressLogTicker := time.NewTicker(30 * time.Second)
	defer progressLogTicker.Stop()

	var lastBlock *types.Block
	batchFetchStartTime := time.Now()
	fetchStartTime := time.Now()
	var blockCount, blocksTotalSize atomic.Uint64

	for len(waypoints) > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			// carry-on
		}

		endBlockNum := waypoints[len(waypoints)-1].EndBlock().Uint64()
		peerBlockNum := endBlockNum
		if start > waypoints[0].StartBlock().Uint64() {
			// resuming mid-waypoint: peers only need the tail we still fetch from the network
			peerBlockNum = start
		}
		peers := d.p2pService.ListPeersMayHaveBlockNum(peerBlockNum)
		if len(peers) == 0 {
			peers = d.p2pService.ListPeers()
			if len(peers) > 0 {
				d.logger.Warn(
					syncLogPrefix("no peers passed block height filter, falling back to all connected peers"),
					"peerBlockNum", peerBlockNum,
					"peerCount", len(peers),
				)
			}
		}
		if len(peers) == 0 {
			d.logger.Warn(
				syncLogPrefix("can't use any peers to download blocks, will try again in a bit"),
				"start", waypoints[0].StartBlock(),
				"end", endBlockNum,
				"retryBackOffSecs", d.retryBackOff.Seconds(),
			)

			if err := common.Sleep(ctx, d.retryBackOff); err != nil {
				return nil, err
			}

			continue
		}

		numWorkers := min(d.maxWorkers, len(peers), len(waypoints))
		waypointsBatch := waypoints[:numWorkers]

		select {
		case <-progressLogTicker.C:
			d.logger.Info(
				syncLogPrefix("downloading blocks progress"),
				"waypointsBatchLength", len(waypointsBatch),
				"startBlockNum", waypointsBatch[0].StartBlock(),
				"endBlockNum", waypointsBatch[len(waypointsBatch)-1].EndBlock(),
				"kind", reflect.TypeOf(waypointsBatch[0]),
				"peerCount", len(peers),
				"maxWorkers", d.maxWorkers,
				"blk/s", fmt.Sprintf("%.2f", float64(blockCount.Load())/time.Since(fetchStartTime).Seconds()),
				"bytes/s", common.ByteCount(uint64(float64(blocksTotalSize.Load())/time.Since(fetchStartTime).Seconds())),
			)

			blockCount.Store(0)
			blocksTotalSize.Store(0)
			fetchStartTime = time.Now()

		default:
			// carry on
		}

		blockBatches := make([][]*types.Block, len(waypointsBatch))
		maxWaypointLength := uint64(0)
		wg := sync.WaitGroup{}
		for i, waypoint := range waypointsBatch {
			maxWaypointLength = max(waypoint.Length(), maxWaypointLength)
			wg.Add(1)
			go func(i int, waypoint heimdall.Waypoint, peerId *p2p.PeerId) {
				defer wg.Done()

				if blocks, ok := waypointBlocksMemo.Get(waypoint.RootHash()); ok {
					blockBatches[i] = blocks
					return
				}

				blocks, totalSize, err := d.fetchVerifiedBlocks(ctx, waypoint, peerId, verifier, start)
				if err != nil {
					d.logger.Warn(
						syncLogPrefix("issue downloading waypoint blocks - will try again"),
						"err", err,
						"start", waypoint.StartBlock(),
						"end", waypoint.EndBlock(),
						"rootHash", waypoint.RootHash(),
						"kind", reflect.TypeOf(waypoint),
						"peerId", peerId,
					)

					return
				}

				blocksTotalSize.Add(uint64(totalSize))
				blockCount.Add(uint64(len(blocks)))

				waypointBlocksMemo.Add(waypoint.RootHash(), blocks)
				blockBatches[i] = blocks
			}(i, waypoint, peers[i])
		}

		wg.Wait()
		blocks := make([]*types.Block, 0, int(maxWaypointLength)*len(waypointsBatch))
		gapIndex := -1
		for i, blockBatch := range blockBatches {
			if len(blockBatch) == 0 {
				d.logger.Info(
					syncLogPrefix("no blocks - will try again"),
					"start", waypointsBatch[i].StartBlock(),
					"end", waypointsBatch[i].EndBlock(),
					"rootHash", waypointsBatch[i].RootHash(),
					"kind", reflect.TypeOf(waypointsBatch[i]),
				)

				gapIndex = i
				break
			}

			batchStart := blockBatch[0].Number().Uint64()
			batchEnd := blockBatch[len(blockBatch)-1].Number().Uint64()
			if batchStart <= start && start <= batchEnd {
				// do not re-insert blocks already on the local chain when resuming mid-waypoint
				blockBatch = blockBatch[start-batchStart:]
			} else if batchEnd < start {
				blockBatch = nil
			}

			if len(blockBatch) > 0 && blockBatch[0].Number().Uint64() == 0 {
				// we do not want to insert block 0 (genesis)
				blockBatch = blockBatch[1:]
			}

			if len(blockBatch) > 0 {
				blocks = append(blocks, blockBatch...)
			}
		}

		if gapIndex >= 0 {
			waypoints = waypoints[gapIndex:]
		} else {
			waypoints = waypoints[len(waypointsBatch):]
		}

		if len(blocks) == 0 {
			continue
		}

		d.logger.Debug(
			syncLogPrefix("fetched blocks"),
			"start", blocks[0].NumberU64(),
			"end", blocks[len(blocks)-1].NumberU64(),
			"blocks", len(blocks),
			"duration", time.Since(batchFetchStartTime),
			"blks/sec", float64(len(blocks))/math.Max(time.Since(batchFetchStartTime).Seconds(), 0.0001),
		)

		if end != nil {
			for i := range blocks {
				if blocks[i].Number().Uint64() > *end {
					blocks = blocks[:i]
					break
				}
			}
		}

		batchFetchStartTime = time.Now() // reset for next time

		d.logger.Info(
			syncLogPrefix("inserting fetched blocks"),
			"start", blocks[0].NumberU64(),
			"end", blocks[len(blocks)-1].NumberU64(),
			"blocks", len(blocks),
		)
		if err := d.store.InsertBlocks(ctx, blocks); err != nil {
			return nil, err
		}

		lastBlock = blocks[len(blocks)-1]
	}

	d.logger.Debug(syncLogPrefix("finished downloading blocks using waypoints"))
	return lastBlock.Header(), nil
}

func (d *BlockDownloader) fetchVerifiedBlocks(
	ctx context.Context,
	waypoint heimdall.Waypoint,
	peerId *p2p.PeerId,
	verifier WaypointHeadersVerifier,
	downloadStart uint64,
) ([]*types.Block, int, error) {
	waypointStart := waypoint.StartBlock().Uint64()
	waypointEnd := waypoint.EndBlock().Uint64()
	end := waypointEnd + 1 // waypoint end is inclusive, fetch headers is [start, end)

	if downloadStart > waypointStart && d.localChainReader != nil {
		if blocks, totalSize, ok := d.tryFetchVerifiedCheckpointTailFromLocal(
			ctx, waypoint, downloadStart, verifier,
		); ok {
			return blocks, totalSize, nil
		}
	}

	prefixHeaders, fetchStart := d.loadLocalCheckpointPrefix(ctx, waypointStart, downloadStart)

	tailHeaderCount := int(end - fetchStart)
	fetchOpts := waypointFetchOpts(tailHeaderCount)

	// 1. Fetch headers in waypoint from a peer
	headers, err := d.p2pService.FetchHeaders(ctx, fetchStart, end, peerId, fetchOpts...)
	if err != nil {
		return nil, 0, err
	}

	allHeaders := append(prefixHeaders, headers.Data...)

	// 2. Verify headers match waypoint root hash
	if err = verifier(waypoint, allHeaders); err != nil {
		d.logger.Warn(syncLogPrefix("penalizing peer - invalid headers"), "peerId", peerId, "err", err)

		if penalizeErr := d.p2pService.Penalize(ctx, peerId); penalizeErr != nil {
			err = fmt.Errorf("%w: %w", penalizeErr, err)
		}

		return nil, 0, err
	}

	// 3. Fetch bodies for the tail; prefer bodies already present in the local chain.
	tailHeaders := headers.Data
	bodies, bodiesSize, err := d.fetchTailBodiesPreferLocal(ctx, tailHeaders, peerId, fetchOpts)
	if err != nil {
		if errors.Is(err, &p2p.ErrMissingBodies{}) {
			d.logger.Warn(syncLogPrefix("penalizing peer - missing bodies"), "peerId", peerId, "err", err)

			if penalizeErr := d.p2pService.Penalize(ctx, peerId); penalizeErr != nil {
				err = fmt.Errorf("%w: %w", penalizeErr, err)
			}
		}

		return nil, 0, err
	}

	// 4. Assemble blocks (tail only; prefix blocks are already on the local chain)
	blocks := make([]*types.Block, len(tailHeaders))
	for i, header := range tailHeaders {
		blocks[i] = types.NewBlockFromNetwork(header, bodies[i])
	}

	// 5. Verify blocks
	if err = d.blocksVerifier(blocks); err != nil {
		d.logger.Warn(syncLogPrefix("penalizing peer - invalid blocks"), "peerId", peerId, "err", err)

		if penalizeErr := d.p2pService.Penalize(ctx, peerId); penalizeErr != nil {
			err = fmt.Errorf("%w: %w", penalizeErr, err)
		}

		return nil, 0, err
	}

	return blocks, headers.TotalSize + bodiesSize, nil
}

func waypointFetchOpts(headerCount int) []p2p.FetcherOption {
	timeout := minWaypointFetchTimeout + time.Duration(headerCount)*perHeaderFetchTimeout
	if timeout > maxWaypointFetchTimeout {
		timeout = maxWaypointFetchTimeout
	}

	return []p2p.FetcherOption{
		p2p.WithResponseTimeout(timeout),
		p2p.WithMaxRetries(waypointBodyFetchMaxRetries),
	}
}

func (d *BlockDownloader) fetchTailBodiesPreferLocal(
	ctx context.Context,
	tailHeaders []*types.Header,
	peerId *p2p.PeerId,
	fetchOpts []p2p.FetcherOption,
) ([]*types.Body, int, error) {
	bodies := make([]*types.Body, len(tailHeaders))
	peerHeaders := make([]*types.Header, 0, len(tailHeaders))
	peerIndices := make([]int, 0, len(tailHeaders))
	localBodies := 0

	for i, header := range tailHeaders {
		if d.localChainReader != nil {
			body, err := d.localChainReader.GetBody(ctx, header.Number.Uint64(), header.Hash())
			if err == nil && body != nil {
				bodies[i] = body
				localBodies++
				continue
			}
		}

		peerHeaders = append(peerHeaders, header)
		peerIndices = append(peerIndices, i)
	}

	if len(peerHeaders) == 0 {
		d.logger.Info(
			syncLogPrefix("checkpoint tail bodies loaded from local chain"),
			"blocks", localBodies,
		)
		return bodies, 0, nil
	}

	d.logger.Info(
		syncLogPrefix("fetching waypoint tail bodies from peer"),
		"localBodies", localBodies,
		"peerBodies", len(peerHeaders),
		"batchSize", waypointBodyFetchBatchSize,
	)

	totalSize, err := d.fetchPeerBodiesInBatches(ctx, peerHeaders, peerIndices, bodies, peerId, fetchOpts)
	if err != nil {
		return nil, 0, err
	}

	return bodies, totalSize, nil
}

func (d *BlockDownloader) fetchPeerBodiesInBatches(
	ctx context.Context,
	peerHeaders []*types.Header,
	peerIndices []int,
	bodies []*types.Body,
	primaryPeer *p2p.PeerId,
	_ []p2p.FetcherOption,
) (int, error) {
	peerCandidates := make([]*p2p.PeerId, 0, 1+len(d.p2pService.ListPeers()))
	if primaryPeer != nil {
		peerCandidates = append(peerCandidates, primaryPeer)
	}
	for _, peer := range d.p2pService.ListPeers() {
		if peer == nil {
			continue
		}
		if primaryPeer != nil && *peer == *primaryPeer {
			continue
		}
		peerCandidates = append(peerCandidates, peer)
	}

	var totalSize int
	for offset := 0; offset < len(peerHeaders); {
		batchEnd := offset + waypointBodyFetchBatchSize
		if batchEnd > len(peerHeaders) {
			batchEnd = len(peerHeaders)
		}

		batchHeaders := peerHeaders[offset:batchEnd]
		batchIndices := peerIndices[offset:batchEnd]
		batchOpts := waypointFetchOpts(len(batchHeaders))

		var lastErr error
		fetched := false
		for _, peerId := range peerCandidates {
			peerResp, err := d.p2pService.FetchBodies(ctx, batchHeaders, peerId, batchOpts...)
			if err != nil {
				lastErr = err
				continue
			}
			if len(peerResp.Data) != len(batchHeaders) {
				lastErr = fmt.Errorf("peer returned %d bodies, expected %d", len(peerResp.Data), len(batchHeaders))
				continue
			}

			for j, idx := range batchIndices {
				bodies[idx] = peerResp.Data[j]
			}

			totalSize += peerResp.TotalSize
			offset = batchEnd
			fetched = true
			break
		}

		if !fetched {
			if lastErr == nil {
				lastErr = p2p.ErrPeerNotFound
			}
			return 0, lastErr
		}
	}

	return totalSize, nil
}

func (d *BlockDownloader) tryFetchVerifiedCheckpointTailFromLocal(
	ctx context.Context,
	waypoint heimdall.Waypoint,
	downloadStart uint64,
	verifier WaypointHeadersVerifier,
) ([]*types.Block, int, bool) {
	waypointStart := waypoint.StartBlock().Uint64()
	waypointEnd := waypoint.EndBlock().Uint64()

	allHeaders, ok := d.loadLocalCheckpointHeaders(ctx, waypointStart, waypointEnd)
	if !ok {
		d.logger.Debug(
			syncLogPrefix("checkpoint not fully available locally"),
			"waypointStart", waypointStart,
			"waypointEnd", waypointEnd,
			"reason", "missing headers",
		)
		return nil, 0, false
	}

	if err := verifier(waypoint, allHeaders); err != nil {
		d.logger.Debug(
			syncLogPrefix("checkpoint not fully available locally"),
			"waypointStart", waypointStart,
			"waypointEnd", waypointEnd,
			"reason", "header verification failed",
			"err", err,
		)
		return nil, 0, false
	}

	blocks := make([]*types.Block, 0, waypointEnd-downloadStart+1)
	for _, header := range allHeaders {
		blockNum := header.Number.Uint64()
		if blockNum < downloadStart {
			continue
		}

		body, err := d.localChainReader.GetBody(ctx, blockNum, header.Hash())
		if err != nil || body == nil {
			d.logger.Debug(
				syncLogPrefix("checkpoint not fully available locally"),
				"waypointStart", waypointStart,
				"waypointEnd", waypointEnd,
				"downloadStart", downloadStart,
				"reason", "missing body",
				"blockNum", blockNum,
				"err", err,
			)
			return nil, 0, false
		}

		blocks = append(blocks, types.NewBlockFromNetwork(header, body))
	}

	if err := d.blocksVerifier(blocks); err != nil {
		return nil, 0, false
	}

	d.logger.Info(
		syncLogPrefix("checkpoint tail fetched from local chain"),
		"waypointStart", waypointStart,
		"waypointEnd", waypointEnd,
		"downloadStart", downloadStart,
		"blocks", len(blocks),
	)

	return blocks, 0, true
}

func (d *BlockDownloader) loadLocalCheckpointHeaders(
	ctx context.Context,
	waypointStart uint64,
	waypointEnd uint64,
) ([]*types.Header, bool) {
	headers := make([]*types.Header, 0, waypointEnd-waypointStart+1)
	for blockNum := waypointStart; blockNum <= waypointEnd; blockNum++ {
		header, err := d.localChainReader.GetHeader(ctx, blockNum)
		if err != nil || header == nil {
			return nil, false
		}

		headers = append(headers, header)
	}

	return headers, true
}

func (d *BlockDownloader) loadLocalCheckpointPrefix(
	ctx context.Context,
	waypointStart uint64,
	downloadStart uint64,
) ([]*types.Header, uint64) {
	if downloadStart <= waypointStart || d.localChainReader == nil {
		return nil, waypointStart
	}

	prefixLen := downloadStart - waypointStart
	prefixHeaders := make([]*types.Header, 0, prefixLen)
	for blockNum := waypointStart; blockNum < downloadStart; blockNum++ {
		header, err := d.localChainReader.GetHeader(ctx, blockNum)
		if err != nil || header == nil {
			d.logger.Warn(
				syncLogPrefix("incomplete local checkpoint prefix, will fetch full range from peer"),
				"waypointStart", waypointStart,
				"downloadStart", downloadStart,
				"missingBlock", blockNum,
				"err", err,
			)

			return nil, waypointStart
		}

		prefixHeaders = append(prefixHeaders, header)
	}

	d.logger.Info(
		syncLogPrefix("using local headers for checkpoint prefix"),
		"waypointStart", waypointStart,
		"downloadStart", downloadStart,
		"prefixLen", prefixLen,
	)

	return prefixHeaders, downloadStart
}

func (d *BlockDownloader) limitWaypoints(waypoints []heimdall.Waypoint) []heimdall.Waypoint {
	if d.blockLimit == 0 {
		return waypoints
	}

	startBlockNum := waypoints[0].StartBlock().Uint64()
	for i, waypoint := range waypoints {
		// we allow a bit of surplus to overflow above the block limit at waypoint boundary
		if waypoint.EndBlock().Uint64()-startBlockNum < uint64(d.blockLimit) {
			continue
		}

		waypoints = waypoints[:i+1]
		break
	}

	return waypoints
}

func limitWaypointsEndBlock(waypoints []heimdall.Waypoint, end *uint64) []heimdall.Waypoint {
	if end == nil {
		return waypoints
	}

	for i, waypoint := range waypoints {
		if waypoint.StartBlock().Uint64() > *end {
			waypoints = waypoints[:i]
			break
		}
	}

	return waypoints
}
