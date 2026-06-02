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

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
	"github.com/erigontech/erigon/turbo/services"
)

// dbLocalChainReader loads headers and bodies the same way as eth_getBlockByNumber.
type dbLocalChainReader struct {
	db kv.TemporalRwDB
	br services.FullBlockReader
}

func newDBLocalChainReader(db kv.TemporalRwDB, br services.FullBlockReader) *dbLocalChainReader {
	return &dbLocalChainReader{db: db, br: br}
}

func (r *dbLocalChainReader) readBlock(ctx context.Context, blockNum uint64) (*types.Block, error) {
	tx, err := r.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	n, h, _, err := rpchelper.GetBlockNumber(
		ctx,
		rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(blockNum)),
		tx,
		r.br,
		nil,
	)
	if err != nil {
		return nil, err
	}

	block, _, err := r.br.BlockWithSenders(ctx, tx, h, n)
	if err != nil {
		return nil, err
	}

	return block, nil
}

func (r *dbLocalChainReader) GetHeader(ctx context.Context, blockNum uint64) (*types.Header, error) {
	tx, err := r.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	n, h, _, err := rpchelper.GetBlockNumber(
		ctx,
		rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(blockNum)),
		tx,
		r.br,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return r.br.Header(ctx, tx, h, n)
}

func (r *dbLocalChainReader) GetBodyByNumber(ctx context.Context, blockNum uint64) (*types.Body, error) {
	block, err := r.readBlock(ctx, blockNum)
	if err != nil || block == nil {
		return nil, err
	}

	return block.Body(), nil
}

func (r *dbLocalChainReader) GetBody(ctx context.Context, blockNum uint64, _ common.Hash) (*types.Body, error) {
	return r.GetBodyByNumber(ctx, blockNum)
}
