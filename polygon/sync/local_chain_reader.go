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

// local reads must not use a cancelled waypoint-download context.
func (r *dbLocalChainReader) localReadCtx(context.Context) context.Context {
	return context.Background()
}

func (r *dbLocalChainReader) readBlock(ctx context.Context, blockNum uint64) (*types.Block, error) {
	tx, err := r.db.BeginRo(r.localReadCtx(ctx))
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	readCtx := r.localReadCtx(ctx)

	n, h, _, err := rpchelper.GetBlockNumber(
		readCtx,
		rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(blockNum)),
		tx,
		r.br,
		nil,
	)
	if err == nil {
		block, _, berr := r.br.BlockWithSenders(readCtx, tx, h, n)
		if berr != nil {
			return nil, berr
		}
		if block != nil {
			return block, nil
		}
	}

	header, herr := r.br.Header(readCtx, tx, common.Hash{}, blockNum)
	if herr == nil && header != nil {
		block, _, berr := r.br.BlockWithSenders(readCtx, tx, header.Hash(), blockNum)
		if berr != nil {
			return nil, berr
		}
		if block != nil {
			return block, nil
		}
	}

	header, herr = r.br.HeaderByNumber(readCtx, tx, blockNum)
	if herr == nil && header != nil {
		block, _, berr := r.br.BlockWithSenders(readCtx, tx, header.Hash(), blockNum)
		if berr != nil {
			return nil, berr
		}
		if block != nil {
			return block, nil
		}
	}

	return nil, nil
}

func (r *dbLocalChainReader) GetHeader(ctx context.Context, blockNum uint64) (*types.Header, error) {
	block, err := r.readBlock(ctx, blockNum)
	if err != nil || block == nil {
		tx, err := r.db.BeginRo(r.localReadCtx(ctx))
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()

		readCtx := r.localReadCtx(ctx)
		n, h, _, err := rpchelper.GetBlockNumber(
			readCtx,
			rpc.BlockNumberOrHashWithNumber(rpc.BlockNumber(blockNum)),
			tx,
			r.br,
			nil,
		)
		if err != nil {
			return nil, err
		}

		return r.br.Header(readCtx, tx, h, n)
	}

	return block.Header(), nil
}

func (r *dbLocalChainReader) GetBodyByNumber(ctx context.Context, blockNum uint64) (*types.Body, error) {
	block, err := r.readBlock(ctx, blockNum)
	if err != nil || block == nil {
		return nil, err
	}

	return block.Body(), nil
}

func (r *dbLocalChainReader) GetBody(ctx context.Context, blockNum uint64, blockHash common.Hash) (*types.Body, error) {
	tx, err := r.db.BeginRo(r.localReadCtx(ctx))
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	readCtx := r.localReadCtx(ctx)
	block, _, err := r.br.BlockWithSenders(readCtx, tx, blockHash, blockNum)
	if err != nil || block == nil {
		return r.GetBodyByNumber(ctx, blockNum)
	}

	return block.Body(), nil
}
