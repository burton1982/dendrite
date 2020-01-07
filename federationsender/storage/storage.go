// Copyright 2020 The Matrix.org Foundation C.I.C.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"context"
	"database/sql"
	"errors"
	"net/url"

	"github.com/matrix-org/dendrite/common"
	"github.com/matrix-org/dendrite/federationsender/storage/postgres"
	"github.com/matrix-org/dendrite/federationsender/storage/sqlite3"
	"github.com/matrix-org/dendrite/federationsender/types"

	_ "github.com/mattn/go-sqlite3"
)

type Database interface {
	common.PartitionStorer
	UpdateRoom(ctx context.Context, roomID, oldEventID, newEventID string, addHosts []types.JoinedHost, removeHosts []string) (joinedHosts []types.JoinedHost, err error)
	GetJoinedHosts(ctx context.Context, roomID string) ([]types.JoinedHost, error)
}

type Storage struct {
	db *sql.DB
	common.PartitionOffsetStatements
	RoomTable
	JoinedHostsTable
}

// NewDatabase opens a new database
func NewDatabase(dataSourceName string) (*Storage, error) {
	result := &Storage{}
	uri, err := url.Parse(dataSourceName)
	if err != nil {
		return nil, err
	}
	switch uri.Scheme {
	case "postgres":
		if result.db, err = sql.Open("postgres", dataSourceName); err != nil {
			return nil, err
		}
		if err = result.prepareRoomTable(result.db, &postgres.PostgresRoomTable{}); err != nil {
			return nil, err
		}
		if err = result.prepareJoinedHostsTable(result.db, &postgres.PostgresJoinedHostsTable{}); err != nil {
			return nil, err
		}

	case "file":
		if uri.Opaque != "" { // file:filename.db
			if result.db, err = sql.Open("sqlite3", uri.Opaque); err != nil {
				return nil, err
			}
		}
		if uri.Path != "" { // file:///path/to/filename.db
			if result.db, err = sql.Open("sqlite3", uri.Path); err != nil {
				return nil, err
			}
		}
		if result.db != nil {
			if err = result.prepareRoomTable(result.db, &sqlite3.SqliteRoomTable{}); err != nil {
				return nil, err
			}
			if err = result.prepareJoinedHostsTable(result.db, &sqlite3.SqliteJoinedHostsTable{}); err != nil {
				return nil, err
			}
		}

	default:
		return nil, errors.New("unknown schema")
	}

	if err := result.PartitionOffsetStatements.Prepare(result.db, "federationsender"); err != nil {
		return nil, err
	}

	return result, nil
}
