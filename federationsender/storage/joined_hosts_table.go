// Copyright 2017-2018 New Vector Ltd
// Copyright 2019-2020 The Matrix.org Foundation C.I.C.
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

	"github.com/lib/pq"
	"github.com/matrix-org/dendrite/common"
	"github.com/matrix-org/dendrite/federationsender/types"
	"github.com/matrix-org/gomatrixserverlib"
)

type JoinedHostsTable struct {
	insertJoinedHostsStmt *sql.Stmt
	deleteJoinedHostsStmt *sql.Stmt
	selectJoinedHostsStmt *sql.Stmt
}

type JoinedHostsSchema interface {
	Schema() string
	InsertJoinedHostsStmt(*sql.DB) (*sql.Stmt, error)
	DeleteJoinedHostsStmt(*sql.DB) (*sql.Stmt, error)
	SelectJoinedHostsStmt(*sql.DB) (*sql.Stmt, error)
}

func (s *JoinedHostsTable) prepareJoinedHostsTable(db *sql.DB, schema JoinedHostsSchema) (err error) {
	_, err = db.Exec(schema.Schema())
	if err != nil {
		return
	}
	if s.insertJoinedHostsStmt, err = schema.InsertJoinedHostsStmt(db); err != nil {
		return
	}
	if s.deleteJoinedHostsStmt, err = schema.DeleteJoinedHostsStmt(db); err != nil {
		return
	}
	if s.selectJoinedHostsStmt, err = schema.SelectJoinedHostsStmt(db); err != nil {
		return
	}
	return
}

func (s *JoinedHostsTable) insertJoinedHosts(
	ctx context.Context,
	txn *sql.Tx,
	roomID, eventID string,
	serverName gomatrixserverlib.ServerName,
) error {
	stmt := common.TxStmt(txn, s.insertJoinedHostsStmt)
	_, err := stmt.ExecContext(ctx, roomID, eventID, serverName)
	return err
}

func (s *JoinedHostsTable) deleteJoinedHosts(
	ctx context.Context, txn *sql.Tx, eventIDs []string,
) error {
	stmt := common.TxStmt(txn, s.deleteJoinedHostsStmt)
	_, err := stmt.ExecContext(ctx, pq.StringArray(eventIDs))
	return err
}

func (s *JoinedHostsTable) selectJoinedHostsWithTx(
	ctx context.Context, txn *sql.Tx, roomID string,
) ([]types.JoinedHost, error) {
	stmt := common.TxStmt(txn, s.selectJoinedHostsStmt)
	return joinedHostsFromStmt(ctx, stmt, roomID)
}

func (s *JoinedHostsTable) selectJoinedHosts(
	ctx context.Context, roomID string,
) ([]types.JoinedHost, error) {
	return joinedHostsFromStmt(ctx, s.selectJoinedHostsStmt, roomID)
}

func joinedHostsFromStmt(
	ctx context.Context, stmt *sql.Stmt, roomID string,
) ([]types.JoinedHost, error) {
	rows, err := stmt.QueryContext(ctx, roomID)
	if err != nil {
		return nil, err
	}
	defer rows.Close() // nolint: errcheck

	var result []types.JoinedHost
	for rows.Next() {
		var eventID, serverName string
		if err = rows.Scan(&eventID, &serverName); err != nil {
			return nil, err
		}
		result = append(result, types.JoinedHost{
			MemberEventID: eventID,
			ServerName:    gomatrixserverlib.ServerName(serverName),
		})
	}

	return result, nil
}
