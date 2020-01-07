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

package postgres

import (
	"database/sql"
)

type PostgresJoinedHostsTable struct {
}

func (t *PostgresJoinedHostsTable) Schema() string {
	return `
	-- The joined_hosts table stores a list of m.room.member event ids in the
	-- current state for each room where the membership is "join".
	-- There will be an entry for every user that is joined to the room.
	CREATE TABLE IF NOT EXISTS federationsender_joined_hosts (
	    -- The string ID of the room.
	    room_id TEXT NOT NULL,
	    -- The event ID of the m.room.member join event.
	    event_id TEXT NOT NULL,
	    -- The domain part of the user ID the m.room.member event is for.
	    server_name TEXT NOT NULL
	);

	CREATE UNIQUE INDEX IF NOT EXISTS federatonsender_joined_hosts_event_id_idx
	    ON federationsender_joined_hosts (event_id);

	CREATE INDEX IF NOT EXISTS federatonsender_joined_hosts_room_id_idx
	    ON federationsender_joined_hosts (room_id)
	`
}

func (t *PostgresJoinedHostsTable) InsertJoinedHostsStmt(db *sql.DB) (*sql.Stmt, error) {
	return db.Prepare(`
		INSERT INTO federationsender_joined_hosts (room_id, event_id, server_name)
		  VALUES ($1, $2, $3)
	`)
}

func (t *PostgresJoinedHostsTable) DeleteJoinedHostsStmt(db *sql.DB) (*sql.Stmt, error) {
	return db.Prepare(`
		DELETE FROM federationsender_joined_hosts WHERE event_id = $1
	`)
}

func (t *PostgresJoinedHostsTable) SelectJoinedHostsStmt(db *sql.DB) (*sql.Stmt, error) {
	return db.Prepare(`
		SELECT event_id, server_name FROM federationsender_joined_hosts
		  WHERE room_id = $1
	`)
}
