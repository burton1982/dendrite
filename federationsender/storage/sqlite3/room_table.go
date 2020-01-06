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

package sqlite3

import (
	"database/sql"
)

type SqliteRoomTable struct {
}

const roomSchema = `
CREATE TABLE IF NOT EXISTS federationsender_rooms (
    -- The string ID of the room
    room_id TEXT PRIMARY KEY,
    -- The most recent event state by the room server.
    -- We can use this to tell if our view of the room state has become
    -- desynchronised.
    last_event_id TEXT NOT NULL
);`

func (t *SqliteRoomTable) Schema() string {
	return roomSchema
}

const insertRoomSQL = "" +
	"INSERT INTO federationsender_rooms (room_id, last_event_id) VALUES ($1, '')" +
	" ON CONFLICT DO NOTHING"

func (t *SqliteRoomTable) InsertRoomStmt(db *sql.DB) (*sql.Stmt, error) {
	return db.Prepare(insertRoomSQL)
}

const selectRoomForUpdateSQL = "" +
	"SELECT last_event_id FROM federationsender_rooms WHERE room_id = $1 FOR UPDATE"

func (t *SqliteRoomTable) SelectRoomForUpdateStmt(db *sql.DB) (*sql.Stmt, error) {
	return db.Prepare(selectRoomForUpdateSQL)
}

const updateRoomSQL = "" +
	"UPDATE federationsender_rooms SET last_event_id = $2 WHERE room_id = $1"

func (t *SqliteRoomTable) UpdateRoomStmt(db *sql.DB) (*sql.Stmt, error) {
	return db.Prepare(updateRoomSQL)
}
