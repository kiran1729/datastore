// Copyright (c) 2015 ZeroStack Inc.
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
//

package datastore

import (
  "fmt"
  "reflect"
  "time"

  "github.com/zerostackinc/dbconn"
  "github.com/zerostackinc/util"
)

const noTTL = -1

// insertQuery represents a cql insert query.
type insertQuery struct {
  einfo      *entityInfo
  ttl        int64
  typ        reflect.Type
  ifNotExist bool
}

// newInsertQuery returns an instance of insert query.
func newInsertQuery(src interface{}) (*insertQuery, error) {
  einfo, err := newEntityInfo(src)
  if err != nil {
    return nil, err
  }
  return &insertQuery{
    einfo: einfo,
    ttl:   noTTL,
    typ:   reflect.TypeOf(src).Elem(),
  }, nil
}

// SetTTL specifies ttl in seconds for the CQL update query.
func (q *insertQuery) SetTTL(ttl int64) (*insertQuery, error) {
  if ttl < 0 {
    return nil, fmt.Errorf("error ttl %d is less than zero", ttl)
  }
  q = q.clone()
  q.ttl = ttl
  return q, nil
}

func (q *insertQuery) ifNotExists() *insertQuery {
  q = q.clone()
  q.ifNotExist = true
  return q
}

func (q *insertQuery) clone() *insertQuery {
  x := *q
  return &x
}

func (q *insertQuery) toCQL() (cql string, args []interface{}) {
  codec := q.einfo.codec
  vals := q.einfo.getValues()
  notExists := ""
  if q.ifNotExist {
    notExists = " IF NOT EXISTS "
  }
  var queryStr string
  if q.ttl == noTTL {
    queryStr = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) %s",
      codec.columnFamily, codec.getColumnStr(), codec.getPlaceHolderStr(),
      notExists)
  } else {
    queryStr = fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s) %s USING TTL %d",
      codec.columnFamily, codec.getColumnStr(), codec.getPlaceHolderStr(),
      notExists, q.ttl)
  }
  return queryStr, vals
}

// run executes a insert query.
func (q *insertQuery) run(dbConn dbconn.DBConn) error {
  defer util.LogExecutionTime(1, time.Now(), "datastore:insert_query:run_internal")

  session := dbConn.GetSession()
  if session == nil {
    return fmt.Errorf("invalid session")
  }
  defer dbConn.ReleaseSession(session)

  cql, args := q.toCQL()

  cqlQ := session.Query(cql, args...)
  if !q.ifNotExist {
    return cqlQ.Exec()
  }

  // For insert queries with CAS, MapScanCAS will return a map with keys as column
  // names and interface values representing original values if the cas was not
  // applied.

  queryHash := q.hash()
  casLockManager.GetLock(queryHash)
  defer casLockManager.ReleaseLock(queryHash)

  previousValsMap := make(map[string]interface{})
  applied, err := cqlQ.MapScanCAS(previousValsMap)
  if err != nil {
    return err
  }
  if !applied {
    return &ErrNotApplied{previousValsMap}
  }

  return nil
}

func (q *insertQuery) partitionKey() string {
  pKeyGen := q.einfo.codec.columnFamily
  einfo := q.einfo
  codec := einfo.codec
  for _, v := range codec.byIndex {
    if v.name == "-" {
      continue
    }

    if !codec.pk.isPresentInPartitionKey(v.name) {
      continue
    }

    field := einfo.v.Field(codec.byName[v.name].index)

    pKeyGen = fmt.Sprintf("%s%s", pKeyGen, interfacePtrStr(field))
  }
  return pKeyGen
}

// hash returns the hash for partition key.
func (q *insertQuery) hash() int {
  return util.HashStr(q.partitionKey())
}
