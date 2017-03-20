// Copyright (c) 2014 ZeroStack Inc.
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
  "errors"
  "fmt"
  "reflect"
  "strings"
  "time"

  "github.com/zerostackinc/dbconn"
  "github.com/zerostackinc/util"
)

// NewDeleteQuery returns a DeleteQuery instance given a typ. Typ needs to be
// a struct with column definitions with CQL tags. Refer to datastore_test
// for examples.
func NewDeleteQuery(typ reflect.Type) (*DeleteQuery, error) {
  codec, err := getStructCodec(typ)
  if err != nil {
    return nil, err
  }
  return &DeleteQuery{
    codec:      codec,
    deleteCols: []string{},
  }, nil
}

// DeleteQuery represent a CQL update query.
type DeleteQuery struct {
  filter     []filter
  deleteCols []string
  codec      *structCodec

  err error
}

func (q *DeleteQuery) clone() *DeleteQuery {
  x := *q
  if len(q.filter) > 0 {
    x.filter = make([]filter, len(q.filter))
    copy(x.filter, q.filter)
  }
  if len(q.deleteCols) > 0 {
    x.deleteCols = make([]string, len(q.deleteCols))
    copy(x.deleteCols, q.deleteCols)
  }
  return &x
}

// Filter returns a derivative query with a field-based filter.
// Args : The field name (with optional space), Operator, and value.
// Fields are compared against the provided value using the operator.
// Multiple filters are AND'ed together.
func (q *DeleteQuery) Filter(field string, op Operator,
  value interface{}) *DeleteQuery {

  q = q.clone()
  field = strings.TrimSpace(field)
  if len(field) < 1 {
    q.err = errors.New("datastore: invalid filter: " + field)
    return q
  }

  // IN expects values as Slice.
  if op == IN && reflect.TypeOf(value).Kind() != reflect.Slice {
    q.err = errors.New("datastore: invalid IN :: " + "filter " + field +
      ", expects Slice got " + reflect.TypeOf(value).Kind().String())
    return q
  }

  f := filter{
    FieldName: field,
    Op:        op,
    Value:     value,
  }
  q.filter = append(q.filter, f)
  return q
}

// AddColumnsToDelete adds given fieldName to the list of columns that will be
// deleted when query is executed. Empty fieldnames will delete the entire row.
func (q *DeleteQuery) AddColumnsToDelete(fieldNames ...string) *DeleteQuery {
  q = q.clone()
  q.deleteCols = append([]string(nil), fieldNames...)
  return q
}

func (q *DeleteQuery) toCQL() (cql string, args []interface{}, err error) {
  codec := q.codec

  columnStr := ""
  if len(q.deleteCols) > 0 {
    columnStr = strings.Join(q.deleteCols, ",")
  }
  cql = fmt.Sprintf("DELETE %s FROM %s", columnStr, codec.columnFamily)

  whereClause, whereArgs, err := getWhereClause(q.codec, q.filter)
  if err != nil {
    return "", whereArgs, err
  }
  cql = cql + whereClause
  args = append(args, whereArgs...)

  return cql, args, nil
}

// CQL returns the CQL query statement corresponding to the update query.
func (q *DeleteQuery) CQL() (string, error) {
  cql, _, err := q.toCQL()
  return cql, err
}

// Run executes the DeleteQuery.
func (q *DeleteQuery) Run(dbConn dbconn.DBConn) error {
  defer util.LogExecutionTime(1, time.Now(), "datastore:delete_query:run")
  session := dbConn.GetSession()
  if session == nil {
    return fmt.Errorf("invalid session")
  }
  defer dbConn.ReleaseSession(session)

  cql, args, err := q.toCQL()
  if err != nil {
    return err
  }
  cqlQ := session.Query(cql, args...)
  return cqlQ.Exec()
}
