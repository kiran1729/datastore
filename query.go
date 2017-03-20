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

package datastore

import (
  "errors"
  "fmt"
  "math"
  "reflect"
  "strings"
  "time"

  "github.com/gocql/gocql"
  "github.com/zerostackinc/dbconn"
  "github.com/zerostackinc/util"
)

// Operator for filtering.
type Operator string

// Operator constants.
const (
  LessThan     Operator = "<"
  LessEq       Operator = "<="
  Equal        Operator = "="
  GreaterEq    Operator = ">="
  GreaterThan  Operator = ">"
  IN           Operator = "IN"
  cPageSize    int      = 100
  cWhereClause string   = "WHERE"
  cIfClause    string   = "IF"
  cCASLockCnt  int      = 200
)

// ErrDone is returned when a query iteration has completed.
var ErrDone = errors.New("datastore: query has no more results")

// casLockManager manages the locking for insert and update cas query.
var casLockManager = util.NewLockManager(cCASLockCnt)

// ErrNotApplied is returned if CAS update query is not successfully applied
// ErrNotApplied represents an error message when a query is not applied because
// of predicate mismatch.
type ErrNotApplied struct {
  previousVals map[string]interface{}
}

// Error returns error for ErrNotApplied.
func (e *ErrNotApplied) Error() string {
  return "datastore: query not applied"
}

// PreviousVals returns a map of previous values on a predicate query falire.
func (e *ErrNotApplied) PreviousVals() map[string]interface{} {
  return e.previousVals
}

// GetPreviousVal returns a fields value.
func (e *ErrNotApplied) GetPreviousVal(field string) (interface{}, bool) {
  val, found := e.previousVals[field]
  return val, found
}

// IsErrNotApplied returns true if err is of type ErrNotApplied.
// For nil error it returns false.
func IsErrNotApplied(err error) (*ErrNotApplied, bool) {
  if err == nil {
    return nil, false
  }
  errNotApplied, ok := err.(*ErrNotApplied)
  return errNotApplied, ok
}

// ErrInvalidCASQuery is returned is update query is not of CAS type
var ErrInvalidCASQuery = errors.New("datastore: invalid cas query")

// filter is a conditional filter on query results.
type filter struct {
  FieldName string
  Op        Operator
  Value     interface{}
}

// getWhereClause is a helper function to get the Where clause related info to
// construct CQL query.
func getWhereClause(codec *structCodec, filters []filter) (
  cond string, args []interface{}, err error) {

  return getClause(cWhereClause, codec, filters)
}

// getIfClause is a helper function to get the If clause related info to
// construct CQL query.
func getIfClause(codec *structCodec, filters []filter) (
  cond string, args []interface{}, err error) {

  return getClause(cIfClause, codec, filters)
}

func getClause(clause string, codec *structCodec, filters []filter) (
  cond string, args []interface{}, err error) {

  if len(filters) <= 0 {
    return cond, args, err
  }
  conditions := make([]string, len(filters))
  for i, filter := range filters {
    _, ok := codec.byName[filter.FieldName]
    if !ok {
      return cond, args,
        fmt.Errorf("query : fieldname %s not found", filter.FieldName)
    }
    conditions[i] = fmt.Sprintf("%s %s ?", filter.FieldName,
      filter.Op)
    args = append(args, filter.Value)
  }
  cond = fmt.Sprintf(" %s %s", clause, strings.Join(conditions, " AND "))
  return cond, args, err
}

type sortDirection string

const (
  ascending  sortDirection = "ASC"
  descending sortDirection = "DESC"
)

// order is a sort order on query results.
type order struct {
  FieldName string
  Direction sortDirection
}

// NewQuery creates a new Query given an entity type.
func NewQuery(typ reflect.Type) (*Query, error) {
  codec, err := getStructCodec(typ)
  if err != nil {
    return nil, err
  }
  return &Query{
    limit:     -1,
    codec:     codec,
    filtering: true,
    pageSize:  cPageSize,
  }, nil
}

// Query represents a CQL query.
type Query struct {
  filter     []filter
  order      []order
  projection []string
  codec      *structCodec
  limit      int32
  // Allow filtering for query
  filtering bool
  // Page size tells the iterator to fetch results in pages of size n
  pageSize int

  err error
}

func (q *Query) clone() *Query {
  x := *q
  // Copy the contents of the slice-typed fields
  if len(q.filter) > 0 {
    x.filter = make([]filter, len(q.filter))
    copy(x.filter, q.filter)
  }
  if len(q.order) > 0 {
    x.order = make([]order, len(q.order))
    copy(x.order, q.order)
  }
  return &x
}

// Filter returns a derivative query with a field-based filter.
// Args : The field name (with optional space), Operator, and value.
// Fields are compared against the provided value using the operator.
// Multiple filters are AND'ed together.
func (q *Query) Filter(field string, op Operator,
  value interface{}) *Query {

  q = q.clone()
  f, err := createFilter(field, op, value)
  if err != nil {
    q.err = err
    return q
  }

  q.filter = append(q.filter, *f)
  return q
}

// Order returns a derivative query with a field-based sort order. Orders are
// applied in the order they are added. The default order is ascending; to sort
// in descending order prefix the fieldName with a minus sign (-).
func (q *Query) Order(fieldName string) *Query {
  q = q.clone()
  fieldName = strings.TrimSpace(fieldName)
  o := order{
    Direction: ascending,
    FieldName: fieldName,
  }
  if strings.HasPrefix(fieldName, "-") {
    o.Direction = descending
    o.FieldName = strings.TrimSpace(fieldName[1:])
  } else if strings.HasPrefix(fieldName, "+") {
    q.err = fmt.Errorf("datastore: invalid order: %q", fieldName)
    return q
  }
  if len(o.FieldName) == 0 {
    q.err = errors.New("datastore: empty order")
    return q
  }
  q.order = append(q.order, o)
  return q
}

// Filtering returns a derivative query with the specified filtering param.
func (q *Query) Filtering(filtering bool) *Query {
  q = q.clone()
  q.filtering = filtering
  return q
}

// PageSize returns a derivative query with the specified page size.
// Page size of 0 disables paging.
func (q *Query) PageSize(pageSize int) *Query {
  q = q.clone()
  q.pageSize = pageSize
  return q
}

// Project returns a derivative query that yields only the given fields.
func (q *Query) Project(fieldNames ...string) *Query {
  q = q.clone()
  q.projection = append([]string(nil), fieldNames...)
  return q
}

// Limit returns a derivative query that has a limit on the number of results
// returned. A negative value means unlimited.
func (q *Query) Limit(limit int) *Query {
  q = q.clone()
  if limit < math.MinInt32 || limit > math.MaxInt32 {
    q.err = errors.New("datastore: query limit overflow")
    return q
  }
  q.limit = int32(limit)
  return q

}

// toCQL returns CQL query statement corresponding to the query q.
func (q *Query) toCQL() (string, []interface{}, error) {
  codec := q.codec

  var columnStr string
  if len(q.projection) > 0 {
    columnStr = strings.Join(q.projection, ",")
  } else {
    columnStr = codec.getColumnStr()
  }

  cql := fmt.Sprintf("SELECT %s FROM %s", columnStr, codec.columnFamily)

  var args []interface{}

  whereClause, whereArgs, err := getWhereClause(q.codec, q.filter)
  if err != nil {
    return "", whereArgs, err
  }

  // WHERE
  cql = cql + whereClause
  args = append(args, whereArgs...)

  // ORDER BY
  if len(q.order) > 0 {
    if len(q.order) != 1 {
      return "", whereArgs, fmt.Errorf("query: multiple ORDER BY fields")
    }
    cql = cql + fmt.Sprintf(" ORDER BY %s %s",
      q.order[0].FieldName, q.order[0].Direction)
  }

  // LIMIT
  if q.limit > 0 {
    cql = cql + fmt.Sprintf(" LIMIT %d", q.limit)
  }

  if q.filtering {
    cql = cql + " ALLOW FILTERING "
  }

  return cql, args, nil
}

// Run returns Iterator by executing the query.
func (q *Query) Run(dbConn dbconn.DBConn) *Iterator {
  defer util.LogExecutionTime(1, time.Now(), "datastore:query:run")
  session := dbConn.GetSession()
  if session == nil {
    return &Iterator{err: fmt.Errorf("chronos: session is nil")}
  }

  if q.err != nil {
    return &Iterator{err: q.err}
  }

  cql, args, err := q.toCQL()
  if err != nil {
    return &Iterator{
      dbConn:  dbConn,
      session: session,
      err:     err,
    }
  }

  cqlQ := session.Query(cql, args...)
  cqlQ.PageSize(q.pageSize)
  iter := cqlQ.Iter()

  t := &Iterator{
    q:        q,
    dbConn:   dbConn,
    session:  session,
    iter:     iter,
    cql:      cql,
    cqlQuery: cqlQ,
  }
  return t
}

// First captures the first query result in dst object.
func (q *Query) First(dbConn dbconn.DBConn, dst interface{}) error {
  defer util.LogExecutionTime(1, time.Now(), "datastore:query:first")
  if dbConn == nil {
    return fmt.Errorf("invalid dbConn")
  }

  iter := q.Run(dbConn)
  if iter.err != nil {
    iter.Close()
    return iter.err
  }
  if err := iter.Next(dst); err == ErrDone {
    // we couldn't even fetch first row here
    iter.Close()
    return gocql.ErrNotFound
  }
  return iter.Close()
}

// Iterator is the result of running a query.
type Iterator struct {
  dbConn   dbconn.DBConn
  session  *gocql.Session
  iter     *gocql.Iter
  cql      string
  cqlQuery *gocql.Query
  err      error
  // limit is the limit on the number of results this iterator should return.
  // A negative value means unlimited.
  limit int32
  // q is the original query which yielded this iterator.
  q *Query
}

// Next returns row of the next result. When there are no more results,
// ErrDone is returned as the error.
func (t *Iterator) Next(dst interface{}) error {
  iter := t.iter
  return LoadEntity(dst, iter)
}

// Close closed the iterator.
func (t *Iterator) Close() error {
  if t.dbConn != nil {
    t.dbConn.ReleaseSession(t.session)
  }
  if t.iter == nil {
    return nil
  }
  return t.iter.Close()
}

func createFilter(field string, op Operator,
  value interface{}) (*filter, error) {

  field = strings.TrimSpace(field)
  if len(field) < 1 {
    return nil, errors.New("datastore: invalid filter: " + field)
  }

  // IN expects values as Slice.
  if op == IN && reflect.TypeOf(value).Kind() != reflect.Slice {
    return nil, errors.New("datastore: invalid IN :: " + "filter " + field +
      ", expects Slice got " + reflect.TypeOf(value).Kind().String())
  }

  f := filter{
    FieldName: field,
    Op:        op,
    Value:     value,
  }
  return &f, nil
}

// interfacePtrStr returns a string representation of reflect.Value
// Only supports string and int ptr types.
func interfacePtrStr(val reflect.Value) string {
  if val.Kind() != reflect.Ptr {
    return ""
  }

  e := reflect.Indirect(val)
  switch e.Kind() {
  case reflect.String:
    return e.String()
  case reflect.Int, reflect.Int64:
    return fmt.Sprintf("%d", e.Int())
  }

  return ""
}
