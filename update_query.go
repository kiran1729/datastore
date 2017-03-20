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

package datastore

import (
  "fmt"
  "reflect"
  "strings"
  "time"

  "github.com/zerostackinc/dbconn"
  "github.com/zerostackinc/util"
)

// NewUpdateQuery returns a UpdateQuery instance given a typ. Typ needs to be
// a struct with column definitions with CQL tags. Refer to datastore_test
// for examples.
func NewUpdateQuery(typ reflect.Type) (*UpdateQuery, error) {
  codec, err := getStructCodec(typ)
  if err != nil {
    return nil, err
  }
  return &UpdateQuery{
    codec:   codec,
    ttl:     noTTL,
    updates: make(map[string]interface{}),
    addOps:  make(map[string]interface{}),
    delOps:  make(map[string]interface{}),
  }, nil
}

// NewUpdateAllQuery returns a UpdateQuery instance with the filters and updates
// setup as per the input.
func NewUpdateAllQuery(src interface{}) (*UpdateQuery, error) {
  einfo, err := newEntityInfo(src)
  if err != nil {
    return nil, err
  }

  codec := einfo.codec
  q := &UpdateQuery{
    codec:   codec,
    ttl:     noTTL,
    updates: make(map[string]interface{}),
    addOps:  make(map[string]interface{}),
    delOps:  make(map[string]interface{}),
  }

  // Get all values from the src. If the field below to primary key append it to
  // the filter list. Else append it to the update list.
  vals := einfo.getValues()
  for i := 0; i < len(codec.cols); i++ {
    if codec.pk.isPresent(codec.cols[i]) {
      q = q.Filter(codec.cols[i], Equal, vals[i])
    } else {
      q = q.Update(codec.cols[i], vals[i])
    }
  }

  return q, nil
}

// UpdateQuery represent a CQL update query.
type UpdateQuery struct {
  filter  []filter
  ttl     int64
  updates map[string]interface{}
  // add represent + operations on collection type fields
  addOps map[string]interface{}
  // del represent - operations on collection type fields
  delOps     map[string]interface{}
  codec      *structCodec
  predicates []filter

  err error
}

func (q *UpdateQuery) clone() *UpdateQuery {
  x := *q
  if len(q.filter) > 0 {
    x.filter = make([]filter, len(q.filter))
    copy(x.filter, q.filter)
  }
  if len(q.updates) > 0 {
    x.updates = make(map[string]interface{})
    for k, v := range q.updates {
      x.updates[k] = v
    }
  }
  return &x
}

// Filter returns a derivative query with a field-based filter.
// Args : The field name (with optional space), Operator, and value.
// Fields are compared against the provided value using the operator.
// Multiple filters are AND'ed together.
func (q *UpdateQuery) Filter(field string, op Operator,
  value interface{}) *UpdateQuery {

  q = q.clone()
  f, err := createFilter(field, op, value)
  if err != nil {
    q.err = err
    return q
  }

  q.filter = append(q.filter, *f)
  return q
}

// TTL specifies ttl in seconds for the CQL update query.
func (q *UpdateQuery) TTL(ttl int64) (*UpdateQuery, error) {
  if ttl < 0 {
    return nil, fmt.Errorf("error ttl %d is less than zero", ttl)
  }
  q = q.clone()
  q.ttl = ttl
  return q, nil
}

// Update appends given fieldName to the list of columns that will be updated
// when query is executed.
func (q *UpdateQuery) Update(fieldName string,
  fieldVal interface{}) *UpdateQuery {

  q = q.clone()
  q.updates[fieldName] = fieldVal
  return q
}

// Add adds given val to field collection on given entity.
func (q *UpdateQuery) Add(field string, val interface{}) *UpdateQuery {
  q = q.clone()
  q.addOps[field] = val
  return q
}

// Remove removes given val from field collection on given entity.
func (q *UpdateQuery) Remove(field string, val interface{}) *UpdateQuery {
  q = q.clone()
  q.delOps[field] = val
  return q
}

// If returns a derivative query by appending the requested predicate.
func (q *UpdateQuery) If(field string, op Operator,
  value interface{}) *UpdateQuery {

  q = q.clone()
  f, err := createFilter(field, op, value)
  if err != nil {
    q.err = err
    return q
  }

  q.predicates = append(q.predicates, *f)
  return q
}

func (q *UpdateQuery) toCQL() (cql string, args []interface{}, err error) {

  setClause, setArgs, err := q.setClause()
  if err != nil {
    return "", nil, err
  }

  whereClause, whereArgs, err := getWhereClause(q.codec, q.filter)
  if err != nil {
    return "", nil, err
  }

  ifClause, ifArgs, err := getIfClause(q.codec, q.predicates)
  if err != nil {
    return "", nil, err
  }

  // UPDATE <schema> using TTL <ttl> <set-clause> <where-clause> <if-clause>
  if q.ttl == noTTL {
    cql = fmt.Sprintf("UPDATE %s %s %s %s", q.codec.columnFamily,
      setClause, whereClause, ifClause)
  } else {
    cql = fmt.Sprintf("UPDATE %s USING TTL %d %s %s %s", q.codec.columnFamily,
      q.ttl, setClause, whereClause, ifClause)
  }

  args = append(args, setArgs...)
  args = append(args, whereArgs...)
  args = append(args, ifArgs...)

  return cql, args, nil
}

// CQL returns the CQL query statement corresponding to the update query.
func (q *UpdateQuery) CQL() (string, error) {
  cql, _, err := q.toCQL()
  return cql, err
}

// Run executes the update query.
func (q *UpdateQuery) Run(dbConn dbconn.DBConn) error {
  defer util.LogExecutionTime(1, time.Now(), "datastore:update_query:run")
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

  if !q.isPredicateQuery() {
    // If query is without predicates, nothing to CAS
    return cqlQ.Exec()
  }

  // For update queries with predicates, ScanCAS will return the rows that
  // describe if the query was successfully applied.
  // If not, it populates the list of interfaces with the current value of the
  // predicate field.
  // ** Success case **
  // [applied]
  // -----------
  //       True
  //
  // ** Fail case **
  // [applied] | finite_duration | life_in_days
  // -----------+-----------------+--------------
  //      False |           False |            0

  // Construct list of interfaces of type same as the predicate fields.
  previousVals := make([]interface{}, len(q.predicates))
  for idx, filter := range q.predicates {
    previousValInf := reflect.New(reflect.TypeOf(filter.Value)).Interface()
    previousVals[idx] = previousValInf
  }

  queryHash := q.hash()
  casLockManager.GetLock(queryHash)
  defer casLockManager.ReleaseLock(queryHash)

  applied, err := cqlQ.ScanCAS(previousVals...)
  if err != nil {
    return err
  }
  if !applied {
    previousValsMap := make(map[string]interface{}, len(q.predicates))
    for idx, filter := range q.predicates {
      previousValsMap[filter.FieldName] = previousVals[idx]
    }
    return &ErrNotApplied{previousValsMap}
  }

  return nil
}

func (q *UpdateQuery) isPredicateQuery() bool {
  return len(q.predicates) > 0
}

func (q *UpdateQuery) setClause() (string, []interface{}, error) {
  if len(q.updates) <= 0 && len(q.addOps) <= 0 && len(q.delOps) <= 0 {
    return "", nil, nil
  }

  updates := make([]string, len(q.updates)+len(q.addOps)+len(q.delOps))
  setArgs := make([]interface{}, len(updates))
  i := 0
  for k, v := range q.updates {
    updates[i] = fmt.Sprintf("%s = ?", k)
    setArgs[i] = v
    i++
  }
  for k, v := range q.addOps {
    updates[i] = fmt.Sprintf("%s = %s + ?", k, k)
    setArgs[i] = v
    i++
  }
  for k, v := range q.delOps {
    updates[i] = fmt.Sprintf("%s = %s - ?", k, k)
    setArgs[i] = v
    i++
  }
  setClause := fmt.Sprintf("SET %s", strings.Join(updates, ", "))
  return setClause, setArgs, nil
}

// partitionKey returns a concatenated string of partition keys.
func (q *UpdateQuery) partitionKey() string {
  pKeyGen := q.codec.columnFamily
  for _, filter := range q.filter {
    if !q.codec.pk.isPresentInPartitionKey(filter.FieldName) {
      continue
    }

    pKeyGen = fmt.Sprintf("%s%s", pKeyGen,
      interfacePtrStr(reflect.ValueOf(filter.Value)))
  }
  return pKeyGen
}

// hash returns the hash for partition key.
func (q *UpdateQuery) hash() int {
  return util.HashStr(q.partitionKey())
}
