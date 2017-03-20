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
  "fmt"
  "reflect"
  "strings"
  "sync"
  "time"

  "github.com/gocql/gocql"
  "github.com/zerostackinc/dbconn"
  "github.com/zerostackinc/util"
)

// structTag is the parsed `cql:"name,options"` tag of a struct field.
// if a field has no tag, or the tag has an empty name, then the structTag's
// name is just the field name. A "-" name means that the datastore ignores
// that field.
type structTag struct {
  name string
  opts string
}

// key represent primary key constituents (partition key or cluster key).
type key struct {
  // map of key items (needed for fast key-item lookups)
  m map[string]bool
  // order list of key items (will be needed for schema generation)
  l []string
}

// newKey returns Key instance given a comma separted keyStr.
func newKey(keyStr string) *key {
  k := &key{
    m: map[string]bool{},
    l: nil,
  }
  for _, v := range strings.Split(keyStr, ",") {
    vv := strings.TrimSpace(v)
    if vv != "" {
      k.m[vv] = true
      k.l = append(k.l, vv)
    }
  }
  return k
}

func (k *key) isPresent(field string) bool {
  return k.m[field]
}

// primaryKey represent primay key for a column family.
type primaryKey struct {
  partitionKey *key
  clusterKey   *key
}

func (pk *primaryKey) isPresentInPartitionKey(field string) bool {
  return pk.partitionKey.isPresent(field)
}

func (pk *primaryKey) isPresentInClusterKey(field string) bool {
  return pk.clusterKey.isPresent(field)
}

func (pk *primaryKey) isPresent(field string) bool {
  return pk.isPresentInPartitionKey(field) || pk.isPresentInClusterKey(field)
}

// newPrimaryKey returns an instance of PrimaryKey given paritionKey and
// clusterKey strings.
func newPrimaryKey(pkStr, ckStr string) (*primaryKey, error) {
  if pkStr == "" {
    return nil, fmt.Errorf("datastore : missing primary key")
  }
  pk := &primaryKey{
    partitionKey: newKey(pkStr),
    clusterKey:   newKey(ckStr),
  }
  return pk, nil
}

// parsePrimaryKey parses given string and returns PrimaryKey instance.
func parsePrimaryKey(pkStr string) (*primaryKey, error) {

  pkStr = strings.TrimSpace(pkStr)
  if pkStr == "" {
    return nil, fmt.Errorf("datastore :: empty primarykey string")
  }
  if !strings.HasPrefix(pkStr, "(") || !strings.HasSuffix(pkStr, ")") {
    return nil, fmt.Errorf("datastore :: missing parans in primary key %s",
      pkStr)
  }
  pkStr = pkStr[1 : len(pkStr)-1]
  if len(pkStr) == 0 {
    return nil, fmt.Errorf("datastore :: empty content in parens in pk: %s",
      pkStr)
  }
  var partitionKeyStr, clusterKeyStr string
  if strings.Index(pkStr, "(") == 0 {
    // composite primary key
    j := strings.Index(pkStr, ")")
    if j == -1 {
      return nil, fmt.Errorf("datastore :: ill defined primary key: %s", pkStr)
    }
    partitionKeyStr = pkStr[1:j]
    if len(pkStr) >= j+1 {
      clusterKeyStr = pkStr[j+1 : len(pkStr)]
    }
  } else {
    if j := strings.Index(pkStr, ","); j != -1 {
      partitionKeyStr, clusterKeyStr = pkStr[:j], pkStr[j+1:]
    } else {
      partitionKeyStr = pkStr
    }
  }
  return newPrimaryKey(partitionKeyStr, clusterKeyStr)
}

// structCodec describes how to convert a struct to and from a sequence of
// column values.
type structCodec struct {
  // column family name this struct represent
  columnFamily string

  // primary key for the column family this represent
  pk *primaryKey

  // byIndex gives the structTag for the i'th field.
  byIndex []structTag

  // byName gives the field codec for the structTag with the given name.
  byName map[string]fieldCodec

  // cols is the list of all the column names in the codec.
  cols []string
}

// fieldCodec is a struct field's index
type fieldCodec struct {
  index int
}

// structCodecs collects the structCodecs that have already been calculated.
var (
  structCodecsMutex sync.Mutex
  structCodecs      = make(map[reflect.Type]*structCodec)
)

func (codec *structCodec) getColumnStr() string {
  return strings.Join(codec.cols, ",")
}

func (codec *structCodec) getPlaceHolderStr() string {
  placeHolders := make([]string, len(codec.cols))
  for placeIdx := 0; placeIdx < len(codec.cols); placeIdx++ {
    placeHolders[placeIdx] = "?"
  }
  return strings.Join(placeHolders, ",")
}

func getStructCodec(t reflect.Type) (*structCodec, error) {
  structCodecsMutex.Lock()
  defer structCodecsMutex.Unlock()
  return getStructCodecLocked(t)
}

func getStructCodecLocked(t reflect.Type) (ret *structCodec, err error) {
  c, ok := structCodecs[t]
  if ok {
    return c, nil
  }
  c = &structCodec{
    byIndex: make([]structTag, t.NumField()),
    byName:  make(map[string]fieldCodec),
    cols:    []string{},
  }

  structCodecs[t] = c
  defer func() {
    if err != nil {
      delete(structCodecs, t)
    }
  }()

  // iterate over each struct field
  for i := range c.byIndex {

    f := t.Field(i)
    name, opts := f.Tag.Get("cql"), ""

    if ii := strings.Index(name, ","); ii != -1 {
      // comma found in the tag
      name, opts = name[:ii], name[ii+1:]
    }

    if name == "" {
      if !f.Anonymous {
        // if no name has been assigned, use the struct field name
        name = f.Name
      } else {
        // if no name is assigned and field is anonymous, ignore the field
        name = "-"
      }
    }

    if f.Name == "ColumnFamily" {
      if name == "" || name == "-" {
        return nil, fmt.Errorf("datastore: name %s not allowed", name)
      }
      c.columnFamily = name
      name = "-" // ignore this columnFamily for DB storage

      pk, errParse := parsePrimaryKey(opts)
      if err != nil {
        return nil, errParse
      }
      c.pk = pk
    }
    if f.Name == "XXX_unrecognized" {
      // XXX_unrecognized is present in the models generated by gogoprotobuf
      // and there is no easy way to assign it cql ignore tag
      name = "-" // ignore this columnFamily for DB storage
    }
    // TODO: Check if the name is valid or not
    if name != "-" {
      // we need lookup by name for db fields only.
      c.byName[name] = fieldCodec{index: i}
      c.cols = append(c.cols, name)
    }
    c.byIndex[i] = structTag{name: name, opts: opts}
  }

  if c.columnFamily == "" {
    // column family is not defined for this entity type
    return nil,
      fmt.Errorf("datastore: ColumnFamily field missing in %v", t)
  }
  if c.pk == nil {
    return nil, fmt.Errorf("datastore: PK not defined for %v", t)
  }
  return c, nil
}

// entityInfo contains info for decoding a struct from a CQL scanner.
type entityInfo struct {
  v     reflect.Value
  codec *structCodec
}

// allocateIfPtr allocates pointer of a field type if required.
func allocateIfNilPtr(field reflect.Value) {
  // TODO: check if field.IsNil() is unnecessary
  switch field.Kind() {
  case reflect.Ptr:
    if field.IsNil() && field.CanSet() {
      field.Set(reflect.New(field.Type().Elem()))
    }
  case reflect.Map:
    if field.IsNil() && field.CanSet() {
      field.Set(reflect.MakeMap(field.Type()))
    }
  case reflect.Slice:
    if field.IsNil() && field.CanSet() {
      field.Set(reflect.MakeSlice(field.Type(), field.Len(), field.Len()))
    }
  default:
  }
}

func (einfo *entityInfo) load(iter *gocql.Iter) error {
  if iter == nil {
    return fmt.Errorf("invalid input params")
  }
  rowData, err := iter.RowData()
  if err != nil {
    return err
  }
  for i, col := range rowData.Columns {
    f, ok := einfo.codec.byName[col]
    if ok {
      field := einfo.v.Field(f.index)
      allocateIfNilPtr(field)

      switch field.Kind() {
      case reflect.Ptr, reflect.Map:
        rowData.Values[i] = field.Interface()
      default:
        rowData.Values[i] = field.Addr().Interface()
      }
    }
  }
  if iter.Scan(rowData.Values...) {
    // row was fetched successfully
    return nil
  }

  // some error got caused while fetching next row. Call iter.Close() to
  // get the error.
  err = iter.Close()
  if err != nil {
    return err
  }
  // we are here means result exhausted
  return ErrDone
}

// getValues returns a list of values for all the fields in the codec.
func (einfo *entityInfo) getValues() []interface{} {
  i := 0
  vals := make([]interface{}, len(einfo.codec.cols))
  codec := einfo.codec
  for _, v := range codec.byIndex {
    if v.name == "-" {
      continue
    }
    field := einfo.v.Field(codec.byName[v.name].index)
    allocateIfNilPtr(field)

    switch field.Kind() {
    case reflect.Ptr:
      fe := field.Elem()
      // If it is a ptr to Map/Slice then we should dereference it, otherwise
      // we just use the field interface as is.
      if fe.Kind() == reflect.Map || fe.Kind() == reflect.Slice {
        vals[i] = fe.Interface()
      } else {
        vals[i] = field.Interface()
      }
    case reflect.Map:
      vals[i] = field.Interface()

    default:
      vals[i] = field.Addr().Interface()
    }
    i++
  }
  return vals
}

// save persists entityinfos, based on batch
func save(session *gocql.Session, doLoggedBatch bool, qs []*insertQuery) error {
  defer util.LogExecutionTime(1, time.Now(), "datastore:entity:save_internal")
  var cqlBatch *gocql.Batch
  if doLoggedBatch {
    cqlBatch = session.NewBatch(gocql.LoggedBatch)
  } else {
    cqlBatch = session.NewBatch(gocql.UnloggedBatch)
  }

  for ii := 0; ii < len(qs); ii++ {
    queryStr, vals := qs[ii].toCQL()
    cqlBatch.Query(queryStr, vals...)
  }

  if err := session.ExecuteBatch(cqlBatch); err != nil {
    return err
  }
  return nil
}

// newEntityInfo returns entityInfo.
func newEntityInfo(p interface{}) (*entityInfo, error) {
  v := reflect.ValueOf(p)
  if v.Kind() != reflect.Ptr || v.IsNil() || v.Elem().Kind() != reflect.Struct {
    return nil, fmt.Errorf("invalid entity type")
  }
  v = v.Elem()
  codec, err := getStructCodec(v.Type())
  if err != nil {
    return nil, err
  }
  return &entityInfo{v, codec}, nil
}

// LoadEntity loads the columns from iter to dst, dst must be a struct pointer.
func LoadEntity(dst interface{}, iter *gocql.Iter) error {
  ei, err := newEntityInfo(dst)
  if err != nil {
    return err
  }
  return ei.load(iter)
}

// GetEntity loads the given entity from DB.
func GetEntity(dbConn dbconn.DBConn, src interface{}) error {
  if dbConn == nil {
    return fmt.Errorf("invalid dbConn")
  }

  v := reflect.ValueOf(src)
  if v.Kind() != reflect.Ptr || v.IsNil() || v.Elem().Kind() != reflect.Struct {
    return fmt.Errorf("invalid entity type")
  }
  v = v.Elem()
  codec, err := getStructCodec(v.Type())
  if err != nil {
    return err
  }

  if codec.pk == nil {
    return fmt.Errorf("datastore : primary key not defined for %v", v.Type())
  }

  q, err := NewQuery(v.Type())
  if err != nil {
    return err
  }
  for _, vv := range codec.byIndex {
    if vv.name == "-" {
      continue
    }
    field := v.Field(codec.byName[vv.name].index)
    if field.Kind() != reflect.Ptr {
      continue
    }
    if codec.pk.isPresent(vv.name) {
      if field.Kind() == reflect.String && len(field.String()) == 0 {
        return fmt.Errorf("datastore: empty primary key %s", vv.name)
      }
      q = q.Filter(vv.name, Equal, field.Interface())
    }
  }
  err = q.First(dbConn, src)
  if err != nil {
    return err
  }

  return nil
}

// DeleteEntity deletes given entity from DB.
func DeleteEntity(dbConn dbconn.DBConn, src interface{}) error {
  if dbConn == nil {
    return fmt.Errorf("invalid dbConn")
  }

  v := reflect.ValueOf(src)
  if v.Kind() != reflect.Ptr || v.IsNil() || v.Elem().Kind() != reflect.Struct {
    return fmt.Errorf("invalid entity type %v", v.Kind())
  }
  v = v.Elem()
  codec, err := getStructCodec(v.Type())
  if err != nil {
    return err
  }

  if codec.pk == nil {
    return fmt.Errorf("datastore : primary key not defined for %v", v.Type())
  }

  q, err := NewDeleteQuery(v.Type())
  if err != nil {
    return err
  }
  for _, vv := range codec.byIndex {
    if vv.name == "-" {
      continue
    }
    field := v.Field(codec.byName[vv.name].index)
    if field.Kind() != reflect.Ptr {
      // all fields in our DB models are pointers, so we skip non-pointers for
      // now.
      continue
    }
    if field.IsNil() {
      // nil fields are supposed to be skipped.
      continue
    }
    if codec.pk.isPresent(vv.name) {
      // if field is part of PK, add it to filter clause
      q = q.Filter(vv.name, Equal, field.Interface())
    }
  }
  return q.Run(dbConn)
}

// UpdateEntity updates the given entity in DB.
func UpdateEntity(dbConn dbconn.DBConn, src interface{}) error {
  if dbConn == nil {
    return fmt.Errorf("invalid dbConn")
  }

  v := reflect.ValueOf(src)
  if v.Kind() != reflect.Ptr || v.IsNil() || v.Elem().Kind() != reflect.Struct {
    return fmt.Errorf("invalid entity type")
  }
  v = v.Elem()
  codec, err := getStructCodec(v.Type())
  if err != nil {
    return err
  }

  if codec.pk == nil {
    return fmt.Errorf("datastore : primary key not defined for %v", v.Type())
  }

  q, err := NewUpdateQuery(v.Type())
  if err != nil {
    return err
  }
  for _, vv := range codec.byIndex {
    if vv.name == "-" {
      continue
    }
    field := v.Field(codec.byName[vv.name].index)
    if field.Kind() != reflect.Ptr {
      // all fields in our DB models are pointers, so we skip non-pointers for
      // now.
      continue
    }
    if field.IsNil() {
      // nil fields are supposed to be skipped.
      continue
    }
    if codec.pk.isPresent(vv.name) {
      // if field is part of PK, add it to filter clause
      q = q.Filter(vv.name, Equal, field.Interface())
      continue
    }
    fe := field.Elem()
    if fe.Kind() == reflect.Map {
      // TODO: support map update operations. current implementation
      // overwrites the map.
      q = q.Update(vv.name, fe.Interface())
    } else if fe.Kind() == reflect.Slice {
      // TODO: figure out a way to support Add update on list/set
      q = q.Update(vv.name, fe.Interface())
    } else {
      q = q.Update(vv.name, field.Interface())
    }
  }

  return q.Run(dbConn)
}

// SaveEntity saves a entity.
func SaveEntity(dbConn dbconn.DBConn, src interface{}) error {
  if dbConn == nil {
    return fmt.Errorf("invalid dbConn")
  }

  session := dbConn.GetSession()
  if session == nil {
    return fmt.Errorf("invalid session")
  }
  defer dbConn.ReleaseSession(session)

  q, err := newInsertQuery(src)
  if err != nil {
    return err
  }

  return q.run(dbConn)
}

// SaveEntityCAS saves a entity if not exists.
// NOTE: Batch with conditions cannot span multiple tables
func SaveEntityCAS(dbConn dbconn.DBConn, src interface{}) error {
  if dbConn == nil {
    return fmt.Errorf("invalid dbConn")
  }

  session := dbConn.GetSession()
  if session == nil {
    return fmt.Errorf("invalid session")
  }
  defer dbConn.ReleaseSession(session)

  q, err := newInsertQuery(src)
  if err != nil {
    return err
  }

  return q.ifNotExists().run(dbConn)
}

// SaveEntities saves a given entity instance in datastore, src must be a struct
// pointer of column family kind. This call always uses batching. It uses
// logged batch (atomic) if doLoggedBatch is true, else unlogged batch (non-atomic)
// is used.
func SaveEntities(dbConn dbconn.DBConn, doLoggedBatch bool, src ...interface{}) error {

  if dbConn == nil {
    return fmt.Errorf("invalid dbConn")
  }

  session := dbConn.GetSession()
  if session == nil {
    return fmt.Errorf("invalid session")
  }
  defer dbConn.ReleaseSession(session)

  qs := make([]*insertQuery, len(src))

  defer util.LogIfDurationAboveThreshold(0, time.Now(),
    fmt.Sprintf("slow cassandra SaveEntities query: %+v", qs), 5*time.Second)

  for ii := 0; ii < len(src); ii++ {
    q, err := newInsertQuery(src[ii])
    if err != nil {
      return err
    }
    qs[ii] = q
  }

  return save(session, doLoggedBatch, qs)
}

// DeleteAll deletes all the entries for a given column family type.
func DeleteAll(dbConn dbconn.DBConn, typ reflect.Type) error {
  defer util.LogExecutionTime(1, time.Now(), "datastore:entity:deleteall")
  if dbConn == nil {
    return fmt.Errorf("invalid dbConn")
  }

  session := dbConn.GetSession()
  if session == nil {
    return fmt.Errorf("invalid session")
  }
  defer dbConn.ReleaseSession(session)

  codec, err := getStructCodec(typ)
  if err != nil {
    return err
  }
  q := fmt.Sprintf("TRUNCATE %s", codec.columnFamily)
  if err := session.Query(q).Exec(); err != nil {
    return err
  }
  return nil
}
