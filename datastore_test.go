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
// +build cassandra
//
// This file implements test for datastore package. Tests need to be invoked
// with 'go test -tags 'cassandra' -v -db_host=<casandra_host>'.
//
// The protobuf also needs to be built first since it is used in testing.

package datastore

import (
  "encoding/json"
  "flag"
  "fmt"
  "math/rand"
  "reflect"
  "testing"
  "time"

  "github.com/gocql/gocql"
  "github.com/stretchr/testify/assert"
  "github.com/stretchr/testify/suite"

  "github.com/zerostackinc/dbconn"
)

var (
  typeOfTweet        = reflect.TypeOf(Tweet{})
  typeOfCompositeKey = reflect.TypeOf(CompositeKey{})
  dbHost             = flag.String("db_host", "127.0.0.1", "Cassandra DB server")
  keyspace           = flag.String("keyspace", "datastore_test", "Keyspace")
  random             = rand.NewSource(99)

  DefaultDBIOTimeout time.Duration = 10 * time.Second
)

type TweetStatus int

const (
  TweetActive TweetStatus = iota
  TweetInActive
)

type Tweet struct {
  ColumnFamily *string     `cql:"tweet,(id)"`
  ID           *int64      `cql:"id"`
  Timeline     *string     `cql:"timeline"`
  TextVal      *string     `cql:"text"`
  Tags         *[]string   `cql:"tags"`
  TopicList    *[]string   `cql:"topics"`
  Data         *[]byte     `cql:"data"`
  DataSlice    []byte      `cql:"data_slice"`
  Status       TweetStatus `cql:"status"`
}

type dbTestSuite struct {
  suite.Suite
  dbConn *dbconn.DBConnCassandra
}

// filterTestEntries number of entries in the columnfamily.
var filterTestEntries = 10

type filterTest struct {
  op            Operator
  filterValue   interface{} // must be <= filterTestEntries for count type
  expectedCount int
  result        bool
}

var filterTests = []filterTest{
  {LessThan, 5, 4 /*1..4*/, true},
  {LessEq, 5, 5 /*1..5*/, true},
  {Equal, 4, 1 /*4*/, true},
  {GreaterEq, 5, filterTestEntries - 5 + 1 /*5..10*/, true},
  {GreaterThan, 5, filterTestEntries - 5 /*6..10*/, true},
  {IN, []int{1, 2, 3, 4, 5}, 5 /*5*/, true},
  {IN, 1, -1 /*error*/, false},
}

func (suite *dbTestSuite) SetupSuite() {
  // destroy existing keyspace if any
  suite.destroyKeyspace(*keyspace)
  // initialize the keyspace
  suite.createKeyspace(*keyspace)

  // create db session. This session will be used in all the tests.
  dbconn := suite.createDBConn()
  assert.NotNil(suite.T(), dbconn)
  suite.dbConn = dbconn

  // Schema definition for Tweet columnfamily should be in sync with the
  // model definition in datastore_test.proto
  err := dbconn.GetSession().Query(`CREATE TABLE tweet(
    timeline text,
    id bigint,
    text text,
    tags set<text>,
    topics list<text>,
    status int,
    data blob,
    data_slice blob,
    PRIMARY KEY(id)
  );
  `).Consistency(gocql.All).Exec()
  assert.Nil(suite.T(), err)

  // Schema definition for compositekey columnfamily should be in sync with the
  // model definition in datastore_test.proto
  err = dbconn.GetSession().Query(`CREATE TABLE composite_key(
    composite_key1 text,
    composite_key2 text,
    key3 int,
    value text,
    PRIMARY KEY((composite_key1, composite_key2), key3)
  );
  `).Consistency(gocql.All).Exec()
  assert.Nil(suite.T(), err)

  // table need to be index'd on timeline column.
  err = dbconn.GetSession().Query("CREATE INDEX on tweet(timeline);").Exec()
  assert.Nil(suite.T(), err)
}

func (suite *dbTestSuite) TearDownSuite() {
  suite.destroyKeyspace(*keyspace)
}

func createCluster() *gocql.ClusterConfig {
  cluster := gocql.NewCluster(*dbHost)
  cluster.Timeout = 5 * time.Second
  cluster.Consistency = gocql.Quorum
  return cluster
}

func (suite *dbTestSuite) createKeyspace(keyspace string) {
  cluster := createCluster()
  session, err := cluster.CreateSession()
  defer session.Close()
  assert.Nil(suite.T(), err)

  err = session.Query(fmt.Sprintf(`CREATE KEYSPACE %s
  WITH replication = {
    'class' : 'SimpleStrategy',
    'replication_factor' : %d
  }`, keyspace, 1)).Consistency(gocql.Quorum).Exec()
  assert.Nil(suite.T(), err)
  session.Close()
}

func (suite *dbTestSuite) destroyKeyspace(keyspace string) error {
  cluster := createCluster()
  session, err := cluster.CreateSession()
  defer session.Close()
  assert.Nil(suite.T(), err)

  q := "DROP KEYSPACE " + keyspace
  return session.Query(q).Exec()
}

func (suite *dbTestSuite) createDBConn() *dbconn.DBConnCassandra {
  dbConfig, errDB := dbconn.NewDBConfig(*dbHost, *keyspace,
    "quorum", 1, 0, DefaultDBIOTimeout)
  assert.Nil(suite.T(), errDB)

  dbConn, err := dbconn.NewDBConnCassandra(dbConfig, nil)
  assert.Nil(suite.T(), err)
  assert.NotNil(suite.T(), dbConn)
  return dbConn
}

// SetupTest is invoked before every tests.
func (suite *dbTestSuite) SetupTest() {
  err := DeleteAll(suite.dbConn, typeOfTweet)
  assert.Nil(suite.T(), err)
}

// TestSaveEntity tests entity save method.
func (suite *dbTestSuite) TestSaveEntity() {
  T := suite.T()
  randomID := random.Int63()
  timeline := "me"
  textVal := fmt.Sprintf("Auto generated at %s", time.Now())
  inputData := map[string]string{
    "state": "CA",
    "zip":   "94085",
  }
  dataBytes, err := json.Marshal(inputData)
  assert.NoError(T, err)

  origTw := Tweet{
    Timeline:  &timeline,
    ID:        &randomID,
    TextVal:   &textVal,
    Tags:      &[]string{"goodone"},
    TopicList: &[]string{"t1"},
    Data:      &dataBytes,
    DataSlice: dataBytes,
    Status:    TweetActive,
  }
  err = SaveEntity(suite.dbConn, &origTw)
  assert.Nil(T, err)

  q, err := NewQuery(typeOfTweet)
  assert.Nil(T, err)

  var fetchTw Tweet
  q = q.Filter("id", Equal, origTw.ID)
  err = q.First(suite.dbConn, &fetchTw)
  assert.NoError(T, err)
  assert.Equal(T, &origTw, &fetchTw)

  outData := make(map[string]string)
  err = json.Unmarshal(*fetchTw.Data, &outData)
  assert.NoError(T, err)
  assert.Equal(T, outData, inputData)

  outDataSlice := make(map[string]string)
  err = json.Unmarshal(fetchTw.DataSlice, &outDataSlice)
  assert.NoError(T, err)
  assert.Equal(T, outDataSlice, inputData)
}

// TestSaveEntityCAS tests save entity using cas.
func (suite *dbTestSuite) TestSaveEntityCAS() {
  T := suite.T()
  randomID := random.Int63()
  timeline := "me"
  textVal := fmt.Sprintf("Auto generated at %s", time.Now())
  inputData := map[string]string{
    "state": "CA",
    "zip":   "94085",
  }
  dataBytes, err := json.Marshal(inputData)
  assert.NoError(T, err)

  origTw := Tweet{
    Timeline:  &timeline,
    ID:        &randomID,
    TextVal:   &textVal,
    Tags:      &[]string{"goodone"},
    TopicList: &[]string{"t1"},
    Data:      &dataBytes,
    DataSlice: dataBytes,
    Status:    TweetActive,
  }
  err = SaveEntity(suite.dbConn, &origTw)
  assert.Nil(T, err)

  time.Sleep(time.Second)
  err = SaveEntityCAS(suite.dbConn, &origTw)
  assert.IsType(T, &ErrNotApplied{}, err)

  q, err := NewQuery(typeOfTweet)
  assert.Nil(T, err)

  var fetchTw Tweet
  q = q.Filter("id", Equal, origTw.ID)
  err = q.First(suite.dbConn, &fetchTw)
  assert.NoError(T, err)
  assert.Equal(T, &origTw, &fetchTw)

  outData := make(map[string]string)
  err = json.Unmarshal(*fetchTw.Data, &outData)
  assert.NoError(T, err)
  assert.Equal(T, outData, inputData)

  outDataSlice := make(map[string]string)
  err = json.Unmarshal(fetchTw.DataSlice, &outDataSlice)
  assert.NoError(T, err)
  assert.Equal(T, outDataSlice, inputData)
}

// TestSaveEntityNilPtr tests entity save method with a few ptrs field as nil.
func (suite *dbTestSuite) TestSaveEntityNilPtr() {
  T := suite.T()
  randomID := random.Int63()
  timeline := "me"
  origTw := Tweet{
    Timeline: &timeline,
    ID:       &randomID,
    Status:   TweetActive,
  }
  err := SaveEntity(suite.dbConn, &origTw)
  assert.Nil(T, err)

  q, err := NewQuery(typeOfTweet)
  assert.Nil(T, err)

  var fetchTw Tweet
  q = q.Filter("id", Equal, origTw.ID)
  err = q.First(suite.dbConn, &fetchTw)
  assert.Nil(T, err)
  assert.Equal(T, &origTw, &fetchTw)
}

// TestUpdateEntity tests UpdateEntity function.
func (suite *dbTestSuite) TestUpdateEntity() {
  T := suite.T()
  randomID := int64(123456)
  timeline := "me"
  textVal := fmt.Sprintf("Auto generated at %s", time.Now())
  origTw := Tweet{
    Timeline:  &timeline,
    ID:        &randomID,
    TextVal:   &textVal,
    Tags:      &[]string{"a", "c"},
    TopicList: &[]string{"t1_abc", "t2_abc"},
    Status:    TweetActive,
  }
  err := SaveEntity(suite.dbConn, &origTw)
  assert.Nil(T, err)

  q, err := NewQuery(typeOfTweet)
  assert.Nil(T, err)

  var fetchTw Tweet
  q = q.Filter("id", Equal, origTw.ID)
  err = q.First(suite.dbConn, &fetchTw)
  assert.Nil(T, err)
  assert.Equal(T, &origTw, &fetchTw)

  updatedTxt := "updated text"

  err = UpdateEntity(suite.dbConn, &Tweet{
    ID:        origTw.ID,
    TextVal:   &updatedTxt,
    Tags:      &[]string{"b"},
    TopicList: &[]string{"t1", "t2"},
  })
  assert.NoError(T, err)

  var updatedTw Tweet
  err = q.First(suite.dbConn, &updatedTw)
  assert.Nil(T, err)
  assert.Equal(T, *updatedTw.TextVal, updatedTxt)
  assert.Equal(T, *updatedTw.Tags, []string{"b"})
  assert.Equal(T, *updatedTw.TopicList, []string{"t1", "t2"})
}

// TestUpdateQuery tests update query.
func (suite *dbTestSuite) TestUpdateQuery() {
  T := suite.T()
  randomID := random.Int63()
  timeline := "me"
  textVal := fmt.Sprintf("Auto generated at %s", time.Now())

  origTw := Tweet{
    Timeline:  &timeline,
    ID:        &randomID,
    TextVal:   &textVal,
    Tags:      &[]string{"a", "c", "d"},
    TopicList: &[]string{"t1", "t2"},
    Status:    TweetActive,
  }
  err := SaveEntity(suite.dbConn, &origTw)
  assert.Nil(T, err)

  q, err := NewQuery(typeOfTweet)
  assert.Nil(T, err)

  var fetchTw Tweet
  q = q.Filter("id", Equal, origTw.ID)
  err = q.First(suite.dbConn, &fetchTw)
  assert.Nil(T, err)
  assert.Equal(T, &origTw, &fetchTw)

  updatedTxt := "updated text"
  inputData := map[string]string{
    "state": "CA",
    "zip":   "94085",
  }
  dataBytes, err := json.Marshal(inputData)
  assert.NoError(T, err)

  updateQ, err := NewUpdateQuery(typeOfTweet)
  assert.Nil(T, err)
  updateQ = updateQ.Filter("id", Equal, origTw.ID).
    Update("text", updatedTxt).
    Add("tags", []string{"b"}).
    Add("tags", []string{"b"}). // test for duplicate insertion on a set field
    Remove("tags", []string{"d"}).
    Remove("topics", []string{"t1"}).
    Add("topics", []string{"t2"}).
    Update("data_slice", dataBytes).
    Update("data", &dataBytes)

  err = updateQ.Run(suite.dbConn)
  assert.NoError(T, err)

  var updatedTw Tweet
  err = q.First(suite.dbConn, &updatedTw)
  assert.Nil(T, err)
  assert.Equal(T, *updatedTw.TextVal, updatedTxt)
  assert.Equal(T, *updatedTw.Tags, []string{"a", "b", "c"})
  assert.Equal(T, *updatedTw.TopicList, []string{"t2", "t2"})

  outData := make(map[string]string)
  err = json.Unmarshal(*updatedTw.Data, &outData)
  assert.NoError(T, err)
  assert.Equal(T, outData, inputData)

  outDataSlice := make(map[string]string)
  err = json.Unmarshal(updatedTw.DataSlice, &outDataSlice)
  assert.NoError(T, err)
  assert.Equal(T, outDataSlice, inputData)
}

// TestInsertQuery tests insert query.
func (suite *dbTestSuite) TestInsertQuery() {
  T := suite.T()
  randomID := random.Int63()
  timeline := "me"
  textVal := fmt.Sprintf("Auto generated at %s", time.Now())

  // (1) Insert a tweet
  origTw := Tweet{
    Timeline:  &timeline,
    ID:        &randomID,
    TextVal:   &textVal,
    Tags:      &[]string{"a", "c", "d"},
    TopicList: &[]string{"t1", "t2"},
    Status:    TweetActive,
  }

  insertQ, err := newInsertQuery(&origTw)
  assert.Nil(T, err)

  err = insertQ.run(suite.dbConn)
  assert.Nil(T, err)

  q, err := NewQuery(typeOfTweet)
  assert.Nil(T, err)
  var fetchTw Tweet
  q = q.Filter("id", Equal, randomID)
  err = q.First(suite.dbConn, &fetchTw)
  assert.Nil(T, err)
  assert.Equal(T, &origTw, &fetchTw)
}

// TestInsertCASQuery tests CAS functionality of insert query.
func (suite *dbTestSuite) TestInsertCASQuery() {
  T := suite.T()
  randomID := random.Int63()
  timeline := "me"
  textVal := fmt.Sprintf("Auto generated at %s", time.Now())

  // (1) Insert a tweet
  origTw := Tweet{
    Timeline:  &timeline,
    ID:        &randomID,
    TextVal:   &textVal,
    Tags:      &[]string{"a", "c", "d"},
    TopicList: &[]string{"t1", "t2"},
    Status:    TweetActive,
  }

  insertQ, err := newInsertQuery(&origTw)
  assert.Nil(T, err)

  err = insertQ.run(suite.dbConn)
  assert.Nil(T, err)

  q, err := NewQuery(typeOfTweet)
  assert.Nil(T, err)
  var fetchTw Tweet
  q = q.Filter("id", Equal, randomID)
  err = q.First(suite.dbConn, &fetchTw)
  assert.Nil(T, err)
  assert.Equal(T, &origTw, &fetchTw)

  // (2) Insert tweet with same key with new values if not exists.
  textValR := fmt.Sprintf("Auto generated at %s", time.Now())
  origTwReinsert := Tweet{
    Timeline:  &timeline,
    ID:        &randomID,
    TextVal:   &textValR,
    Tags:      &[]string{"x", "y", "z"},
    TopicList: &[]string{"t1", "t2"},
    Status:    TweetActive,
  }

  insertQR, err := newInsertQuery(&origTwReinsert)
  assert.Nil(T, err)
  insertQR = insertQR.ifNotExists()

  // (3) Verify CAS fails and old values are correctly returned.
  err = insertQR.run(suite.dbConn)
  assert.IsType(T, err, &ErrNotApplied{})
  valsMap := err.(*ErrNotApplied).PreviousVals()
  origTags := valsMap["text"]
  assert.Equal(T, origTags, textVal)

  // (4) Verify old values are returned on fetch query.
  q, err = NewQuery(typeOfTweet)
  assert.Nil(T, err)
  q = q.Filter("id", Equal, randomID)
  err = q.First(suite.dbConn, &fetchTw)
  assert.Nil(T, err)
  assert.Equal(T, &origTw, &fetchTw)
}

// TestUpdateAllQuery tests
func (suite *dbTestSuite) TestUpdateAllQuery() {
  T := suite.T()
  randomID := random.Int63()
  timeline := "me"
  textVal := fmt.Sprintf("Auto generated at %s", time.Now())

  // (1) Insert a tweet
  origTw := Tweet{
    Timeline:  &timeline,
    ID:        &randomID,
    TextVal:   &textVal,
    Tags:      &[]string{"a", "c", "d"},
    TopicList: &[]string{"t1", "t2"},
    Status:    TweetActive,
  }
  err := SaveEntity(suite.dbConn, &origTw)
  assert.Nil(T, err)

  // (2) Update the tweet with UpdataAll query.
  updatedTimeline := "me_new"
  updatedTextVal := fmt.Sprintf("Auto generated at %s", time.Now())
  updatedTags := []string{"x", "y", "z"}
  updatedTw := Tweet{
    Timeline:  &updatedTimeline,
    ID:        &randomID,
    TextVal:   &updatedTextVal,
    Tags:      &updatedTags,
    TopicList: &[]string{"t1", "t2"},
    Status:    TweetActive,
  }

  updateQ, err := NewUpdateAllQuery(&updatedTw)
  assert.Nil(T, err)
  err = updateQ.Run(suite.dbConn)
  assert.NoError(T, err)

  // (3) Verify fields are updated as expected.
  q, err := NewQuery(typeOfTweet)
  assert.Nil(T, err)
  var fetchTw Tweet
  q = q.Filter("id", Equal, updatedTw.ID)
  err = q.First(suite.dbConn, &fetchTw)
  assert.Nil(T, err)
  assert.Equal(T, &updatedTw, &fetchTw)

  // (4) Run CAS with invalid predicate.
  updateQI := updateQ.If("text", Equal, &textVal)
  err = updateQI.Run(suite.dbConn)
  assert.IsType(T, err, &ErrNotApplied{})

  // (4) Run CAS with valid predicate.
  updateQ = updateQ.If("text", Equal, &updatedTextVal)
  err = updateQ.Run(suite.dbConn)
  assert.Nil(T, err)
}

// TestCASQuery tests update query with lightweight transaction.
func (suite *dbTestSuite) TestCASQuery() {
  T := suite.T()
  randomID := random.Int63()
  timeline := "me"
  textVal := fmt.Sprintf("Auto generated at %s", time.Now())

  origTw := Tweet{
    Timeline:  &timeline,
    ID:        &randomID,
    TextVal:   &textVal,
    Tags:      &[]string{"a", "c", "d"},
    TopicList: &[]string{"t1", "t2"},
    Status:    TweetActive,
  }
  err := SaveEntity(suite.dbConn, &origTw)
  assert.Nil(T, err)

  q, err := NewQuery(typeOfTweet)
  assert.Nil(T, err)

  var fetchTw Tweet
  q = q.Filter("id", Equal, origTw.ID)
  err = q.First(suite.dbConn, &fetchTw)
  assert.Nil(T, err)
  assert.Equal(T, &origTw, &fetchTw)

  updatedTxt := "updated text"
  inputData := map[string]string{
    "state": "CA",
    "zip":   "94085",
  }
  dataBytes, err := json.Marshal(inputData)
  assert.NoError(T, err)

  updateQf, err := NewUpdateQuery(typeOfTweet)
  assert.Nil(T, err)
  updateQf = updateQf.Filter("id", Equal, origTw.ID).
    Update("text", updatedTxt).
    Add("tags", []string{"b"}).
    Add("tags", []string{"b"}). // test for duplicate insertion on a set field
    Remove("tags", []string{"d"}).
    Remove("topics", []string{"t1"}).
    Add("topics", []string{"t2"}).
    Update("data_slice", dataBytes).
    Update("data", &dataBytes).
    If("text", Equal, "foobar") // will fail as "foobar" != textVal

  err = updateQf.Run(suite.dbConn)
  assert.IsType(T, err, &ErrNotApplied{})
  pVals := err.(*ErrNotApplied).PreviousVals()
  assert.NotNil(T, pVals)
  assert.Equal(T, len(pVals), 1)
  pTextInf := pVals["text"]
  pTextVal := pTextInf.(*string)
  assert.NotNil(T, pTextVal)
  assert.Equal(T, *pTextVal, textVal)

  updateQ, err := NewUpdateQuery(typeOfTweet)
  assert.Nil(T, err)
  updateQ = updateQ.Filter("id", Equal, origTw.ID).
    Update("text", updatedTxt).
    Add("tags", []string{"b"}).
    Add("tags", []string{"b"}). // test for duplicate insertion on a set field
    Remove("tags", []string{"d"}).
    Remove("topics", []string{"t1"}).
    Add("topics", []string{"t2"}).
    Update("data_slice", dataBytes).
    Update("data", &dataBytes).
    If("text", Equal, &textVal)

  err = updateQ.Run(suite.dbConn)
  assert.NoError(T, err)

  var updatedTw Tweet
  err = q.First(suite.dbConn, &updatedTw)
  assert.Equal(T, *updatedTw.TextVal, updatedTxt)
  assert.Equal(T, *updatedTw.Tags, []string{"a", "b", "c"})
  assert.Equal(T, *updatedTw.TopicList, []string{"t2", "t2"})

  outData := make(map[string]string)
  err = json.Unmarshal(*updatedTw.Data, &outData)
  assert.NoError(T, err)
  assert.Equal(T, outData, inputData)

  outDataSlice := make(map[string]string)
  err = json.Unmarshal(updatedTw.DataSlice, &outDataSlice)
  assert.NoError(T, err)
  assert.Equal(T, outDataSlice, inputData)

}

// TestDeleteQuery tests delete query.
func (suite *dbTestSuite) TestDeleteQuery() {
  T := suite.T()
  randomID := random.Int63()
  timeline := "me"
  textVal := fmt.Sprintf("Auto generated at %s", time.Now())
  origTw := Tweet{
    Timeline: &timeline,
    ID:       &randomID,
    TextVal:  &textVal,
    Status:   TweetActive,
  }
  err := SaveEntity(suite.dbConn, &origTw)
  assert.Nil(T, err)

  deleteQ, err := NewDeleteQuery(typeOfTweet)
  assert.Nil(T, err)
  deleteQ = deleteQ.Filter("id", Equal, origTw.ID).AddColumnsToDelete()

  err = deleteQ.Run(suite.dbConn)
  assert.Nil(T, err)

  q, err := NewQuery(typeOfTweet)
  assert.Nil(T, err)

  var fetchTw Tweet
  q = q.Filter("id", Equal, origTw.ID)
  err = q.First(suite.dbConn, &fetchTw)
  assert.Equal(T, err, gocql.ErrNotFound)
}

// TestDeleteEntity tests delete query.
func (suite *dbTestSuite) TestDeleteEntity() {
  T := suite.T()
  randomID := random.Int63()
  timeline := "me"
  textVal := fmt.Sprintf("Auto generated at %s", time.Now())
  origTw := Tweet{
    Timeline: &timeline,
    ID:       &randomID,
    TextVal:  &textVal,
    Status:   TweetActive,
  }
  err := SaveEntity(suite.dbConn, &origTw)
  assert.Nil(T, err)

  delEntity := &Tweet{
    ID: origTw.ID,
  }
  err = DeleteEntity(suite.dbConn, delEntity)
  assert.Nil(T, err)

  q, err := NewQuery(typeOfTweet)
  assert.Nil(T, err)

  var fetchTw Tweet
  q = q.Filter("id", Equal, origTw.ID)
  err = q.First(suite.dbConn, &fetchTw)
  assert.Equal(T, err, gocql.ErrNotFound)
}

// TestColumnDelete tests delete query.
func (suite *dbTestSuite) TestColumnDelete() {
  T := suite.T()
  randomID := random.Int63()
  timeline := "me"
  textVal := fmt.Sprintf("Auto generated at %s", time.Now())
  origTw := Tweet{
    Timeline: &timeline,
    ID:       &randomID,
    TextVal:  &textVal,
    Status:   TweetActive,
  }
  err := SaveEntity(suite.dbConn, &origTw)
  assert.Nil(T, err)

  deleteQ, err := NewDeleteQuery(typeOfTweet)
  assert.Nil(T, err)
  deleteQ = deleteQ.Filter("id", Equal, origTw.ID).AddColumnsToDelete("text")

  err = deleteQ.Run(suite.dbConn)
  assert.Nil(T, err)

  q, err := NewQuery(typeOfTweet)
  assert.Nil(T, err)

  var fetchTw Tweet
  q = q.Filter("id", Equal, origTw.ID)
  err = q.First(suite.dbConn, &fetchTw)
  assert.Nil(T, err)
  assert.Empty(T, *fetchTw.TextVal)
}

// TestIterator perform a simple test where we insert 10 entries and fetch
// them using iterator and compare if the count matches.
func (suite *dbTestSuite) TestIterator() {
  T := suite.T()
  timeline := "iterator_test"
  nrEntries := 10
  randomID := random.Int63()
  textVal := fmt.Sprintf("Auto generated at %s", time.Now())
  tw := Tweet{
    Timeline: &timeline,
    ID:       &randomID,
    TextVal:  &textVal,
    Status:   TweetInActive,
  }
  for i := 0; i < nrEntries; i++ {
    randomID = random.Int63()
    tw.Timeline = &timeline
    tw.ID = &randomID
    err := SaveEntity(suite.dbConn, &tw)
    assert.Nil(T, err)
  }

  q, err := NewQuery(typeOfTweet)
  q = q.Filter("timeline", Equal, timeline)
  iter := q.Run(suite.dbConn)
  defer iter.Close()

  var fetchTw Tweet

  fetchedCount := 0
  for err = iter.Next(&fetchTw); err != ErrDone; err = iter.Next(&fetchTw) {
    fetchedCount++
    assert.Nil(T, err)
  }
  assert.Equal(T, fetchedCount, nrEntries)
}

// TestFirstNotFound tests if First() returns NotFound for non-existing entity.
func (suite *dbTestSuite) TestFirstNotFound() {
  var tweet Tweet
  q, err := NewQuery(typeOfTweet)
  assert.Nil(suite.T(), err)

  q = q.Filter("id", Equal, random.Int63())
  err = q.First(suite.dbConn, &tweet)
  assert.Equal(suite.T(), err, gocql.ErrNotFound)
}

func (suite *dbTestSuite) testFilter(key1, value1, key2,
  value2, key3 string, t filterTest) error {

  T := suite.T()
  q, err := NewQuery(typeOfCompositeKey)
  if err != nil {
    return err
  }
  q = q.Filter(key1, Equal, value1).
    Filter(key2, Equal, value2).
    Filter(key3, t.op, t.filterValue)
  iter := q.Run(suite.dbConn)
  if !t.result {
    if iter.err == nil {
      return fmt.Errorf("expected iter err, but got no error")
    }
    return nil
  }
  defer iter.Close()

  fetchedCount := 0
  var fetchedCompositeKey CompositeKey
  for err = iter.Next(&fetchedCompositeKey); err != ErrDone; err =
    iter.Next(&fetchedCompositeKey) {
    fetchedCount++
    assert.Nil(T, err)
  }
  if fetchedCount != t.expectedCount {
    return fmt.Errorf("Expected %d got %d for operator %s",
      t.expectedCount, fetchedCount, t.op)
  }
  return nil
}

func (suite *dbTestSuite) TestFilters() {
  T := suite.T()
  compositeKey1 := "composite_key1"
  compositeKey2 := "composite_key2"
  value := "value"

  // Keyspace 1..compositeKeyEntries
  for ii := 1; ii <= filterTestEntries; ii++ {
    key := int64(ii)
    ck := CompositeKey{
      CompositeKey1: &compositeKey1,
      CompositeKey2: &compositeKey2,
      Key3:          &key,
      Value:         &value,
    }
    err := SaveEntity(suite.dbConn, &ck)
    assert.Nil(T, err)
  }

  // Test all operator by consulting the test structures.
  for _, t := range filterTests {
    err := suite.testFilter("composite_key1", compositeKey1,
      "composite_key2", compositeKey2, "key3", t)
    assert.Nil(T, err)
  }
}

func TestDataStore(t *testing.T) {
  tester := new(dbTestSuite)
  suite.Run(t, tester)
}
