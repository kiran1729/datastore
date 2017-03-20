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
  "testing"

  "github.com/stretchr/testify/assert"
)

func TestParsePrimaryKey(t *testing.T) {

  tt := []struct {
    pkStr  string
    expErr bool
    expPK  *primaryKey
  }{
    {
      "(a)",
      false,
      &primaryKey{
        newKey("a"),
        newKey(""),
      },
    },
    {
      "(a,b)",
      false,
      &primaryKey{
        newKey("a"),
        newKey("b"),
      },
    },
    {
      "((m,n),a,b)",
      false,
      &primaryKey{
        newKey("m, n"),
        newKey("a, b"),
      },
    },
    {
      "(a,b)",
      false,
      &primaryKey{
        newKey("a"),
        newKey("b"),
      },
    },
    {
      "(a,b",
      true, // error case
      &primaryKey{
        newKey("a"),
        newKey("b"),
      },
    },
  }

  for _, test := range tt {
    gotPK, err := parsePrimaryKey(test.pkStr)
    if test.expErr {
      assert.Error(t, err)
    } else {
      assert.NoError(t, err)
      assert.Equal(t, gotPK, test.expPK)
    }
  }

}
