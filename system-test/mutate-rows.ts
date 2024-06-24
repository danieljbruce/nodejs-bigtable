// Copyright 2016 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// TODO: Go through code remove any and look for the snake case and camel case mix.

import {Bigtable, Table} from '../src';
import {Test} from './testTypes';
const {tests} =
  require('../../system-test/data/mutate-rows-retry-test.json') as {
    tests: Test[];
  };

import * as assert from 'assert';
import {before, beforeEach, describe, it} from 'mocha';
import {Entry, PartialFailureError} from '../src/table';
import {MockServer} from '../src/util/mock-servers/mock-server';
import {MockService} from '../src/util/mock-servers/mock-service';
import {BigtableClientMockService} from '../src/util/mock-servers/service-implementations/bigtable-client-mock-service';
import * as protos from '../protos/protos';
import {ServerWritableStream} from '@grpc/grpc-js';
import {GoogleError, ServiceError} from 'google-gax';

function entryResponses(statusCodes: number[]) {
  return {
    entries: statusCodes.map((code, index) => ({
      index,
      status: {code},
    })),
  };
}

describe('Bigtable/Table', () => {
  describe.only('mutate with mock server', () => {
    const requests = [];
    let mutationBatchesInvoked: Array<{}>;
    let mutationCallTimes: number[];
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    let responses: any[] | null;
    let currentRetryAttempt: number;

    let server: MockServer;
    let service: MockService;
    let bigtable = new Bigtable();
    let table: Table;

    before(async () => {
      // make sure we have everything initialized before starting tests
      const port = await new Promise<string>(resolve => {
        server = new MockServer(resolve);
      });
      bigtable = new Bigtable({
        apiEndpoint: `localhost:${port}`,
      });
      table = bigtable.instance('fake-instance').table('fake-table');
      service = new BigtableClientMockService(server);
    });

    beforeEach(() => {
      service.setService({
        MutateRows: (
          stream: ServerWritableStream<
            protos.google.bigtable.v2.IMutateRowsRequest,
            protos.google.bigtable.v2.IMutateRowsResponse
          >
        ) => {
          requests.push(stream.request!);
          mutationBatchesInvoked.push(
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            stream.request!.entries!.map(entry =>
              (entry.rowKey as any).asciiSlice()
            )
          );
          mutationCallTimes.push(new Date().getTime());
          // Dispatches the response through the stream
          const response = responses!.shift();
          if (response.entry_codes) {
            stream.write(entryResponses(response.entry_codes));
          }
          if (response.end_with_error) {
            const error: GoogleError = new GoogleError();
            error.code = response.end_with_error;
            stream.emit('error', error);
          } else {
            stream.end();
          }
        },
      });
    });

    after(async () => {
      server.shutdown(() => {});
    });

    tests.forEach(test => {
      it(test.name, done => {
        currentRetryAttempt = 0;
        mutationBatchesInvoked = [];
        mutationCallTimes = [];
        responses = test.responses;
        table.maxRetries = test.max_retries;
        table.mutate(test.mutations_request, error => {
          assert.deepStrictEqual(
            mutationBatchesInvoked,
            test.mutation_batches_invoked
          );
          if (test.errors) {
            const expectedIndices = test.errors.map(error => {
              return error.index_in_mutations_request;
            });
            assert.deepStrictEqual(error!.name, 'PartialFailureError');
            const actualIndices = (error as PartialFailureError).errors!.map(
              error => {
                return test.mutations_request.indexOf(
                  (error as {entry: Entry}).entry
                );
              }
            );
            assert.deepStrictEqual(expectedIndices, actualIndices);
          } else {
            if (test.error) {
              // If the test is expecting an error.
              // Make sure that the error code matches the expected error code.
              assert.strictEqual((error as ServiceError)!.code, test.error);
            } else {
              // If the test is not expecting an error.
              // Fail the test with the error that mutate returns.
              assert.ifError(error);
            }
          }
          done();
        });
      });
    });
  });
});
