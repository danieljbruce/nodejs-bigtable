import {Entry, MutateOptions} from '../table';
import {CallOptions, GoogleError, RetryOptions} from 'google-gax';
import {
  DEFAULT_BACKOFF_SETTINGS,
  RETRYABLE_STATUS_CODES,
} from './retry-options';
import {RequestType} from 'google-gax/build/src/apitypes';
import * as protos from '../../protos/protos';
import {Mutation} from '../mutation';

// This interface contains the information that will be used in a request.
interface TableStrategyInfo {
  tableName: string;
  appProfileId?: string;
}

export class MutateRowsResumptionStrategy {
  numRequestsMade = 0;
  pendingEntryIndices: Set<number>;
  private maxRetries: number;
  private entries: Entry[];
  private options: MutateOptions;
  private tableStrategyInfo: TableStrategyInfo;
  entryBatch: Entry[];

  constructor(
    entries: Entry[],
    maxRetries: number,
    tableStrategyInfo: TableStrategyInfo,
    options: MutateOptions
  ) {
    this.options = options;
    this.entries = entries;
    this.pendingEntryIndices = new Set(
      entries.map((entry: Entry, index: number) => index)
    );
    this.maxRetries = maxRetries;
    this.tableStrategyInfo = tableStrategyInfo;
    this.entryBatch = this.entries.filter((entry: Entry, index: number) => {
      return this.pendingEntryIndices.has(index);
    });
  }

  getResumeRequest(): protos.google.bigtable.v2.IMutateRowsRequest {
    this.entryBatch = this.entries.filter((entry: Entry, index: number) => {
      return this.pendingEntryIndices.has(index);
    });
    const entries = this.options.rawMutation
      ? this.entryBatch
      : this.entryBatch.map(Mutation.parse);
    const reqOpts = Object.assign({}, {entries}, this.tableStrategyInfo);
    return reqOpts;
  }

  canResume(error: GoogleError | null): boolean {
    if (this.numRequestsMade === 0) {
      return false;
    }
    if (
      this.pendingEntryIndices.size === 0 ||
      this.numRequestsMade >= this.maxRetries + 1
    ) {
      return false;
    }
    // If the error is empty but there are still outstanding mutations,
    // it means that there are retryable errors in the mutate response
    // even when the RPC succeeded
    if (error && error.code) {
      return RETRYABLE_STATUS_CODES.has(error.code);
    }
    return false;
  }

  toRetryOptions(gaxOpts: CallOptions): RetryOptions {
    // On individual calls, the user can override any of the default
    // retry options. Overrides can be done on the retryCodes, backoffSettings,
    // shouldRetryFn or getResumptionRequestFn.
    const canResume = (error: GoogleError) => {
      return this.canResume(error);
    };
    const getResumeRequest = () => {
      return this.getResumeRequest() as RequestType;
    };
    // In RetryOptions, the 1st parameter, the retryCodes are ignored if a
    // shouldRetryFn is provided.
    // The 3rd parameter, the shouldRetryFn will determine if the client should retry.
    return new RetryOptions(
      [],
      gaxOpts?.retry?.backoffSettings || DEFAULT_BACKOFF_SETTINGS,
      canResume,
      getResumeRequest
    );
  }
}
