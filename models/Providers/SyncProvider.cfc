component accessors="true" extends="AbstractQueueProvider" {

	public any function push(
		required string queueName,
		required AbstractJob job,
		numeric delay = 0,
		numeric attempts = 0
	) {
		if ( isNull( variables.pool ) ) {
			if ( variables.log.canWarn() ) {
				variables.log.warn( "No worker pools have been defined so this job will not be executed." );
			}
			return;
		}

		arguments.job.setId( createUUID() );
		if ( !isNull( arguments.attempts ) ) {
			arguments.job.setCurrentAttempt( arguments.attempts );
		}

		var chain = arguments.job.getChained();
		var firstJobPayload = arguments.job.getMemento();
		firstJobPayload.chained = [];
		arrayPrepend( chain, firstJobPayload );
		chain = chain.map( ( payload, i, arr ) => {
			payload.chained = arr.len() >= i + 1 ? arr.slice( i + 1 ) : [];
			payload.chained = payload.chained.map( ( p ) => {
				p.chained = [];
				return p;
			} );
			return deserializeJob(
				serializeJSON( payload ),
				createUUID(),
				1
			);
		} );

		for ( var i = 1; i <= chain.len(); ) {
			var nextJob = chain[ i ];
			try {
				marshalJob( nextJob, variables.pool );
				i++;
			} catch ( cbq.SyncProviderJobFailed e ) {
				nextJob.setCurrentAttempt( nextJob.getCurrentAttempt() + 1 );
				sleep( getBackoffForJob( nextJob, variables.pool ) * 1000 );
			}
		}
		return this;
	}

	public function function startWorker( required WorkerPool pool ) {
		variables.pool = arguments.pool;
		return function() {
		};
	}

	public any function listen( required WorkerPool pool ) {
		return this;
	}

	public void function marshalJob( required AbstractJob job, required WorkerPool pool ) {
		try {
			if ( variables.log.canDebug() ) {
				// variables.log.debug( "Marshaling job ###arguments.job.getId()#", arguments.job.getMemento() );
			}

			beforeJobRun( arguments.job );
			if ( structKeyExists( job, "before" ) ) {
				job.before();
			}

			variables.interceptorService.announce( "onCBQJobMarshalled", { "job" : arguments.job } );

			if ( variables.log.canDebug() ) {
				variables.log.debug( "Running job ###arguments.job.getId()#", arguments.job.getMemento() );
			}

			var result = arguments.job.handle();

			if ( job.getIsReleased() ) {
				variables.log.debug( "Job [#job.getId()#] requested manual release." );

				var jobMaxAttempts = getMaxAttemptsForJob( job, arguments.pool );
				if ( jobMaxAttempts != 0 && job.getCurrentAttempt() >= getMaxAttemptsForJob( job, pool ) ) {
					throw(
						type = "cbq.MaxAttemptsReached",
						message = "Job [#job.getId()#] requested manual release, but has reached its maximum attempts [#job.getCurrentAttempt()#]."
					);
				}

				if ( jobMaxAttempts == 0 ) {
					variables.log.debug( "Job ###job.getId()# has a maxAttempts of 0 and will always be released." );
				}

				variables.log.debug( "Releasing job ###job.getId()#" );
				releaseJob( job, pool );
				variables.log.debug( "Released job ###job.getId()#" );
				return;
			}

			if ( variables.log.canDebug() ) {
				variables.log.debug( "Job ###job.getId()# completed successfully." );
			}

			variables.interceptorService.announce(
				"onCBQJobComplete",
				{
					"job" : job,
					"result" : isNull( result ) ? javacast( "null", "" ) : result
				}
			);

			if ( structKeyExists( job, "after" ) ) {
				job.after();
			}

			var stillOwnsJob = true;
			try {
				var ownershipResult = afterJobRun( job, pool );
				stillOwnsJob = isNull( ownershipResult ) ? true : ownershipResult;
			} catch ( any afterJobRunException ) {
				// The job itself already succeeded. A failing completion write must not
				// fall through to the failure path below — that would release or fail
				// (and re-run) work that completed. Ownership is unproven, so skip the
				// success side effects and record the problem loudly.
				logSideEffectFailure(
					"afterJobRun",
					job,
					afterJobRunException
				);
				stillOwnsJob = false;
			}

			if ( stillOwnsJob ) {
				try {
					ensureSuccessfulBatchJobIsRecorded( job, pool );
				} catch ( any sideEffectException ) {
					logSideEffectFailure(
						"ensureSuccessfulBatchJobIsRecorded",
						job,
						sideEffectException
					);
				}
			} else if ( log.canWarn() ) {
				log.warn(
					"Job ###job.getId()# completed, but this worker no longer owns it (or ownership could not be confirmed). Skipping batch recording — the owning worker is responsible for it."
				);
			}
		} catch ( any e ) {
			// log failed job
			if ( log.canError() ) {
				log.error( "Exception when running job: #e.message#" );
			}

			variables.interceptorService.announce( "onCBQJobException", { "job" : job, "exception" : e } );

			var jobMaxAttempts = getMaxAttemptsForJob( job, arguments.pool );
			if ( jobMaxAttempts == 0 || job.getCurrentAttempt() < jobMaxAttempts ) {
				if ( jobMaxAttempts == 0 ) {
					variables.log.debug( "Job ###job.getId()# has a maxAttempts of 0 and will always be released." );
				}
				variables.log.debug( "Releasing job ###job.getId()#" );
				releaseJob( job, pool );
				variables.log.debug( "Released job ###job.getId()#" );
			} else {
				variables.log.debug( "Maximum attempts reached. Deleting job ###job.getId()#" );

				if ( structKeyExists( job, "onFailure" ) ) {
					invoke(
						job,
						"onFailure",
						{ "exception" : e }
					);
				}

				variables.interceptorService.announce( "onCBQJobFailed", { "job" : job, "exception" : e } );

				var stillOwnsFailedJob = true;
				try {
					var failedOwnershipResult = afterJobFailed( job.getId(), job );
					stillOwnsFailedJob = isNull( failedOwnershipResult ) ? true : failedOwnershipResult;
				} catch ( any afterJobFailedException ) {
					logSideEffectFailure(
						"afterJobFailed",
						job,
						afterJobFailedException
					);
					stillOwnsFailedJob = false;
				}

				if ( stillOwnsFailedJob ) {
					ensureFailedBatchJobIsRecorded( job, e );
				} else if ( log.canWarn() ) {
					log.warn(
						"Job ###job.getId()# failed, but this worker no longer owns it (or ownership could not be confirmed). Skipping batch failure recording — the owning worker is responsible for it."
					);
				}

				variables.log.debug( "Deleted job ###job.getId()# after maximum failed attempts." );

				rethrow;
			}
		}
	}

	public void function releaseJob( required AbstractJob job, required WorkerPool pool ) {
		throw(
			type = "cbq.SyncProviderJobFailed",
			message = "Job failed on attempt #arguments.job.getCurrentAttempt()# and was released."
		);
	}

}
