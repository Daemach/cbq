component accessors="true" extends="AbstractQueueProvider" {

	public any function push(
		required string queueName,
		required string payload,
		numeric delay = 0,
		numeric attempts = 0
	) {
		if ( isNull( variables.pool ) ) {
			if ( variables.log.canWarn() ) {
				variables.log.warn( "No workers have been defined so this job will not be executed." );
			}
			return;
		}

		marshalJob(
			deserializeJob(
				arguments.payload,
				createUUID(),
				arguments.attempts
			),
			variables.pool
		);
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
				variables.log.debug( "Marshaling job ###arguments.job.getId()#", arguments.job.getMemento() );
			}

			beforeJobRun( arguments.job );

			variables.interceptorService.announce( "onCBQJobMarshalled", { "job" : arguments.job } );

			if ( variables.log.canDebug() ) {
				variables.log.debug( "Running job ###arguments.job.getId()#", arguments.job.getMemento() );
			}

			var result = arguments.job.handle();

			if ( job.getIsReleased() ) {
				variables.log.debug( "Job [#job.getId()#] requested manual release." );

				if ( job.getCurrentAttempt() >= getMaxAttemptsForJob( job, pool ) ) {
					throw(
						type = "cbq.MaxAttemptsReached",
						message = "Job [#job.getId()#] requested manual release, but has reached its maximum attempts [#job.getCurrentAttempt()#]."
					);
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

			afterJobRun( job );

			var chain = job.getChained();
			if ( chain.isEmpty() ) {
				return;
			}

			var nextJobConfig = chain[ 1 ];
			var nextJob = variables.cbq.job( nextJobConfig.mapping );
			nextJob.applyMemento( nextJobConfig );

			if ( chain.len() >= 2 ) {
				nextJob.setChained( chain.slice( 2 ) );
			}

			nextJob.dispatch();
		} catch ( any e ) {
			// log failed job
			if ( log.canError() ) {
				log.error(
					"Exception when running job: #e.message#",
					{
						"job" : job.getMemento(),
						"exception" : e
					}
				);
			}

			variables.interceptorService.announce( "onCBQJobException", { "job" : job, "exception" : e } );

			if ( job.getCurrentAttempt() < getMaxAttemptsForJob( job, arguments.pool ) ) {
				variables.log.debug( "Releasing job ###job.getId()#" );
				releaseJob( job );
				variables.log.debug( "Released job ###job.getId()#" );
			} else {
				variables.log.debug( "Maximum attempts reached. Deleting job ###job.getId()#" );

				if ( structKeyExists( job, "onFailure" ) ) {
					invoke(
						job,
						"onFailure",
						{ "excpetion" : e }
					);
				}

				variables.interceptorService.announce( "onCBQJobFailed", { "job" : job, "exception" : e } );

				afterJobFailed( job.getId(), job );

				variables.log.debug( "Deleted job ###job.getId()# after maximum failed attempts." );

				rethrow;
			}
		}
	}

}
