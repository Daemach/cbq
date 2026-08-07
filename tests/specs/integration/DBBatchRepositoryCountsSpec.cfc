component extends="tests.resources.ModuleIntegrationSpec" appMapping="/app" {

	function run() {
		describe( "DBBatchRepository counts", function() {
			it( "initializes successfulJobs for newly stored batches", function() {
				var repository = getWireBox().getInstance( "DBBatchRepository@cbq" );
				var batch = repository.store(
					getWireBox()
						.getInstance( "@cbq" )
						.batch( [] )
						.allowFailures()
				);

				expect( batch.getSuccessfulJobs() ).toBe( 0 );
			} );

			it( "successful jobs increment successfulJobs and decrement pendingJobs", function() {
				var repository = getWireBox().getInstance( "DBBatchRepository@cbq" );
				var config = registerSyncConnectionAndWorkerPool();
				var batch = createTrackedBatch( repository, 1 );
				var provider = config.getConnection( "syncBatchCounts" ).getProvider();
				var pool = config.getWorkerPool( "syncBatchCounts" );

				var job = getWireBox()
					.getInstance( "@cbq" )
					.job( "SendWelcomeEmailJob" )
					.setId( createUUID() )
					.withBatchId( batch.getId() );

				provider.marshalJob( job, pool );

				var updatedBatch = repository.find( batch.getId() );

				expect( updatedBatch.getPendingJobs() ).toBe( 0 );
				expect( updatedBatch.getFailedJobs() ).toBe( 0 );
				expect( updatedBatch.getSuccessfulJobs() ).toBe( 1 );
			} );

			it( "retryable errors do not change pending, successful, or failed counts", function() {
				var repository = getWireBox().getInstance( "DBBatchRepository@cbq" );
				var config = registerSyncConnectionAndWorkerPool();
				var batch = createTrackedBatch( repository, 1 );
				var provider = config.getConnection( "syncBatchCounts" ).getProvider();
				var pool = config.getWorkerPool( "syncBatchCounts" );

				var job = getWireBox()
					.getInstance( "@cbq" )
					.job( "AlwaysErrorJob" )
					.setId( createUUID() )
					.withBatchId( batch.getId() )
					.setCurrentAttempt( 1 )
					.setMaxAttempts( 2 );

				expect( () => provider.marshalJob( job, pool ) ).toThrow( "cbq.SyncProviderJobFailed" );

				var updatedBatch = repository.find( batch.getId() );

				expect( updatedBatch.getPendingJobs() ).toBe( 1 );
				expect( updatedBatch.getSuccessfulJobs() ).toBe( 0 );
				expect( updatedBatch.getFailedJobs() ).toBe( 0 );
				expect( updatedBatch.getFailedJobIds() ).toBeEmpty();
			} );

			it( "failed jobs increment failedJobs, append failedJobIds, and decrement pendingJobs", function() {
				var repository = getWireBox().getInstance( "DBBatchRepository@cbq" );
				var config = registerSyncConnectionAndWorkerPool();
				var batch = createTrackedBatch( repository, 1 );
				var provider = config.getConnection( "syncBatchCounts" ).getProvider();
				var pool = config.getWorkerPool( "syncBatchCounts" );
				var failedJobId = createUUID();

				var job = getWireBox()
					.getInstance( "@cbq" )
					.job( "AlwaysErrorJob" )
					.setId( failedJobId )
					.withBatchId( batch.getId() )
					.setCurrentAttempt( 1 )
					.setMaxAttempts( 1 );

				expect( () => provider.marshalJob( job, pool ) ).toThrow();

				var updatedBatch = repository.find( batch.getId() );

				expect( updatedBatch.getPendingJobs() ).toBe( 0 );
				expect( updatedBatch.getSuccessfulJobs() ).toBe( 0 );
				expect( updatedBatch.getFailedJobs() ).toBe( 1 );
				expect( updatedBatch.getFailedJobIds() ).toHaveLength( 1 );
				expect( updatedBatch.getFailedJobIds()[ 1 ] ).toBe( failedJobId );
			} );

			it( "recording the same successful job twice only moves the counters once", function() {
				var repository = getWireBox().getInstance( "DBBatchRepository@cbq" );
				var batch = createTrackedBatch( repository, 2 );
				var jobId = createUUID();

				var firstCounts = repository.decrementPendingJobs( batch.getId(), jobId );
				var secondCounts = repository.decrementPendingJobs( batch.getId(), jobId );

				expect( firstCounts.alreadyRecorded ).toBeFalse();
				expect( secondCounts.alreadyRecorded ).toBeTrue();
				expect( secondCounts.pendingJobs ).toBe( 1 );

				var updatedBatch = repository.find( batch.getId() );
				expect( updatedBatch.getPendingJobs() ).toBe( 1 );
				expect( updatedBatch.getSuccessfulJobs() ).toBe( 1 );
			} );

			it( "recording the same failed job twice only moves the counters once", function() {
				var repository = getWireBox().getInstance( "DBBatchRepository@cbq" );
				var batch = createTrackedBatch( repository, 2 );
				var jobId = createUUID();

				var firstCounts = repository.incrementFailedJobs( batch.getId(), jobId );
				var secondCounts = repository.incrementFailedJobs( batch.getId(), jobId );

				expect( firstCounts.alreadyRecorded ).toBeFalse();
				expect( secondCounts.alreadyRecorded ).toBeTrue();

				var updatedBatch = repository.find( batch.getId() );
				expect( updatedBatch.getPendingJobs() ).toBe( 1 );
				expect( updatedBatch.getFailedJobs() ).toBe( 1 );
				expect( updatedBatch.getFailedJobIds() ).toHaveLength( 1 );
			} );

			it( "a job recorded as successful cannot later be recorded as failed", function() {
				var repository = getWireBox().getInstance( "DBBatchRepository@cbq" );
				var batch = createTrackedBatch( repository, 2 );
				var jobId = createUUID();

				repository.decrementPendingJobs( batch.getId(), jobId );
				var failedCounts = repository.incrementFailedJobs( batch.getId(), jobId );

				expect( failedCounts.alreadyRecorded ).toBeTrue();

				var updatedBatch = repository.find( batch.getId() );
				expect( updatedBatch.getPendingJobs() ).toBe( 1 );
				expect( updatedBatch.getSuccessfulJobs() ).toBe( 1 );
				expect( updatedBatch.getFailedJobs() ).toBe( 0 );
				expect( updatedBatch.getFailedJobIds() ).toBeEmpty();
			} );

			it( "a duplicate delivery of the same job is only counted once, end to end", function() {
				var repository = getWireBox().getInstance( "DBBatchRepository@cbq" );
				var config = registerSyncConnectionAndWorkerPool();
				var batch = createTrackedBatch( repository, 2 );
				var provider = config.getConnection( "syncBatchCounts" ).getProvider();
				var pool = config.getWorkerPool( "syncBatchCounts" );
				var duplicatedJobId = createUUID();

				// First delivery.
				provider.marshalJob(
					getWireBox()
						.getInstance( "@cbq" )
						.job( "SendWelcomeEmailJob" )
						.setId( duplicatedJobId )
						.withBatchId( batch.getId() ),
					pool
				);

				// Duplicate delivery of the SAME job id — what a timeout-based
				// re-delivery or a stolen-and-rerun claim looks like to the batch.
				provider.marshalJob(
					getWireBox()
						.getInstance( "@cbq" )
						.job( "SendWelcomeEmailJob" )
						.setId( duplicatedJobId )
						.withBatchId( batch.getId() ),
					pool
				);

				var updatedBatch = repository.find( batch.getId() );
				expect( updatedBatch.getPendingJobs() ).toBe(
					1,
					"Two deliveries of one job must decrement pendingJobs exactly once"
				);
				expect( updatedBatch.getSuccessfulJobs() ).toBe( 1, "Two deliveries of one job must count one success" );
				expect( updatedBatch.getCompletedDate() ).toBeNull( "The batch must not finish while its second job is still pending" );
			} );

			it( "skips batch recording and chain dispatch when the worker no longer owns the job", function() {
				var repository = getWireBox().getInstance( "DBBatchRepository@cbq" );
				var config = registerGatingConnectionAndWorkerPool();
				var batch = createTrackedBatch( repository, 1 );
				var provider = config.getConnection( "syncBatchGating" ).getProvider();
				var pool = config.getWorkerPool( "syncBatchGating" );

				// Simulate a stolen delivery: the ownership-guarded terminal write matched 0 rows.
				prepareMock( provider ).$( "afterJobRun", false );

				var job = getWireBox()
					.getInstance( "@cbq" )
					.job( "SendWelcomeEmailJob" )
					.setId( createUUID() )
					.withBatchId( batch.getId() );

				provider.marshalJob( job, pool );

				var updatedBatch = repository.find( batch.getId() );
				expect( updatedBatch.getPendingJobs() ).toBe(
					1,
					"A job this worker no longer owns must not be recorded against the batch"
				);
				expect( updatedBatch.getSuccessfulJobs() ).toBe( 0 );
			} );

			it( "does not route a successful job into the failure path when the completion write throws", function() {
				var repository = getWireBox().getInstance( "DBBatchRepository@cbq" );
				var config = registerGatingConnectionAndWorkerPool();
				var batch = createTrackedBatch( repository, 1 );
				var provider = config.getConnection( "syncBatchGating" ).getProvider();
				var pool = config.getWorkerPool( "syncBatchGating" );

				// Simulate a DB blip at exactly the wrong moment: the job succeeded,
				// then the completion write itself throws.
				prepareMock( provider )
					.$( "afterJobRun" )
					.$throws( type = "database", message = "connection reset during completion write" );
				$spy( provider, "releaseJob" );

				var job = getWireBox()
					.getInstance( "@cbq" )
					.job( "SendWelcomeEmailJob" )
					.setId( createUUID() )
					.withBatchId( batch.getId() );

				expect( () => provider.marshalJob( job, pool ) ).notToThrow();

				expect( provider.$never( "releaseJob" ) ).toBeTrue(
					"A job that already succeeded must not be released and re-run because its completion write failed"
				);

				var updatedBatch = repository.find( batch.getId() );
				expect( updatedBatch.getPendingJobs() ).toBe( 1, "Unproven ownership must not record the batch job" );
				expect( updatedBatch.getSuccessfulJobs() ).toBe( 0 );
				expect( updatedBatch.getFailedJobs() ).toBe(
					0,
					"The batch must not be told the job failed — it succeeded"
				);
			} );

			it( "a duplicate success record does not drive pendingJobs negative or re-finish the batch", function() {
				var repository = getWireBox().getInstance( "DBBatchRepository@cbq" );
				var batch = createTrackedBatch( repository, 1 );
				var jobId = createUUID();

				batch.recordSuccessfulJob( jobId );
				var finishedBatch = repository.find( batch.getId() );
				expect( finishedBatch.getPendingJobs() ).toBe( 0 );
				expect( finishedBatch.getCompletedDate() ).notToBeNull();

				finishedBatch.recordSuccessfulJob( jobId );

				var updatedBatch = repository.find( batch.getId() );
				expect( updatedBatch.getPendingJobs() ).toBe( 0 );
				expect( updatedBatch.getSuccessfulJobs() ).toBe( 1 );
			} );
		} );
	}

	private any function registerSyncConnectionAndWorkerPool() {
		var config = getWireBox().getInstance( "Config@cbq" );

		if ( !config.getConnections().keyExists( "syncBatchCounts" ) ) {
			config.registerConnection(
				name = "syncBatchCounts",
				provider = getWireBox().getInstance( "SyncProvider@cbq" ).setProperties( {} )
			);
		}

		if ( !config.getWorkerPools().keyExists( "syncBatchCounts" ) ) {
			config.registerWorkerPool(
				name = "syncBatchCounts",
				connectionName = "syncBatchCounts",
				maxAttempts = 2
			);
		}

		return config;
	}

	private any function registerGatingConnectionAndWorkerPool() {
		var config = getWireBox().getInstance( "Config@cbq" );

		if ( !config.getConnections().keyExists( "syncBatchGating" ) ) {
			config.registerConnection(
				name = "syncBatchGating",
				provider = getWireBox().getInstance( "SyncProvider@cbq" ).setProperties( {} )
			);
		}

		if ( !config.getWorkerPools().keyExists( "syncBatchGating" ) ) {
			config.registerWorkerPool(
				name = "syncBatchGating",
				connectionName = "syncBatchGating",
				maxAttempts = 2
			);
		}

		return config;
	}

	private any function createTrackedBatch( required any repository, required numeric totalJobs ) {
		var pendingBatch = getWireBox()
			.getInstance( "@cbq" )
			.batch( [] )
			.allowFailures();
		var batch = arguments.repository.store( pendingBatch );
		arguments.repository.incrementTotalJobs( batch.getId(), arguments.totalJobs );
		return arguments.repository.find( batch.getId() );
	}

}
