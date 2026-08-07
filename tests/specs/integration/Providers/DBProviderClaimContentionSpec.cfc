component extends="tests.resources.ModuleIntegrationSpec" appMapping="/app" {

	function run() {
		describe( "DBProvider claim contention", function() {
			beforeEach( function() {
				lock name="countingJobLock" type="exclusive" timeout="10" {
					application.countingJobExecutions = 0;
					application.countingJobExecutionLog = [];
				}
				getInstance( "QueryBuilder@qb" ).from( "cbq_jobs" ).delete();
			} );

			it( "claims 10 jobs with identical availableDates exactly once each across two competing pools", function() {
				var provider = exposedProvider();
				var poolA = makeContentionPool( provider, "contention-pool-a" );
				var poolB = makeContentionPool( provider, "contention-pool-b" );

				for ( var i = 1; i <= 10; i++ ) {
					provider.push( "default", newCountingJob() );
				}
				// Force every row to the exact same eligibility instant.
				getInstance( "QueryBuilder@qb" )
					.table( "cbq_jobs" )
					.update( { "availableDate" : getInstance( "DBProvider@cbq" ).getCurrentUnixTimestamp( -5 ) } );

				// Alternate full watcher ticks between the two pools until the
				// backlog drains: fetch candidates, lock, fetch locked, process.
				var rounds = 0;
				while ( currentExecutionCount() < 10 && rounds < 40 ) {
					rounds++;
					runWatcherTick(
						provider,
						rounds mod 2 == 1 ? poolA : poolB,
						4
					);
					sleep( 100 ); // let the pool executors drain between ticks
				}
				waitUntil( () => currentExecutionCount() >= 10, 10000 );

				expect( currentExecutionCount() ).toBe(
					10,
					"Every job must execute exactly once — no losses, no duplicates"
				);
				var seen = {};
				for ( var jobId in application.countingJobExecutionLog ) {
					expect( seen ).notToHaveKey( toString( jobId ), "Job [#jobId#] was executed more than once" );
					seen[ toString( jobId ) ] = true;
				}
			} );

			it( "documents the 6.0.0 claim-window steal: the job runs twice, but the batch only counts it once", function() {
				var repository = getWireBox().getInstance( "DBBatchRepository@cbq" );
				var provider = exposedProvider();
				var poolA = makeContentionPool( provider, "steal-pool-a" );
				var poolB = makeContentionPool( provider, "steal-pool-b" );

				var batch = repository.store(
					getWireBox()
						.getInstance( "@cbq" )
						.batch( [] )
						.allowFailures()
				);
				repository.incrementTotalJobs( batch.getId(), 1 );

				var job = newCountingJob().withBatchId( batch.getId() );
				provider.push( "default", job );
				getInstance( "QueryBuilder@qb" )
					.table( "cbq_jobs" )
					.update( { "availableDate" : getInstance( "DBProvider@cbq" ).getCurrentUnixTimestamp( -5 ) } );

				// Pool A claims and fetches its work list, but has not yet reserved
				// (reservedDate is still NULL and availableDate is still in the past —
				// the claim-window state).
				var idsA = provider.fetchPotentiallyOpenRecords_( 10, poolA );
				provider.tryToLockRecords_( idsA, poolA );
				var lockedA = provider.fetchLockedRecords_( 10, poolA );
				expect( lockedA ).toHaveLength( 1, "Pool A should hold the claim" );

				// Pool B's tick fires in the window: the fetch treats A's live claim
				// as an orphan, and the 6.0.0 lock branch steals it.
				var idsB = provider.fetchPotentiallyOpenRecords_( 10, poolB );
				expect( idsB ).toHaveLength(
					1,
					"Pool B sees A's claim-window row as claimable — this is the ae6b23d hazard"
				);
				provider.tryToLockRecords_( idsB, poolB );
				var lockedB = provider.fetchLockedRecords_( 10, poolB );
				expect( lockedB ).toHaveLength( 1, "Pool B stole the claim" );

				// Both pools now process "their" job.
				for ( var record in lockedA ) {
					provider.processLockedRecord_( record, poolA );
				}
				for ( var record in lockedB ) {
					provider.processLockedRecord_( record, poolB );
				}
				waitUntil( () => currentExecutionCount() >= 2, 10000 );

				// The steal itself is real on this branch (the claim redesign fixes it):
				expect( currentExecutionCount() ).toBe(
					2,
					"6.0.0 double-executes the stolen job — fixed by the atomic claim, not this PR"
				);

				// ...but this PR's guarantee holds: the batch is exactly-once anyway.
				waitUntil( () => repository.find( batch.getId() ).getSuccessfulJobs() == 1, 10000 );
				var settled = repository.find( batch.getId() );
				expect( settled.getSuccessfulJobs() ).toBe( 1, "Two executions must record exactly one batch success" );
				expect( settled.getPendingJobs() ).toBe( 0, "pendingJobs must land at 0, not go negative" );
				expect( settled.getCompletedDate() ).notToBeNull( "The batch finishes exactly once" );
			} );
		} );
	}

	private any function newCountingJob() {
		return getWireBox().getInstance( "@cbq" ).job( "CountingJob" );
	}

	private any function exposedProvider() {
		var provider = getWireBox().getInstance( "DBProvider@cbq" ).setProperties( {} );
		makePublic(
			provider,
			"fetchPotentiallyOpenRecords",
			"fetchPotentiallyOpenRecords_"
		);
		makePublic(
			provider,
			"tryToLockRecords",
			"tryToLockRecords_"
		);
		makePublic(
			provider,
			"fetchLockedRecords",
			"fetchLockedRecords_"
		);
		makePublic(
			provider,
			"processLockedRecord",
			"processLockedRecord_"
		);
		return provider;
	}

	private any function makeContentionPool( required any provider, required string name ) {
		var connection = getInstance( "QueueConnection@cbq" )
			.setName( arguments.name & "-connection" )
			.setProvider( arguments.provider );

		return getInstance( "WorkerPool@cbq" )
			.setName( arguments.name )
			.setConnection( connection )
			.setConnectionName( connection.getName() )
			.startWorkers();
	}

	private void function runWatcherTick(
		required any provider,
		required any pool,
		required numeric capacity
	) {
		var ids = arguments.provider.fetchPotentiallyOpenRecords_( arguments.capacity, arguments.pool );
		if ( ids.isEmpty() ) {
			return;
		}
		arguments.provider.tryToLockRecords_( ids, arguments.pool );
		var locked = arguments.provider.fetchLockedRecords_( arguments.capacity, arguments.pool );
		for ( var record in locked ) {
			arguments.provider.processLockedRecord_( record, arguments.pool );
		}
	}

	private numeric function currentExecutionCount() {
		lock name="countingJobLock" type="readonly" timeout="10" {
			return application.countingJobExecutions ?: 0;
		}
	}

	private void function waitUntil( required any condition, numeric timeoutMs = 5000 ) {
		var waited = 0;
		while ( waited < arguments.timeoutMs ) {
			try {
				if ( arguments.condition() ) {
					return;
				}
			} catch ( any e ) {
				// condition not ready yet
			}
			sleep( 50 );
			waited += 50;
		}
	}

}
