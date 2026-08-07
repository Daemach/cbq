component extends="tests.resources.ModuleIntegrationSpec" appMapping="/app" {

	function run() {
		describe( "cbq_jobs attempts column", function() {
			beforeEach( function() {
				getInstance( "QueryBuilder@qb" ).from( "cbq_jobs" ).delete();
			} );

			it( "can store an attempts value above the old tinyint ceiling of 255", function() {
				// Rows really do reach 255 — attempt-burning claim races and
				// maxAttempts: 0 (release-forever) jobs both get there — and at
				// tinyint the next `attempts = attempts + 1` throws mid-claim
				// instead of failing the job cleanly.
				getInstance( "QueryBuilder@qb" )
					.table( "cbq_jobs" )
					.insert( {
						"queue" : "default",
						"payload" : serializeJSON( {
							"mapping" : "NonExistentJob",
							"properties" : {},
							"currentAttempt" : 300
						} ),
						"attempts" : 300,
						"availableDate" : 4102444800,
						"createdDate" : 4102444800
					} );

				var record = getInstance( "QueryBuilder@qb" ).from( "cbq_jobs" ).first();
				expect( record.attempts ).toBe( 300 );
			} );
		} );
	}

}
