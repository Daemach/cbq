component {

	function up( schema, qb ) {
		schema.alter( "cbq_jobs", ( t ) => {
			t.modifyColumn( "attempts", t.unsignedInteger( "attempts" ) );
		} );
	}

	function down( schema, qb ) {
		schema.alter( "cbq_jobs", ( t ) => {
			t.modifyColumn( "attempts", t.unsignedTinyInteger( "attempts" ) );
		} );
	}

}
