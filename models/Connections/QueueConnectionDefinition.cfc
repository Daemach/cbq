component accessors="true" {

	property name="config" inject="provider:Config@cbq";

	property name="name";
	property name="provider";
	property name="properties";
	property name="defaultQueue" default="default";
	property name="makeDefault";

	public QueueConnectionDefinition function init() {
		variables.makeDefault = false;
		return this;
	}

	public QueueConnectionDefinition function provider( required any provider ) {
		variables.provider = arguments.provider;
		return this;
	}

	public QueueConnectionDefinition function onQueue( required string name ) {
		setDefaultQueue( arguments.name );
		return this;
	}

	public QueueConnectionDefinition function markAsDefault( boolean check = true ) {
		variables.makeDefault = arguments.check;
		return this;
	}

	public QueueConnection function register() {
		return variables.config.registerConnectionFromDefinition( this );
	}

}
