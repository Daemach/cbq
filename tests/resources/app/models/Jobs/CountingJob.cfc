component extends="cbq.models.Jobs.AbstractJob" {

	function handle() {
		lock name="countingJobLock" type="exclusive" timeout="10" {
			param application.countingJobExecutions = 0;
			param application.countingJobExecutionLog = [];
			application.countingJobExecutions += 1;
			application.countingJobExecutionLog.append( getId() );
		}
	}

}
