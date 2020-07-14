# es-error-capture
Module is called during the step function if for some reason there is an error in one of the modules. It will send an sns message with a summary of error information, then delete the queue.

### Parameters
run_id: The id of the current run - Type: String<br>
