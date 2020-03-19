# es-error-capture
Module is called during the Imputation step function if for some reason there is an error in one of the modules. It will send an sns message with a summary of error information, then delete the queue.
mike
### Parameters
run_id: The id of the current run - Type: String<br>
queue_url: The url of the queue used for the current run - Type: String
