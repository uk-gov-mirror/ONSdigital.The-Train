# The Train

Transactional file publishing tool over HTTP, name inspired by The Train from The Magic Roundabout.

Runs an embedded Jetty server that accepts files in parallel over HTTP and commits them to a local directory, backing up replaced files as necessary and providing optional encryption.

## Basics

FIve APIs are available to control file publishing:

 * POST to `/begin` to begin a transaction and get a `transactionId`.
 * POST the files you want to publish to `/publish` to add them to the transaction, specifying `transactionId` and `uri` (destination path within the target directory) parameters in the url. 
   * To transfer multiple files in a single API call, send a zip file and specify `zip=true` in the url. Paths in the zip will be appended to the `uri` parameter when extracting files in the destination.
 * POST to `/commit` or `/rollback`, specifying a `transactionId` parameter in the url to end a transaction.
 * GET `/transaction`, specifying a `transactionId` parameter in the url to get the details of a transaction.
 
By default the publisher will operate on temp directories. It prints out console messages about the configuration variables you can use to set up directories in production, or take a look at the `Configuration` class.
