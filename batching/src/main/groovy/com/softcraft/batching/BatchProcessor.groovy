package com.softcraft.batching

import akka.actor.ActorSystem
import akka.actor.Props
import akka.actor.UntypedActor
import akka.event.Logging
import akka.event.LoggingAdapter
import akka.routing.RoundRobinPool

class BatchProcessor extends UntypedActor {

    def workers = context.actorOf(Props.create(ItemProcessingWorkerImpl).withRouter(new RoundRobinPool(5)))

    def totalItemCount = -1, currentBatchSize = 0, currentProcessedItemsCount = 0
    List<ItemProcessingError> currentProcessingErrors = Collections.EMPTY_LIST

    int allProcessedItemsCount = 0
    List<ItemProcessingError> allProcessingErrors = Collections.EMPTY_LIST

    @Override
    void onReceive(Object o) throws Exception {
        switch (o) {
            case ProcessBatch:
                ProcessBatch pb = (ProcessBatch) o
                if (totalItemCount == -1) {
                    totalItemCount = pb.totalItems
                    log.info("Starting to process set with ID ${pb.dataSetId}, we have ${totalItemCount} items to go through")
                }
                def batch = pb.fetchBatch
                processBatch(batch)

            case ProcessedOneItem:
                currentProcessedItemsCount = currentProcessedItemsCount + 1
                continueProcessing()
//            case error @ ItemProcessingError(_, _, _) =>
//                currentProcessingErrors = error :: currentProcessingErrors
//                continueProcessing()
        }
    }

    def processBatch(List batch) {
        if (batch.isEmpty()) {
            log.info("Done migrating all items for data set ${dataSetId}. $totalItems processed items, we had ${allProcessingErrors.size} errors in total")
//            getContext().stop(getSelf())
        } else {
            // reset processing state for the current batch
            currentBatchSize = batch.size()
            allProcessedItemsCount = currentProcessedItemsCount + allProcessedItemsCount
            currentProcessedItemsCount = 0

            allProcessingErrors.addAll(currentProcessingErrors)
            currentProcessingErrors = Collections.EMPTY_LIST

            // distribute the work
            batch.each { item ->
                def batchItem = new BatchItem(id: item)
                workers.tell(new ProcessItem(batchItem), getSelf())
            }
        }
    }


    def continueProcessing() {
        def itemsProcessed = currentProcessedItemsCount + currentProcessingErrors.size()

        if (itemsProcessed > 0 && itemsProcessed % 100 == 0) {
            log.info("Processed ${itemsProcessed} out of ${currentBatchSize} with ${currentProcessingErrors.size} errors")
        }
        if (itemsProcessed == currentBatchSize) {
            self.tell(ProcessBatch)
        }
    }

    static ActorSystem system = ActorSystem.create("rulesUpdaterActorSystem")

    static LoggingAdapter log = Logging.getLogger(system, this)
    public static void main(String[] args) {
        log.info( "START")
        def batchActor = system.actorOf(Props.create(BatchProcessor.class), "terminate")
        def list = (2..100)
        def processBatch = new ProcessBatch(totalItems: list.size(), fetchBatch: list, dataSetId: "test")
        batchActor.tell(processBatch, batchActor)

        Thread.sleep(5000)
        system.terminate()
    }
}
