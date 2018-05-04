package com.softcraft.batching

class ItemProcessingWorkerImpl extends ItemProcessingWorker {
    @Override
    ItemProcessingError process(ProcessItem item) {
        println this.toString()
        println item.toString()
    }
}
