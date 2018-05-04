package com.softcraft.batching

import akka.actor.UntypedActor

abstract class ItemProcessingWorker extends UntypedActor {

    @Override
    void onReceive(Object o) throws Exception {
        switch (o) {
            case ProcessItem:
                process(o)
                break
        }
    }


    abstract def ItemProcessingError process(ProcessItem item)

}
