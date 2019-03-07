package org.dataone.cn.utility;

/**
 * A speculative rebuild of the task processor using CompletableFuture, which can 
 * run tasks asynchronously.  Based on ideas in:
 * 
 *  
 */
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.dataone.cn.index.processor.IndexTaskProcessor;
import org.dataone.cn.index.task.IndexTask;

public class AsyncIndexTaskBatchProcessor  {

	private IndexTaskProcessor processor; 
	private IndexTask indexTask;
	
	final static BlockingQueue<Runnable> QUEUE = new ArrayBlockingQueue<>(100);
	ExecutorService executorService = new ThreadPoolExecutor(10, 10,
	0L, TimeUnit.MILLISECONDS,
	QUEUE);

	public AsyncIndexTaskBatchProcessor(IndexTaskProcessor itp) {
		this.processor = itp;
	}

	public void setIndexTask(IndexTask task) {
		this.indexTask = task;
	}

	public void receiver() {

	}


	public void processIndexTaskQueue() {
		//       logProcessorLoad();

		IndexTask[] queue = (IndexTask[]) processor.getIndexTaskQueue().toArray();
		//        IndexTask task = getNextIndexTask(queue);

		for (int i=0; i<queue.length; i++) {
			if (readyToProcess(queue[i])) {
				final IndexTask t = queue[i];
				CompletableFuture<Void> future =
						CompletableFuture.runAsync(() -> processor.processTask(t), processor.getExecutorService())  // send the task to the executor
						.exceptionally(ex -> logException(t, ex));

			}
		}
		

    	// effectively process failed tasks from previous run
    	processor.processFailedIndexTaskQueue();
    	/*List<IndexTask> retryQueue = getIndexTaskRetryQueue();
        task = getNextIndexTask(retryQueue);
        while (task != null) {
            processTaskOnThread(task);
            task = getNextIndexTask(retryQueue);
        }*/

    }

    
    private boolean readyToProcess(IndexTask task) {
    		return true;
    }
     
    private Void logException(IndexTask task, Throwable ex) {
		// TODO Auto-generated method stub
		 return null;
	}

	private IndexTask getNextIndexTask(List<IndexTask> queue) {
        // index task processor does some checks on the 
        // on the tasks to make sure they are ready to process
        // (objectPath and ORE readiness)
        
        // this is just a simple get next for now
        return queue.remove(0);
    }
    
}
