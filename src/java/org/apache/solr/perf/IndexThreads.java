package org.apache.solr.perf;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.http.NoHttpResponseException;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;

import java.io.IOException;
import java.net.ConnectException;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class IndexThreads {

  final IngestRatePrinter printer;
  final CountDownLatch startLatch = new CountDownLatch(1);
  final AtomicBoolean stop;
  final AtomicBoolean failed;
  final LineFileDocs docs;
  final Thread[] threads;

  public IndexThreads(SolrClient client, AtomicBoolean indexingFailed, LineFileDocs lineFileDocs, int numThreads, int docCountLimit,
                      boolean printDPS, float docsPerSecPerThread, UpdatesListener updatesListener, int batchSize, boolean overwrite)
          throws IOException, InterruptedException {

    this.docs = lineFileDocs;

    threads = new Thread[numThreads];

    final CountDownLatch stopLatch = new CountDownLatch(numThreads);
    final AtomicInteger count = new AtomicInteger();
    stop = new AtomicBoolean(false);
    failed = indexingFailed;

    for (int thread = 0; thread < numThreads; thread++) {
      threads[thread] = new IndexThread(startLatch, stopLatch, client, docs, docCountLimit, count, stop, docsPerSecPerThread, failed, updatesListener, batchSize, overwrite);
      threads[thread].start();
    }

    Thread.sleep(10);

    if (printDPS) {
      printer = new IngestRatePrinter(count, stop);
      printer.start();
    } else {
      printer = null;
    }
  }

  public void start() {
    startLatch.countDown();
  }

  public long getBytesIndexed() {
    return docs.getBytesIndexed();
  }

  public void stop() throws InterruptedException, IOException {
    stop.getAndSet(true);
    for (Thread t : threads) {
      t.join();
    }
    if (printer != null) {
      printer.join();
    }
    docs.close();
  }

  public boolean done() {
    for (Thread t : threads) {
      if (t.isAlive()) {
        return false;
      }
    }

    return true;
  }



  public static interface UpdatesListener {
    public void beforeUpdate();

    public void afterUpdate();
  }

  private static class IndexThread extends Thread {
    private final LineFileDocs docs;
    private final int numTotalDocs;
    private final SolrClient client;
    private final AtomicBoolean stop;
    private final AtomicInteger count;
    private final CountDownLatch startLatch;
    private final CountDownLatch stopLatch;
    private final float docsPerSec;
    private final AtomicBoolean failed;
    private final UpdatesListener updatesListener;
    private final int batchSize;
    private final boolean overwrite;

    public IndexThread(CountDownLatch startLatch, CountDownLatch stopLatch, SolrClient client,
                       LineFileDocs docs, int numTotalDocs, AtomicInteger count,
                       AtomicBoolean stop, float docsPerSec,
                       AtomicBoolean failed, UpdatesListener updatesListener, int batchSize, boolean overwrite) {
      this.startLatch = startLatch;
      this.stopLatch = stopLatch;
      this.client = client;
      this.docs = docs;
      this.numTotalDocs = numTotalDocs;
      this.count = count;
      this.stop = stop;
      this.docsPerSec = docsPerSec;
      this.failed = failed;
      this.updatesListener = updatesListener;
      this.batchSize = batchSize;
      this.overwrite = overwrite;
    }

    @Override
    public void run() {
      try {
        final LineFileDocs.DocState docState = docs.newDocState();
        final long tStart = System.nanoTime();

        try {
          startLatch.await();
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt();
          return;
        }

        if (docsPerSec > 0) {
          final long startNS = System.nanoTime();
          int threadCount = 0;
          while (!stop.get()) {
            final SolrInputDocument doc = docs.nextDoc(docState);
            if (doc == null) {
              break;
            }
            final int id = LineFileDocs.idToInt(doc.getFieldValue("id").toString());
            if (numTotalDocs != -1 && id >= numTotalDocs) {
              break;
            }

            if (((1 + id) % 100000) == 0) {
              System.out.println("Indexer: " + (1 + id) + " docs... (" + TimeUnit.MILLISECONDS.convert(System.nanoTime() - tStart, TimeUnit.NANOSECONDS) + " msec)");
            }
            if (updatesListener != null) {
              updatesListener.beforeUpdate();
            }
            client.add(doc);

            if (updatesListener != null) {
              updatesListener.afterUpdate();
            }
            int docCount = count.incrementAndGet();
            threadCount++;

            if ((docCount % 100000) == 0) {
              System.out.println("Indexer: " + docCount + " docs... (" + TimeUnit.MILLISECONDS.convert(System.nanoTime() - tStart, TimeUnit.NANOSECONDS) + " msec)");
            }

            final long sleepNS = startNS + (long) (1000000000 * (threadCount / docsPerSec)) - System.nanoTime();
            if (sleepNS > 0) {
              final long sleepMS = sleepNS / 1000000;
              final int sleepNS2 = (int) (sleepNS - sleepMS * 1000000);
              Thread.sleep(sleepMS, sleepNS2);
            }
          }
        } else {
          boolean finished = false;
          UpdateRequest updateRequest = new UpdateRequest();
          while (!stop.get() && !finished) {
            if (batchSize > 1)  {
              finished = generateAndIndexBatch(docState, updateRequest, tStart);
              updateRequest.clear();
            } else  {
              final SolrInputDocument doc = docs.nextDoc(docState);
              if (doc == null) {
                break;
              }
              updateRequest.add(doc, overwrite);
              sendBatch(updateRequest, 10, 3);
              updateRequest.clear();
            }
          }
        }
      } catch (Exception e) {
        failed.set(true);
        throw new RuntimeException(e);
      } finally {
        stopLatch.countDown();
      }
    }

    /**
     * @return true if all docs are over
     * @throws Exception
     */
    private boolean generateAndIndexBatch(LineFileDocs.DocState docState, UpdateRequest reuseRequest, long tStart) throws Exception {
      for (int i=0; i<batchSize; i++) {
        final SolrInputDocument doc = docs.nextDoc(docState);
        if (doc == null) {
          reuseRequest.add(doc, overwrite);
          sendBatch(reuseRequest, 10, 3);
          return true;
        }
        int docCount = count.incrementAndGet();
        if (numTotalDocs != -1 && docCount > numTotalDocs) {
          break;
        }
        if ((docCount % 100000) == 0) {
          long timeSinceStart = TimeUnit.MILLISECONDS.convert(System.nanoTime() - tStart, TimeUnit.NANOSECONDS);
          System.out.format(Locale.ROOT, "Indexer: %d docs at %.2f sec (%.1f MB/sec %.1f docs/sec)\n",
                  docCount, timeSinceStart/1000.,
                  (docs.getBytesIndexed() / 1024. / 1024 / (timeSinceStart / 1000.)),
                  (docCount / (timeSinceStart / 1000.)));
        }
        reuseRequest.add(doc, overwrite);
      }
      sendBatch(reuseRequest, 10, 3);
      return false;
    }

    protected int sendBatch(UpdateRequest reuseRequest, int waitBeforeRetry, int maxRetries) {
      int sent;
      try {
        client.request(reuseRequest);
        sent = reuseRequest.getDocuments().size();
      } catch (Exception exc) {
        Throwable rootCause = SolrException.getRootCause(exc);
        boolean wasCommError =
                (rootCause instanceof ConnectException ||
                        rootCause instanceof ConnectTimeoutException ||
                        rootCause instanceof NoHttpResponseException ||
                        rootCause instanceof SocketException);

        if (wasCommError) {
          if (--maxRetries > 0) {
            System.out.println("ERROR: " + rootCause + " ... Sleeping for "
                    + waitBeforeRetry + " seconds before re-try ...");
            try {
              Thread.sleep(waitBeforeRetry * 1000L);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
            sent = sendBatch(reuseRequest, waitBeforeRetry, maxRetries);
          } else {
            throw new RuntimeException("No more retries available!", exc);
          }
        } else {
          throw new RuntimeException(exc);
        }
      }
      return sent;
    }

  }

  private static class IngestRatePrinter extends Thread {

    private final AtomicInteger count;
    private final AtomicBoolean stop;

    public IngestRatePrinter(AtomicInteger count, AtomicBoolean stop) {
      this.count = count;
      this.stop = stop;
    }

    @Override
    public void run() {
      long time = System.currentTimeMillis();
      System.out.println("startIngest: " + time);
      final long start = time;
      int lastCount = count.get();
      while (!stop.get()) {
        try {
          Thread.sleep(200);
        } catch (Exception ex) {
        }
        int numDocs = count.get();

        double current = numDocs - lastCount;
        long now = System.currentTimeMillis();
        double seconds = (now - time) / 1000.0d;
        System.out.println("ingest: " + (current / seconds) + " " + (now - start));
        time = now;
        lastCount = numDocs;
      }
    }
  }
}
