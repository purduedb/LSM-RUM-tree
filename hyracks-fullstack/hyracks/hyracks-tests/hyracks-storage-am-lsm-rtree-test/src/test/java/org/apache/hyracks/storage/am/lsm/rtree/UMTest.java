package org.apache.hyracks.storage.am.lsm.rtree;

import java.io.*;
import java.util.*;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hyracks.api.dataflow.value.IBinaryComparatorFactory;
import org.apache.hyracks.api.dataflow.value.ISerializerDeserializer;
import org.apache.hyracks.api.dataflow.value.ITypeTraits;
import org.apache.hyracks.api.exceptions.ErrorCode;
import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.data.std.accessors.DoubleBinaryComparatorFactory;
import org.apache.hyracks.data.std.accessors.IntegerBinaryComparatorFactory;
import org.apache.hyracks.data.std.primitive.DoublePointable;
import org.apache.hyracks.data.std.primitive.IntegerPointable;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleBuilder;
import org.apache.hyracks.dataflow.common.comm.io.ArrayTupleReference;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.dataflow.common.data.marshalling.DoubleSerializerDeserializer;
import org.apache.hyracks.dataflow.common.data.marshalling.IntegerSerializerDeserializer;
import org.apache.hyracks.dataflow.common.utils.TupleUtils;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProviderFactory;
import org.apache.hyracks.storage.am.common.api.ITreeIndex;
import org.apache.hyracks.storage.am.common.impls.NoOpIndexAccessParameters;
import org.apache.hyracks.storage.am.common.updatememo.UpdateMemoConfig;
import org.apache.hyracks.storage.am.config.AccessMethodTestsConfig;
import org.apache.hyracks.storage.am.lsm.rtree.util.LSMRTreeTestHarness;
import org.apache.hyracks.storage.am.lsm.rtree.utils.LSMRTreeUtils;
import org.apache.hyracks.storage.am.rtree.frames.RTreePolicyType;
import org.apache.hyracks.storage.am.rtree.impls.SearchPredicate;
import org.apache.hyracks.storage.am.rtree.util.RTreeUtils;
import org.apache.hyracks.storage.common.IIndexAccessor;
import org.apache.hyracks.storage.common.IIndexCursor;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.hyracks.storage.common.buffercache.CleaningConfig;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class UMTest {
    private final LSMRTreeTestHarness lsmRTreeHarness;

    protected final Random rnd = new Random(50);

    protected static final Logger LOGGER = LogManager.getLogger();
    private String dataPath;
    private int numData;
    private String dataIndex;
    private String dataBoundary;
    private String queryPoints;

    ITreeIndex rTreeIndex;
    int rFieldCount;
    int rtreeKeyFieldCount;
    //Libin: concurrent exp
    AtomicInteger total_operations_num;
    long concurrent_time = 0;
    ISerializerDeserializer[] rFieldSerdes =
            { DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                    DoubleSerializerDeserializer.INSTANCE, DoubleSerializerDeserializer.INSTANCE,
                    IntegerSerializerDeserializer.INSTANCE, IntegerSerializerDeserializer.INSTANCE };;
    IBinaryComparatorFactory[] rTreeCmpFactories;

    public UMTest(String[] args) {
        total_operations_num = new AtomicInteger(0);
        UpdateMemoConfig.USE_LOCAL_UM = Boolean.valueOf(args[0]);

        UpdateMemoConfig.CLEAN_UPON_FLUSHING = Boolean.valueOf(args[1]);//F
        UpdateMemoConfig.CLEAN_UPON_MERGING = Boolean.valueOf(args[2]);//M

        UpdateMemoConfig.CLEAN_UPON_UPDATE = Boolean.valueOf(args[3]);//B
        UpdateMemoConfig.VACUUM_CLEAN = Boolean.valueOf(args[4]);//V

        UpdateMemoConfig.SELECTED_MERGE_POLICY = args[5];
        UpdateMemoConfig.NUM_COMPONENTS = args[6];

        CleaningConfig.UPDATE_THRESHOLD = Integer.valueOf(args[7]);
        CleaningConfig.VACUUM_THRESHOLD = Integer.valueOf(args[8]);

        this.dataPath = args[9];
        this.numData = Integer.valueOf(args[10]);
        this.dataIndex = args[11];
        this.dataBoundary = args[12];
        this.queryPoints = args[13];

        LOGGER.error("==========================================================================");
        LOGGER.error("Test params: LSMRTree with UM");
        LOGGER.error(
                "  - policy : " + UpdateMemoConfig.SELECTED_MERGE_POLICY + " : " + UpdateMemoConfig.NUM_COMPONENTS);
        LOGGER.error("  - Disk page size : " + AccessMethodTestsConfig.LSM_RTREE_DISK_PAGE_SIZE);
        LOGGER.error("  - Disk num pages : " + AccessMethodTestsConfig.LSM_RTREE_DISK_NUM_PAGES);
        LOGGER.error("  - Mem page size : " + AccessMethodTestsConfig.LSM_RTREE_MEM_PAGE_SIZE);
        LOGGER.error("  - Mem num pages : " + AccessMethodTestsConfig.LSM_RTREE_MEM_NUM_PAGES);
        LOGGER.error("  - data path	: " + this.dataPath);
        LOGGER.error("  -      #	: " + this.numData);
        LOGGER.error("  -      bdry	: " + this.dataBoundary);
        LOGGER.error("  -      indx	: " + this.dataIndex);
        LOGGER.error("  -      qry	: " + this.queryPoints);
        LOGGER.error(" ");
        LOGGER.error("  - use local um      : " + UpdateMemoConfig.USE_LOCAL_UM);
        LOGGER.error("  - clean-upon-flush  : " + UpdateMemoConfig.CLEAN_UPON_FLUSHING);
        LOGGER.error("  - clean-upon-merge  : " + UpdateMemoConfig.CLEAN_UPON_MERGING);
        LOGGER.error("  - buffered cleaning : " + UpdateMemoConfig.CLEAN_UPON_UPDATE);
        LOGGER.error("  -      threshold    : " + CleaningConfig.UPDATE_THRESHOLD);
        LOGGER.error("  - vacuum cleaning   : " + UpdateMemoConfig.VACUUM_CLEAN);
        LOGGER.error("  -      threshold    : " + CleaningConfig.VACUUM_THRESHOLD);
        LOGGER.error(" ");

        UpdateMemoConfig.reset();

        lsmRTreeHarness = new LSMRTreeTestHarness();
    }

    public void setUp() throws HyracksDataException {
        this.lsmRTreeHarness.setUp();

        // Declare fields.
        rFieldCount = 6;
        ITypeTraits[] rTypeTraits = new ITypeTraits[rFieldCount];
        rTypeTraits[0] = DoublePointable.TYPE_TRAITS;
        rTypeTraits[1] = DoublePointable.TYPE_TRAITS;
        rTypeTraits[2] = DoublePointable.TYPE_TRAITS;
        rTypeTraits[3] = DoublePointable.TYPE_TRAITS;
        rTypeTraits[4] = IntegerPointable.TYPE_TRAITS;
        rTypeTraits[5] = IntegerPointable.TYPE_TRAITS;

        // Declare RTree keys.
        rtreeKeyFieldCount = 4;
        rTreeCmpFactories = new IBinaryComparatorFactory[rtreeKeyFieldCount];
        rTreeCmpFactories[0] = DoubleBinaryComparatorFactory.INSTANCE;
        rTreeCmpFactories[1] = DoubleBinaryComparatorFactory.INSTANCE;
        rTreeCmpFactories[2] = DoubleBinaryComparatorFactory.INSTANCE;
        rTreeCmpFactories[3] = DoubleBinaryComparatorFactory.INSTANCE;

        // create value providers
        IPrimitiveValueProviderFactory[] valueProviderFactories =
                RTreeUtils.createPrimitiveValueProviderFactories(rTreeCmpFactories.length, DoublePointable.FACTORY);

        int btreeKeyFieldCount;
        IBinaryComparatorFactory[] btreeCmpFactories;
        int[] btreeFields = null;
        //Parameters look different for LSM RTREE from LSM RTREE WITH ANTI MATTER TUPLES
        btreeKeyFieldCount = 1;
        btreeCmpFactories = new IBinaryComparatorFactory[btreeKeyFieldCount];
        btreeCmpFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;

        btreeFields = new int[btreeKeyFieldCount];
        for (int i = 0; i < btreeKeyFieldCount; i++) {
            btreeFields[i] = rtreeKeyFieldCount + i;
        }
        //        btreeKeyFieldCount = 2;
        //        btreeCmpFactories = new IBinaryComparatorFactory[btreeKeyFieldCount];
        //        btreeCmpFactories[0] = IntegerBinaryComparatorFactory.INSTANCE;
        //        btreeCmpFactories[1] = IntegerBinaryComparatorFactory.INSTANCE;
        //        btreeFields = new int[btreeKeyFieldCount];
        //        for (int i = 0; i < btreeKeyFieldCount; i++) {
        //            btreeFields[i] = rtreeKeyFieldCount + i;
        //        }

        rTreeIndex = createRTreeIndex(rTypeTraits, rTreeCmpFactories, btreeCmpFactories, valueProviderFactories,
                RTreePolicyType.RTREE, null, btreeFields, null, null, null);

        rTreeIndex.create();
        rTreeIndex.activate();

    }

    public void tearDown() throws HyracksDataException {
        rTreeIndex.deactivate();
        rTreeIndex.destroy();

        this.lsmRTreeHarness.tearDown();
    }

    public void runTest() throws Exception {
        String data = this.dataPath;
        String[] ins = this.dataIndex.split(",");
        int[] index = new int[ins.length];
        for (int in = 0; in < ins.length; in++) {
            index[in] = Integer.parseInt(ins[in]);
        }

        FileInputStream fstream = new FileInputStream(data);
        DataInputStream in = new DataInputStream(fstream);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        long start = 0L;
        long end = 0L;
        long time = 0L;
        ArrayList<Long> times = new ArrayList<Long>();
        ArrayTupleBuilder tb = new ArrayTupleBuilder(rFieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        IIndexAccessor rIndexAccessor = rTreeIndex.createAccessor(NoOpIndexAccessParameters.INSTANCE);

        //        UpdateMemo<Integer> um = LSMRTreeUtils.getUM();
        //        int max = -1;

        int id;
        double x, y;
        double querySize;
        int ts;
        int i = 0;
        boolean isInsert = false;
        boolean isQuery = false;

        String[] boundaries = this.dataBoundary.split(",");
        double minX = Double.valueOf(boundaries[0]);
        double maxX = Double.valueOf(boundaries[1]);
        double minY = Double.valueOf(boundaries[2]);
        double maxY = Double.valueOf(boundaries[3]);
        ArrayTupleBuilder keyTb = new ArrayTupleBuilder(rtreeKeyFieldCount);
        ArrayTupleReference key = new ArrayTupleReference();
        long searchStart = 0L;
        long searchEnd = 0L;
        long searchTime = 0L;
        int numQuery = 0;
        ArrayList<Long> searchTimes = new ArrayList<Long>();
        ArrayList<Integer> numQueries = new ArrayList<Integer>();

        String line = "";
        while ((line = br.readLine()) != null && line.length() > 0) {
            //			LOGGER.error(line);
            String items[] = line.split(",");

            ts = Integer.valueOf(items[index[0]]);
            //System.out.println(line);
            id = Integer.valueOf(items[index[1]]);
            x = Double.valueOf(items[index[2]]);
            y = Double.valueOf(items[index[3]]);
            if (items[0].equals("Q")) {
                isQuery = true;
                isInsert = false;
                numQuery++;
            } else {
                i++;
                isQuery = false;
                if (items[0].equals("I")) {
                    isInsert = true;
                } else {
                    isInsert = false;
                }
                //				TupleUtils.createIntegerTuple(tb, tuple, x, y, x, y, id, ts);
                tuple = (ArrayTupleReference) TupleUtils.createTuple(rFieldSerdes, x, y, x, y, id, ts);
            }

            try {

                if (isQuery) {
                    querySize = (maxX - minX) * (Double.valueOf(items[index[4]]) * 1.0) / 2.0;
                    TupleUtils.createDoubleTuple(keyTb, key, x - querySize, y - querySize, x + querySize,
                            y + querySize);
                    searchStart = System.nanoTime();
                    rangeSearch(rTreeCmpFactories, rIndexAccessor, rFieldSerdes, key, null, null);
                    searchEnd = System.nanoTime();
                    searchTime += searchEnd - searchStart;
                    //System.out.println("run query");
                } else {
                    start = System.nanoTime();
                    if (isInsert)
                        rIndexAccessor.insert(tuple);
                    else {
                        rIndexAccessor.update(tuple);
                    }
                    end = System.nanoTime();
                    time += end - start;
                }

                //				max = Math.max(max, um.getSize());

            } catch (HyracksDataException e) {

                if (e.getErrorCode() != ErrorCode.DUPLICATE_KEY) {
                    throw e;
                }
            }
            if (ts % (this.numData / 10) == 0) {
                //				LOGGER.error(ts + " " + x + " " + y);
                LOGGER.error(String.format("%.2f", ((double) ts / this.numData * 100.0)) + "%");
                times.add(time);
                searchTimes.add(searchTime);
                numQueries.add(numQuery);
            }

        }
        times.add(time);
        searchTimes.add(searchTime);
        numQueries.add(numQuery);
        //Libin: point queries
        //query region
/*
        searchStart = 0L;
        searchEnd = 0L;
        searchTime = 0L;

        boundaries = this.dataBoundary.split(",");
        minX = Double.valueOf(boundaries[0]);
        maxX = Double.valueOf(boundaries[1]);
        minY = Double.valueOf(boundaries[2]);
        maxY = Double.valueOf(boundaries[3]);

        ArrayList<Double[]> searchPoints = new ArrayList<Double[]>();
        String[] points = this.queryPoints.split("/");
        for (int j = 0; j < points.length; j++) {
            String[] tmp = points[j].split(",");
            searchPoints.add(new Double[] { Double.valueOf(tmp[0]), Double.valueOf(tmp[1]) });
        }

        searchTimes = new ArrayList<Long>();
        keyTb = new ArrayTupleBuilder(rtreeKeyFieldCount);
        key = new ArrayTupleReference();

        int multiplier = 8;
        for (int j = 1; j < multiplier; j++) {
            searchTime = 0L;
            LOGGER.error("Search : " + String.format("%.4f", ((j * j * j * 0.001) * (j * j * j * 0.001) * 100)) + " %");
            for (int k = 0; k < searchPoints.size(); k++) {
                TupleUtils.createIntegerTuple(keyTb, key, -500 * i, -500 * i, 500 * i, 500 * i);
                double xx = searchPoints.get(k)[0];
                double yy = searchPoints.get(k)[1];
                double sizeX = ((maxX - minX) * j * j * j * 0.001) / 2.0;
                double sizeY = ((maxY - minY) * j * j * j * 0.001) / 2.0;

                LOGGER.error((xx - sizeX) + ", " + (yy - sizeY) + " to " + (xx + sizeX) + ", " + (yy + sizeY));
                TupleUtils.createDoubleTuple(keyTb, key, xx - sizeX, yy - sizeY, xx + sizeX, yy + sizeY);
                //tuple = (ArrayTupleReference) TupleUtils.createTuple(fieldSerdes, xx - sizeX, yy - sizeY, xx + sizeX, yy + sizeY);

                searchStart = System.nanoTime();
                LOGGER.error("k: " + k);
                rangeSearch(rTreeCmpFactories, rIndexAccessor, rFieldSerdes, key, null, null);
                searchEnd = System.nanoTime();
                searchTime += searchEnd - searchStart;
                LOGGER.error(searchTime);
            }

            searchTimes.add(searchTime / searchPoints.size());
        }

 */
        //query region
        LOGGER.error("-------------------------------------------------------------------");
        String lines = "";
        for (int j = 0; j < times.size(); j++)
            lines += times.get(j) + ", ";
        LOGGER.error(i + " inserts in " + lines + " ms");

        LOGGER.error("-------------------------------------------------------------------");
        lines = "";
        for (int j = 0; j < searchTimes.size(); j++)
            lines += searchTimes.get(j) + ", ";
        LOGGER.error(" queries in " + lines + " ms");
        lines = "";
        for (int j = 0; j < numQueries.size(); j++)
            lines += numQueries.get(j) + ", ";
        LOGGER.error("  # queries:  " + lines);
        //query region
        /*String reg = "(";
        for (int j = 1; j < multiplier; j++) {
            reg += String.format("%.4f", ((j * j * j * 0.001) * (j * j * j * 0.001) * 100)) + " %, ";
        }
        reg += ")";
        lines = "";
        for (int j = 0; j < searchTimes.size(); j++)
            lines += searchTimes.get(j) + ", ";
        LOGGER.error(reg + " searched in " + lines + " ms");*/
        //query region

        double af = 0.0;
        double am = 0.0;
        if (UpdateMemoConfig.NUM_FLUSH == 0)
            LOGGER.error("No Flush");
        else
            af = UpdateMemoConfig.sumFlushTime / UpdateMemoConfig.NUM_FLUSH;

        if (UpdateMemoConfig.NUM_MERGE == 0)
            LOGGER.error("No Merge");
        else
            am = UpdateMemoConfig.sumMergeTime / UpdateMemoConfig.NUM_MERGE;

        LOGGER.error("#Flush: " + UpdateMemoConfig.NUM_FLUSH + ", Sum Flush time: " + UpdateMemoConfig.sumFlushTime
                + ", Avg. Flush time: " + af + " ms // #Merge: " + UpdateMemoConfig.NUM_MERGE + ", Sum Merge time: "
                + UpdateMemoConfig.sumMergeTime + " ms, Avg. Merge time: " + am + " ms.");
        //        LOGGER.error("MAX: " + max);
        LOGGER.error("-------------------------------------------------------------------");
    }

    class Concurrent_Worker extends Thread {
        int my_id;
        int total_worker_num;
        CyclicBarrier barrier;
        IIndexAccessor rIndexAccessor; //= rTreeIndex.createAccessor(NoOpIndexAccessParameters.INSTANCE);

        Concurrent_Worker(int id, int num, CyclicBarrier input_barrier) {
            super();
            my_id = id;
            total_worker_num = num;
            barrier = input_barrier;
            //this.rIndexAccessor = rIndexAccessor;
        }

        public void run() {
            try {
                String data = dataPath;
                data += "_" + total_worker_num + "_" + my_id + ".dat";
                String[] ins = dataIndex.split(",");
                int[] index = new int[ins.length];
                for (int in = 0; in < ins.length; in++) {
                    index[in] = Integer.parseInt(ins[in]);
                }

                FileInputStream fstream = new FileInputStream(data);
                DataInputStream in = new DataInputStream(fstream);
                BufferedReader br = new BufferedReader(new InputStreamReader(in));
                rIndexAccessor = rTreeIndex.createAccessor(NoOpIndexAccessParameters.INSTANCE);
                long start = 0L;
                long end = 0L;
                long time = 0L;
                //ArrayList<Long> times = new ArrayList<Long>();
                ArrayTupleBuilder tb = new ArrayTupleBuilder(rFieldCount);
                ArrayTupleReference tuple = new ArrayTupleReference();
                //start loading the array
                ArrayList<String> input = new ArrayList<>();
                int id;
                double x, y;
                double querySize;
                int ts;
                int i = 0;
                boolean isInsert = false;
                boolean isQuery = false;

                String[] boundaries = dataBoundary.split(",");
                double minX = Double.valueOf(boundaries[0]);
                double maxX = Double.valueOf(boundaries[1]);
                double minY = Double.valueOf(boundaries[2]);
                double maxY = Double.valueOf(boundaries[3]);
                ArrayTupleBuilder keyTb = new ArrayTupleBuilder(rtreeKeyFieldCount);
                ArrayTupleReference key = new ArrayTupleReference();
                long searchStart = 0L;
                long searchEnd = 0L;
                long searchTime = 0L;
                int numQuery = 0;
                int num_inserts = 0;
                int num_updates = 0;
                //ArrayList<Long> searchTimes = new ArrayList<Long>();
                //ArrayList<Integer> numQueries = new ArrayList<Integer>();

                String line = "";
                while ((line = br.readLine()) != null && line.length() > 0) {
                    input.add(line);
                }
                br.close();
                fstream.close();
                in.close();
                barrier.await();//synchronize all threads
                if (my_id == 0) {
                    System.out.println("execution starts");
                    barrier.reset();
                    start = System.nanoTime();
                }
                for (String query : input) {
                    String items[] = query.split(",");

                    ts = Integer.valueOf(items[index[0]]);
                    //System.out.println(line);
                    id = Integer.valueOf(items[index[1]]);
                    x = Double.valueOf(items[index[2]]);
                    y = Double.valueOf(items[index[3]]);
                    i++;
                    tuple = (ArrayTupleReference) TupleUtils.createTuple(rFieldSerdes, x, y, x, y, id, ts);
                    //isQuery = false;
                    if (items[0].equals("I")) {
                        rIndexAccessor.insert(tuple);
                        num_inserts++;
                    } else if (items[0].equals("U")) {
                        rIndexAccessor.update(tuple);
                        num_updates++;
                    } else {
                        //current version, crash, it is an error, we don't have this experiment
                        throw new UnsupportedOperationException("error, current version doesn't support query yet");
                    }
                    //				TupleUtils.createIntegerTuple(tb, tuple, x, y, x, y, id, ts);
                }
                barrier.await();
                if (my_id == 0) {
                    end = System.nanoTime();
                    concurrent_time = end - start;
                    System.out.println("total execution time is " + concurrent_time + " ns");
                }
                total_operations_num.getAndAdd(i);
                System.out
                        .println("thread " + my_id + " num inserts: " + num_inserts + ", num updates: " + num_updates);
            } catch (BrokenBarrierException e) {
                throw new RuntimeException(e);
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            } catch (IOException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void multi_thread_main(int total_num) throws InterruptedException, HyracksDataException {
        CyclicBarrier barrier = new CyclicBarrier(total_num);
        //IIndexAccessor rIndexAccessor = rTreeIndex.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        Concurrent_Worker[] workers = new Concurrent_Worker[total_num];
        for (int i = 0; i < total_num; i++) {
            workers[i] = new Concurrent_Worker(i, total_num, barrier);
            workers[i].start();
        }
        for (int i = 0; i < total_num; i++) {
            workers[i].join();
        }
        double throughput_s = ((double) total_operations_num.get()) / (((double) (concurrent_time)) / 1000000000.0);
        System.out.println("throughput is " + throughput_s + " operations/s");
    }

    public void multi_threaded_exp(int my_id, int total_worker_num, CyclicBarrier barrier) throws Exception {
        String data = this.dataPath;
        data += "_" + total_worker_num + "_" + my_id;
        String[] ins = this.dataIndex.split(",");
        int[] index = new int[ins.length];
        for (int in = 0; in < ins.length; in++) {
            index[in] = Integer.parseInt(ins[in]);
        }

        FileInputStream fstream = new FileInputStream(data);
        DataInputStream in = new DataInputStream(fstream);
        BufferedReader br = new BufferedReader(new InputStreamReader(in));

        long start = 0L;
        long end = 0L;
        long time = 0L;
        //ArrayList<Long> times = new ArrayList<Long>();
        ArrayTupleBuilder tb = new ArrayTupleBuilder(rFieldCount);
        ArrayTupleReference tuple = new ArrayTupleReference();
        IIndexAccessor rIndexAccessor = rTreeIndex.createAccessor(NoOpIndexAccessParameters.INSTANCE);
        //start loading the array
        ArrayList<String> input = new ArrayList<>();
        int id;
        double x, y;
        double querySize;
        int ts;
        int i = 0;
        boolean isInsert = false;
        boolean isQuery = false;

        String[] boundaries = this.dataBoundary.split(",");
        double minX = Double.valueOf(boundaries[0]);
        double maxX = Double.valueOf(boundaries[1]);
        double minY = Double.valueOf(boundaries[2]);
        double maxY = Double.valueOf(boundaries[3]);
        ArrayTupleBuilder keyTb = new ArrayTupleBuilder(rtreeKeyFieldCount);
        ArrayTupleReference key = new ArrayTupleReference();
        long searchStart = 0L;
        long searchEnd = 0L;
        long searchTime = 0L;
        int numQuery = 0;
        int num_inserts = 0;
        int num_updates = 0;
        //ArrayList<Long> searchTimes = new ArrayList<Long>();
        //ArrayList<Integer> numQueries = new ArrayList<Integer>();

        String line = "";
        while ((line = br.readLine()) != null && line.length() > 0) {
            input.add(line);
        }
        br.close();
        fstream.close();
        in.close();
        barrier.await();//synchronize all threads
        if (my_id == 0) {
            barrier.reset();
            start = System.nanoTime();
        }
        for (String query : input) {
            String items[] = query.split(",");

            ts = Integer.valueOf(items[index[0]]);
            //System.out.println(line);
            id = Integer.valueOf(items[index[1]]);
            x = Double.valueOf(items[index[2]]);
            y = Double.valueOf(items[index[3]]);
            i++;
            tuple = (ArrayTupleReference) TupleUtils.createTuple(rFieldSerdes, x, y, x, y, id, ts);
            //isQuery = false;
            if (items[0].equals("I")) {
                rIndexAccessor.insert(tuple);
                num_inserts++;
            } else if (items[0].equals("U")) {
                rIndexAccessor.update(tuple);
                num_updates++;
            } else {
                //current version, crash, it is an error, we don't have this experiment
                throw new UnsupportedOperationException("error, current version doesn't support query yet");
            }
            //				TupleUtils.createIntegerTuple(tb, tuple, x, y, x, y, id, ts);
        }
        barrier.await();
        if (my_id == 0) {
            end = System.nanoTime();
            concurrent_time = end - start;
            System.out.println("total execution time is " + concurrent_time + " ns");
        }
        total_operations_num.getAndAdd(i);
        System.out.println("thread " + my_id + " num inserts: " + num_inserts + ", num updates: " + num_updates);
    }

    protected void rangeSearch(IBinaryComparatorFactory[] cmpFactories, IIndexAccessor indexAccessor,
            ISerializerDeserializer[] fieldSerdes, ITupleReference key, ITupleReference minFilterTuple,
            ITupleReference maxFilterTuple) throws Exception {
        if (LOGGER.isInfoEnabled()) {
            String kString = TupleUtils.printTuple(key, fieldSerdes);
            LOGGER.info("Range-Search using key: " + kString);
        }
        MultiComparator cmp = RTreeUtils.getSearchMultiComparator(cmpFactories, key);
        SearchPredicate rangePred;
        if (minFilterTuple != null && maxFilterTuple != null) {
            rangePred = new SearchPredicate(key, cmp, minFilterTuple, maxFilterTuple);
        } else {
            rangePred = new SearchPredicate(key, cmp);
        }
        IIndexCursor rangeCursor = indexAccessor.createSearchCursor(false);
        try {
            indexAccessor.search(rangeCursor, rangePred);
            try {
                int i = 0;
                while (rangeCursor.hasNext()) {
                    i++;
                    rangeCursor.next();
                    ITupleReference frameTuple = rangeCursor.getTuple();
                    //                    String rec = TupleUtils.printTuple(frameTuple, fieldSerdes);
                    //                    if (LOGGER.isInfoEnabled()) {
                    //                        LOGGER.error(rec);
                    //                    }
                }
                //                LOGGER.error("Found " +i+ " tuples?!");
            } finally {
                rangeCursor.close();
            }
        } finally {
            rangeCursor.destroy();
        }
    }

    protected ITreeIndex createRTreeIndex(ITypeTraits[] typeTraits, IBinaryComparatorFactory[] rtreeCmpFactories,
            IBinaryComparatorFactory[] btreeCmpFactories, IPrimitiveValueProviderFactory[] valueProviderFactories,
            RTreePolicyType rtreePolicyType, int[] rtreeFields, int[] btreeFields, ITypeTraits[] filterTypeTraits,
            IBinaryComparatorFactory[] filterCmpFactories, int[] filterFields) throws HyracksDataException {
        return LSMRTreeUtils.createLSMTree(lsmRTreeHarness.getIOManager(), lsmRTreeHarness.getVirtualBufferCaches(),
                lsmRTreeHarness.getFileReference(), lsmRTreeHarness.getDiskBufferCache(), typeTraits, rtreeCmpFactories,
                btreeCmpFactories, valueProviderFactories, rtreePolicyType,
                lsmRTreeHarness.getBoomFilterFalsePositiveRate(), lsmRTreeHarness.getMergePolicy(),
                lsmRTreeHarness.getOperationTracker(), lsmRTreeHarness.getIOScheduler(),
                lsmRTreeHarness.getIOOperationCallbackFactory(), lsmRTreeHarness.getPageWriteCallbackFactory(),
                LSMRTreeUtils.proposeBestLinearizer(typeTraits, rtreeCmpFactories.length), rtreeFields, btreeFields,
                filterTypeTraits, filterCmpFactories, filterFields, true, false,
                lsmRTreeHarness.getMetadataPageManagerFactory());
    }

    public static Collection concurrent_setting() {
        return Arrays.asList(new Object[][] { { false, true, true, true, true, "prefix", "5", 4, 8,
                "/scratch1/zhou822/lsm_rum_exp_data/berlin", 56129943, "1,2,4,5,6,7,9,10",
                "13.08833, 13.74215, 52.343, 52.65968",
                "13.65176,52.44976/13.26564,52.44209/13.5625,52.51313/13.30229,52.53927/13.49958,52.54785/13.3749,52.44346/13.41985,52.49914/13.505,52.46154/13.25955,52.52159/13.44636,52.61624/13.47779,52.47794/13.41155,52.457/13.41293,52.5245/13.58186,52.50628/13.336,52.54173/13.37418,52.45549/13.27924,52.49078/13.61445,52.43033/13.44135,52.51581/13.48921,52.50351/13.501,52.46238/13.443,52.50069/13.51579,52.5583/13.37949,52.50969/13.39278,52.56412/13.24713,52.53811/13.35958,52.4984/13.21387,52.53864/13.37905,52.60008/13.51484,52.42081/13.21073,52.42638/13.52255,52.448" }, });
    }

    public static void run_concurrent() throws Exception {
        List<Object[]> input = (List<Object[]>) concurrent_setting();
        String[] input_str = Arrays.stream(input.get(0)).map(Object::toString).toArray(String[]::new);
        UMTest um = new UMTest(input_str);
        um.setUp();

        um.multi_thread_main(8);

        um.tearDown();
    }

    public static void main(String[] args) throws Exception {
        List<Object[]> input = (List<Object[]>) LSMRTreeExamplesTest.primeNumbers();
        String[] input_str = Arrays.stream(input.get(0)).map(Object::toString).toArray(String[]::new);
        //Arrays.stream(input.get(0)).toArray(String[]::new);//Arrays.copyOf(input.get(0), input.get(0).length, String[].class);
        UMTest um = new UMTest(input_str);
        //System.out.println("Libin Test");
        //UMTest um = new UMTest(args);
        um.setUp();

        um.runTest();

        um.tearDown();
        //UMTest.run_concurrent();
        System.exit(0);
    }

}
