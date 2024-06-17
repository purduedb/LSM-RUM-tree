/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hyracks.storage.am.rtree.frames;

import java.nio.ByteBuffer;

import org.apache.hyracks.api.exceptions.HyracksDataException;
import org.apache.hyracks.dataflow.common.data.accessors.ITupleReference;
import org.apache.hyracks.storage.am.common.api.IPrimitiveValueProvider;
import org.apache.hyracks.storage.am.common.api.ITreeIndexFrame.Constants;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleReference;
import org.apache.hyracks.storage.am.common.api.ITreeIndexTupleWriter;
import org.apache.hyracks.storage.am.common.updatememo.UpdateMemo;
import org.apache.hyracks.storage.am.rtree.api.IRTreeLeafFrame;
import org.apache.hyracks.storage.common.MultiComparator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RTreeNSMLeafFrame extends RTreeNSMFrame implements IRTreeLeafFrame {
    private static final Logger LOGGER = LogManager.getLogger();

    public RTreeNSMLeafFrame(ITreeIndexTupleWriter tupleWriter, IPrimitiveValueProvider[] keyValueProviders,
            RTreePolicyType rtreePolicyType, boolean isPointMBR) {
        super(tupleWriter, keyValueProviders, rtreePolicyType, isPointMBR);
    }

    @Override
    public int getBytesRequiredToWriteTuple(ITupleReference tuple) {
        return tupleWriter.bytesRequired(tuple) + slotManager.getSlotSize();
    }

    @Override
    public ITreeIndexTupleReference createTupleReference() {
        return tupleWriter.createTupleReference();
    }

    @Override
    public int findTupleIndex(ITupleReference tuple, MultiComparator cmp) throws HyracksDataException {
        return slotManager.findTupleIndex(tuple, frameTuple, cmp, null, null);
    }

    @Override
    public boolean intersect(ITupleReference tuple, int tupleIndex, MultiComparator cmp) throws HyracksDataException {
        frameTuple.resetByTupleIndex(this, tupleIndex);
        int maxFieldPos = cmp.getKeyFieldCount() / 2;
        for (int i = 0; i < maxFieldPos; i++) {
            int j = maxFieldPos + i;
            int c = cmp.getComparators()[i].compare(tuple.getFieldData(i), tuple.getFieldStart(i),
                    tuple.getFieldLength(i), frameTuple.getFieldData(j), frameTuple.getFieldStart(j),
                    frameTuple.getFieldLength(j));
            if (c > 0) {
                return false;
            }
            c = cmp.getComparators()[i].compare(tuple.getFieldData(j), tuple.getFieldStart(j), tuple.getFieldLength(j),
                    frameTuple.getFieldData(i), frameTuple.getFieldStart(i), frameTuple.getFieldLength(i));

            if (c < 0) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int getTupleSize(ITupleReference tuple) {
        return tupleWriter.bytesRequired(tuple);
    }

    @Override
    public void insert(ITupleReference tuple, int tupleIndex) {
        //    	LOGGER.error("Buf: " + System.identityHashCode(buf));
        //    	LOGGER.error("LeafFrame: " + System.identityHashCode(this));
        //    	try {
        //			
        //    	}
        //    	catch(Exception e) {
        //    		LOGGER.error(e);
        //    	}
        slotManager.insertSlot(-1, buf.getInt(Constants.FREE_SPACE_OFFSET));
        int bytesWritten = tupleWriter.writeTuple(tuple, buf.array(), buf.getInt(Constants.FREE_SPACE_OFFSET));
        //      LOGGER.info("RTreeNSMLeafFrame-bytesWritten-" + bytesWritten);
        buf.putInt(Constants.TUPLE_COUNT_OFFSET, buf.getInt(Constants.TUPLE_COUNT_OFFSET) + 1);
        buf.putInt(Constants.FREE_SPACE_OFFSET, buf.getInt(Constants.FREE_SPACE_OFFSET) + bytesWritten);
        buf.putInt(TOTAL_FREE_SPACE_OFFSET,
                buf.getInt(TOTAL_FREE_SPACE_OFFSET) - bytesWritten - slotManager.getSlotSize());

        //        LOGGER.error(getTupleCount());
    }

    //    public void insert(ITupleReference tuple, int tupleIndex, UpdateMemo<Integer> memo) {
    ////    	touchCounter++;
    //////    	LOGGER.error(touchCounter);
    ////    	if(touchCounter == UpdateMemoConfig.K_UPDATE) {
    ////    		touchCounter = 0;
    //////    		cleanFrame(memo);
    ////    	}
    ////    	LOGGER.error("Buf: " + System.identityHashCode(buf));
    ////    	LOGGER.error("LeafFrame: " + System.identityHashCode(this));
    //        slotManager.insertSlot(-1, buf.getInt(Constants.FREE_SPACE_OFFSET));
    //        int bytesWritten = tupleWriter.writeTuple(tuple, buf.array(), buf.getInt(Constants.FREE_SPACE_OFFSET));
    ////        LOGGER.info("RTreeNSMLeafFrame-bytesWritten-" + bytesWritten);
    //        buf.putInt(Constants.TUPLE_COUNT_OFFSET, buf.getInt(Constants.TUPLE_COUNT_OFFSET) + 1);
    //        buf.putInt(Constants.FREE_SPACE_OFFSET, buf.getInt(Constants.FREE_SPACE_OFFSET) + bytesWritten);
    //        buf.putInt(TOTAL_FREE_SPACE_OFFSET,
    //                buf.getInt(TOTAL_FREE_SPACE_OFFSET) - bytesWritten - slotManager.getSlotSize());
    //        
    ////        LOGGER.error(getTupleCount());
    //    }

    @Override
    public void delete(int tupleIndex, MultiComparator cmp) {
        int slotOff = slotManager.getSlotOff(tupleIndex);

        int tupleOff = slotManager.getTupleOff(slotOff);
        frameTuple.resetByTupleOffset(buf.array(), tupleOff);
        int tupleSize = tupleWriter.bytesRequired(frameTuple);

        byte[] tupleData = frameTuple.getFieldData(0);
        int ki = frameTuple.getFieldCount() - 2;
        int ti = frameTuple.getFieldCount() - 1;
        //		LOGGER.error("removing.. " + ByteBuffer.wrap(tupleData, frameTuple.getFieldStart(ki), frameTuple.getFieldLength(ki)).getInt());
        //		int aa = slotManager.getSlotSize();
        //		if(ByteBuffer.wrap(tupleData, frameTuple.getFieldStart(ki), frameTuple.getFieldLength(ki)).getInt() == 11
        //			|| ByteBuffer.wrap(tupleData, frameTuple.getFieldStart(ki), frameTuple.getFieldLength(ki)).getInt() == 7
        //			|| ByteBuffer.wrap(tupleData, frameTuple.getFieldStart(ki), frameTuple.getFieldLength(ki)).getInt() == 40) {
        //			LOGGER.error("?");
        //			
        //		}

        // perform deletion (we just do a memcpy to overwrite the slot)
        int slotStartOff = slotManager.getSlotEndOff();
        int length = slotOff - slotStartOff;
        if (length >= 0)
            System.arraycopy(buf.array(), slotStartOff, buf.array(), slotStartOff + slotManager.getSlotSize(), length);

        // maintain space information
        buf.putInt(Constants.TUPLE_COUNT_OFFSET, buf.getInt(Constants.TUPLE_COUNT_OFFSET) - 1);
        buf.putInt(TOTAL_FREE_SPACE_OFFSET,
                buf.getInt(TOTAL_FREE_SPACE_OFFSET) + tupleSize + slotManager.getSlotSize());
    }

    public void cleanFrame(UpdateMemo<Integer> memo) {
        int tupleCount = buf.getInt(Constants.TUPLE_COUNT_OFFSET);

        //        int freeSpace = buf.getInt(Constants.FREE_SPACE_OFFSET);
        //         Sort the slots by the tuple offset they point to.
        //        ArrayList<SlotOffTupleOff> sortedTupleOffs = new ArrayList<>();
        //        sortedTupleOffs.ensureCapacity(tupleCount);

        for (int i = 0; i < tupleCount; i++) {
            //        	LOGGER.error("count: " + i + " / " + tupleCount);
            int slotOff = slotManager.getSlotOff(i);
            int tupleOff = slotManager.getTupleOff(slotOff);

            frameTuple.resetByTupleOffset(buf.array(), tupleOff);
            byte[] tupleData = frameTuple.getFieldData(0);
            int ki = frameTuple.getFieldCount() - 2;
            int ti = frameTuple.getFieldCount() - 1;

            //			LOGGER.error("check: " + ByteBuffer.wrap(tupleData, frameTuple.getFieldStart(ki), frameTuple.getFieldLength(ki)).getInt());

            boolean isOld = memo.cleanEntity(
                    ByteBuffer.wrap(tupleData, frameTuple.getFieldStart(ki), frameTuple.getFieldLength(ki)).getInt(),
                    ByteBuffer.wrap(tupleData, frameTuple.getFieldStart(ti), frameTuple.getFieldLength(ti)).getInt());

            if (isOld) {
                delete(i, null);
                //        		this.setPageLsn);
                i--;
                tupleCount--;
                //        		tupleCount = buf.getInt(Constants.TUPLE_COUNT_OFFSET);
                //        		LOGGER.error("cleaned!!!");
            }
        }
        //        if(tupleCount != 0)
        //        	LOGGER.error("cleaned!!!");
        //    	for (int i = 1; i < getTupleCount(); i++) {
        //            frameTuple.resetByTupleIndex(this, i);
        //            byte[] tupleData = frameTuple.getFieldData(0);
        //        	int ki = frameTuple.getFieldCount()-2;
        //        	int ti = frameTuple.getFieldCount()-1;
        //        	
        //        	boolean isOld;
        //        	isOld = memo.cleanEntity(ByteBuffer.wrap(tupleData, frameTuple.getFieldStart(ki), frameTuple.getFieldLength(ki)).getInt(),
        //        			ByteBuffer.wrap(tupleData, frameTuple.getFieldStart(ti), frameTuple.getFieldLength(ti)).getInt());
        //        	
        //        	if(isOld) {
        //        		delete(i, null);
        //        		LOGGER.error("cleaned?!");
        //        	}
        //    	}
        //    	return getTupleCount();
    }

    @Override
    public int getFieldCount() {
        return frameTuple.getFieldCount();
    }

    @Override
    public ITupleReference getBeforeTuple(ITupleReference tuple, int targetTupleIndex, MultiComparator cmp)
            throws HyracksDataException {
        // Examine the tuple index to determine whether it is valid or not.
        if (targetTupleIndex != slotManager.getGreatestKeyIndicator()) {
            // We need to check the key to determine whether it's an insert or an update.
            frameTuple.resetByTupleIndex(this, targetTupleIndex);
            if (cmp.compare(tuple, frameTuple) == 0) {
                // The keys match, it's an update.
                return frameTuple;
            }
        }
        // Either the tuple index is a special indicator, or the keys don't match.
        // In those cases, we are definitely dealing with an insert.
        return null;
    }
}
