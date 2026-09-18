package org.epics.example;

import org.epics.pvdata.factory.FieldFactory;
import org.epics.pvdata.factory.PVDataFactory;
import org.epics.pvdata.factory.StandardFieldFactory;
import org.epics.pvdata.property.PVTimeStamp;
import org.epics.pvdata.property.PVTimeStampFactory;
import org.epics.pvdata.property.TimeStamp;
import org.epics.pvdata.property.TimeStampFactory;
import org.epics.pvdata.pv.PVFloat;
import org.epics.pvdata.pv.PVInt;
import org.epics.pvdata.pv.PVStructure;
import org.epics.pvdata.pv.ScalarType;
import org.epics.pvdata.pv.Structure;
import org.epics.pvdatabase.PVRecord;

public class SineCounterRecord extends PVRecord {

    private final double periodSeconds;
    private final TimeStamp timeStamp = TimeStampFactory.create();
    private PVTimeStamp pvTimeStamp;
    private PVInt pvCounter;
    private PVFloat pvValue;
    private int counter;

    public static SineCounterRecord create(String recordName, double periodSeconds) {
        Structure structure = FieldFactory.getFieldCreate().createFieldBuilder()
                .setId("example:SineCounter:1.0")
                .add("timeStamp", StandardFieldFactory.getStandardField().timeStamp())
                .add("counter", ScalarType.pvInt)
                .add("value", ScalarType.pvFloat)
                .createStructure();
        PVStructure pvStructure = PVDataFactory.getPVDataCreate().createPVStructure(structure);
        SineCounterRecord record = new SineCounterRecord(recordName, pvStructure, periodSeconds);
        record.init();
        return record;
    }

    private SineCounterRecord(String recordName, PVStructure pvStructure, double periodSeconds) {
        super(recordName, pvStructure);
        this.periodSeconds = periodSeconds;
    }

    private void init() {
        PVStructure pvStructure = getPVStructure();
        pvTimeStamp = PVTimeStampFactory.create();
        if (!pvTimeStamp.attach(pvStructure.getSubField("timeStamp"))) {
            throw new IllegalStateException("can not attach timeStamp");
        }
        pvCounter = pvStructure.getSubField(PVInt.class, "counter");
        pvValue = pvStructure.getSubField(PVFloat.class, "value");
    }

    public void update() {
        lock();
        try {
            beginGroupPut();
            process();
            getPVRecordStructure().postPut();
            endGroupPut();
        } finally {
            unlock();
        }
    }

    @Override
    public void process() {
        timeStamp.getCurrentTime();
        pvTimeStamp.set(timeStamp);
        pvCounter.put(counter);
        counter += 1;
        double t = timeStamp.getMilliSeconds() / 1000.0;
        pvValue.put((float) Math.sin(2.0 * Math.PI * t / periodSeconds));
    }
}
